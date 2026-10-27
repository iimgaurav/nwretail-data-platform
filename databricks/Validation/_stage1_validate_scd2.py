# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 1 validation - SCD2 engine (temporary)
# MAGIC
# MAGIC Exercises `apply_scd2` against an isolated probe table so that
# MAGIC `silver.customers` is never touched. Covers:
# MAGIC 1. initial load
# MAGIC 2. tracked-attribute change -> old version closed, new version opened
# MAGIC 3. rerun idempotency
# MAGIC 4. late-arriving record -> routed to quarantine, not back-dated

# COMMAND ----------

# MAGIC %run ./Silver/NB_Silver_Common

# COMMAND ----------

import json

PROBE = "_scd2_probe"
RUN = "stage1-scd2-probe"
out = {}

# Isolated entity: same source file as customers, different table name.
ENTITY_SPEC[PROBE] = json.loads(json.dumps(ENTITY_SPEC["customers"]))
BRONZE_SCHEMAS[PROBE] = BRONZE_SCHEMAS["customers"]

# Reset so the probe always starts from a clean slate, including the underlying
# data files (UC external tables are not purged by DROP TABLE).
spark.sql(f"DROP TABLE IF EXISTS {table_name(PROBE)}")
spark.sql(f"DROP TABLE IF EXISTS {quarantine_table(PROBE)}")
dbutils.fs.rm(f"{SILVER_PATH}/{PROBE}/", True)
dbutils.fs.rm(f"{QUARANTINE_PATH}/{PROBE}/", True)
for t in ("run_log", "data_quality_summary", "entity_watermarks"):
    spark.sql(f"DELETE FROM {CATALOG}.silver_ops.{t} WHERE entity = '{PROBE}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initial load

# COMMAND ----------

out["initial"] = execute_entity(PROBE, run_id=RUN, batch_id=f"{RUN}-p1")
out["initial_versions"] = spark.read.table(table_name(PROBE)).count()
out["initial_current"] = (
    spark.read.table(table_name(PROBE)).filter(F.col("is_current")).count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tracked attribute change on one customer

# COMMAND ----------

base = (
    spark.read.table(table_name(PROBE))
    .filter(F.col("is_current"))
    .drop("effective_from", "effective_to", "is_current")
)
bumped = base.withColumn("_ingest_ts", F.current_timestamp() + F.expr("INTERVAL 1 DAY"))
changed = bumped.withColumn(
    "segment",
    F.when(F.col("customer_id") == "C0001", F.lit("Corporate"))
    .otherwise(F.col("segment")),
)

new_v, closed_v, late_df = apply_scd2(changed, PROBE, RUN)
out["change_new_versions"] = new_v
out["change_closed_versions"] = closed_v
out["change_late"] = late_df.count()

tbl = spark.read.table(table_name(PROBE))
out["after_change_versions"] = tbl.count()
out["after_change_current"] = tbl.filter(F.col("is_current")).count()

out["c0001_versions"] = [
    {
        "segment": r["segment"],
        "is_current": r["is_current"],
        "effective_from": str(r["effective_from"]),
        "effective_to": str(r["effective_to"]),
    }
    for r in tbl.filter(F.col("customer_id") == "C0001")
    .orderBy("effective_from")
    .collect()
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rerun the same batch - must be a no-op

# COMMAND ----------

base2 = (
    spark.read.table(table_name(PROBE))
    .filter(F.col("is_current"))
    .drop("effective_from", "effective_to", "is_current")
)
bumped2 = base2.withColumn(
    "_ingest_ts", F.current_timestamp() + F.expr("INTERVAL 1 DAY")
)
same = bumped2.withColumn(
    "segment",
    F.when(F.col("customer_id") == "C0001", F.lit("Corporate"))
    .otherwise(F.col("segment")),
)

n2, c2, _l2 = apply_scd2(same, PROBE, RUN)
out["rerun_new_versions"] = n2
out["rerun_closed_versions"] = c2
out["rerun_total_versions"] = spark.read.table(table_name(PROBE)).count()
out["rerun_current"] = (
    spark.read.table(table_name(PROBE)).filter(F.col("is_current")).count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Late-arriving record must not be back-dated

# COMMAND ----------

late_src = (
    spark.read.table(table_name(PROBE))
    .filter(F.col("is_current"))
    .drop("effective_from", "effective_to", "is_current")
    .withColumn(
        "_ingest_ts",
        F.when(
            F.col("customer_id") == "C0002",
            F.current_timestamp() - F.expr("INTERVAL 60 DAYS"),
        ).otherwise(F.current_timestamp() + F.expr("INTERVAL 2 DAYS")),
    )
    .withColumn(
        "segment",
        F.when(F.col("customer_id") == "C0002", F.lit("Small Business"))
        .otherwise(F.col("segment")),
    )
)

n3, c3, late3 = apply_scd2(late_src, PROBE, RUN)
out["late_new_versions"] = n3
out["late_closed_versions"] = c3
out["late_flagged"] = late3.count()
out["late_total_versions"] = spark.read.table(table_name(PROBE)).count()
out["late_c0002_segment"] = (
    spark.read.table(table_name(PROBE))
    .filter((F.col("customer_id") == "C0002") & F.col("is_current"))
    .select("segment")
    .first()["segment"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Assertions

# COMMAND ----------

c1 = [v for v in out["c0001_versions"]]
checks = {
    "initial_load_200_versions": out["initial_versions"] == 200,
    "initial_all_current": out["initial_current"] == 200,
    "one_new_version_opened": out["change_new_versions"] == 1,
    "one_old_version_closed": out["change_closed_versions"] == 1,
    "total_versions_201": out["after_change_versions"] == 201,
    "current_still_200": out["after_change_current"] == 200,
    "c0001_two_versions": len(c1) == 2,
    "c0001_one_current": sum(1 for v in c1 if v["is_current"]) == 1,
    "c0001_current_is_corporate": any(
        v["is_current"] and v["segment"] == "Corporate" for v in c1
    ),
    "c0001_old_is_closed": any(
        (not v["is_current"]) and v["segment"] == "Consumer" and v["effective_to"] != "None"
        for v in c1
    ),
    "rerun_no_new_versions": out["rerun_new_versions"] == 0,
    "rerun_no_closed_versions": out["rerun_closed_versions"] == 0,
    "rerun_total_unchanged": out["rerun_total_versions"] == 201,
    "late_not_inserted": out["late_new_versions"] == 0,
    "late_one_flagged": out["late_flagged"] == 1,
    "late_not_backdated": out["late_c0002_segment"] == "Consumer",
    "late_total_unchanged": out["late_total_versions"] == 201,
}
out["checks"] = checks
out["all_passed"] = all(checks.values())

dbutils.notebook.exit(json.dumps(out, default=str))
