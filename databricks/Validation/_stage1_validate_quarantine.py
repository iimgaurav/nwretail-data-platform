# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 1 validation - quarantine + DQ path (temporary)
# MAGIC
# MAGIC Real NorthWind data is clean, so the quarantine path would never run in
# MAGIC production tests. This injects known defects into an isolated probe entity to
# MAGIC prove: multi-violation reason concatenation, cast-failure capture with the
# MAGIC original value preserved, append-only behaviour, and correct split counts.

# COMMAND ----------

# MAGIC %run ./Silver/NB_Silver_Common

# COMMAND ----------

import json

PROBE = "_dq_probe"
RUN = "stage1-dq-probe"
out = {}

ENTITY_SPEC[PROBE] = json.loads(json.dumps(ENTITY_SPEC["customers"]))
BRONZE_SCHEMAS[PROBE] = BRONZE_SCHEMAS["customers"]

spark.sql(f"DROP TABLE IF EXISTS {quarantine_table(PROBE)}")
spark.sql(f"DROP TABLE IF EXISTS {table_name(PROBE)}")
dbutils.fs.rm(f"{QUARANTINE_PATH}/{PROBE}/", True)
dbutils.fs.rm(f"{SILVER_PATH}/{PROBE}/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a deliberately broken 5-row batch

# COMMAND ----------

raw = (
    read_bronze("customers")
    .limit(5)
    .withColumn("_rn", F.row_number().over(Window.orderBy("customer_id")))
)

corrupted = (
    raw
    # 1: invalid segment AND blank country -> two violations on one row
    .withColumn(
        "segment",
        F.when(F.col("_rn") == 1, F.lit("Enterprise")).otherwise(F.col("segment")),
    )
    .withColumn(
        "country", F.when(F.col("_rn") == 1, F.lit("   ")).otherwise(F.col("country"))
    )
    # 2: malformed email
    .withColumn(
        "email",
        F.when(F.col("_rn") == 2, F.lit("not-an-email")).otherwise(F.col("email")),
    )
    # 3: unparseable date -> cast failure, original must survive in _raw_signup_date
    .withColumn(
        "signup_date",
        F.when(F.col("_rn") == 3, F.lit("31/31/2099")).otherwise(F.col("signup_date")),
    )
    # 4 and 5 stay valid
    .drop("_rn")
)

staged = (
    corrupted
    .transform(lambda d: trim_and_standardize(d, PROBE))
    .transform(lambda d: apply_casts(d, PROBE))
    .transform(lambda d: add_ingestion_metadata(d, PROBE, "dq-probe-b1"))
    .transform(lambda d: add_row_hash(d, PROBE))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate DQ and split

# COMMAND ----------

scored, rule_counts = validate_business_rules(staged, PROBE)
valid, quarantined = split_valid_and_quarantine(scored, PROBE)

out["rule_counts"] = rule_counts
out["valid_rows"] = valid.count()
out["quarantined_rows"] = quarantined.count()

out["reasons"] = sorted(
    [r["_reject_reason"] for r in quarantined.select("_reject_reason").collect()]
)
out["cast_error_columns"] = sorted(
    [
        r["_cast_error_column"] or ""
        for r in quarantined.select("_cast_error_column").collect()
    ]
)
out["raw_date_preserved"] = [
    r["_raw_signup_date"]
    for r in quarantined.filter(F.col("_cast_error_column") == "signup_date")
    .select("_raw_signup_date")
    .collect()
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write quarantine twice -> append-only

# COMMAND ----------

n1 = write_quarantine(quarantined, PROBE, RUN, "dq-probe-b1")
n2 = write_quarantine(quarantined, PROBE, RUN, "dq-probe-b2")

out["first_write"] = n1
out["second_write"] = n2
out["quarantine_table_rows"] = spark.read.table(quarantine_table(PROBE)).count()
out["quarantine_columns"] = spark.read.table(quarantine_table(PROBE)).columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertions

# COMMAND ----------

r = out["reasons"]
multi = [x for x in r if "SEGMENT_VALID" in x and "COUNTRY_VALID" in x]

out["checks"] = {
    "three_rows_quarantined": out["quarantined_rows"] == 3,
    "two_rows_valid": out["valid_rows"] == 2,
    "segment_rule_fired_once": rule_counts.get("SEGMENT_VALID") == 1,
    "email_rule_fired_once": rule_counts.get("EMAIL_FORMAT_VALID") == 1,
    "cast_rule_fired_once": rule_counts.get("TYPE_CAST_VALID") == 1,
    "country_rule_fired_once": rule_counts.get("COUNTRY_VALID") == 1,
    "cast_failure_cascades_to_date_rule": rule_counts.get("SIGNUP_DATE_VALID") == 1,
    "multi_violation_row_has_both": len(multi) == 1,
    "bad_date_preserved_in_raw": out["raw_date_preserved"] == ["31/31/2099"],
    "cast_error_names_column": out["cast_error_columns"] == ["", "", "signup_date"],
    "append_only_3_then_6": out["quarantine_table_rows"] == 6,
    "first_write_3": out["first_write"] == 3,
    "second_write_3": out["second_write"] == 3,
    "reject_reason_column_present": "_reject_reason" in out["quarantine_columns"],
    "run_id_captured": "_quarantine_run_id" in out["quarantine_columns"],
    "batch_id_captured": "_quarantine_batch_id" in out["quarantine_columns"],
    "raw_date_column_kept": "_raw_signup_date" in out["quarantine_columns"],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {quarantine_table(PROBE)}")
spark.sql(f"DROP TABLE IF EXISTS {table_name(PROBE)}")
dbutils.fs.rm(f"{QUARANTINE_PATH}/{PROBE}/", True)
dbutils.fs.rm(f"{SILVER_PATH}/{PROBE}/", True)
dbutils.fs.rm(f"{SILVER_PATH}/_tmp/", True)

out["cleanup_done"] = not spark.catalog.tableExists(quarantine_table(PROBE))
out["all_passed"] = all(out["checks"].values()) and out["cleanup_done"]

dbutils.notebook.exit(json.dumps(out, default=str))
