# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 1 validation - framework smoke test (temporary)
# MAGIC
# MAGIC 1. removes SCD2 probe artifacts left by `_stage1_validate_scd2`
# MAGIC 2. runs `stores` twice through the full framework (non-SCD2 MERGE path)
# MAGIC 3. asserts the silver contract
# MAGIC
# MAGIC Safe to re-run: `stores` is idempotent.

# COMMAND ----------

# MAGIC %run ./Silver/NB_Silver_Common

# COMMAND ----------

import json

PROBE = "_scd2_probe"
out = {"cleanup": {}, "checks": {}}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cleanup of probe artifacts

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {table_name(PROBE)}")
spark.sql(f"DROP TABLE IF EXISTS {quarantine_table(PROBE)}")
dbutils.fs.rm(f"{SILVER_PATH}/{PROBE}/", True)
dbutils.fs.rm(f"{QUARANTINE_PATH}/{PROBE}/", True)
dbutils.fs.rm(f"{SILVER_PATH}/_tmp/", True)
for t in ("run_log", "data_quality_summary", "entity_watermarks"):
    spark.sql(f"DELETE FROM {CATALOG}.silver_ops.{t} WHERE entity = '{PROBE}'")
out["cleanup"]["probe_removed"] = not spark.catalog.tableExists(table_name(PROBE))

# COMMAND ----------

result = {
    "auth_mode": AUTH_MODE,
    "auth_detail": AUTH_DETAIL,
    "spark_version": spark.version,
    "entities": ENTITY_ORDER,
}

# COMMAND ----------

result["stores_pass1"] = execute_entity(
    "stores", run_id="stage1-validate", batch_id="stage1-validate-p1"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Second pass - must update in place, never duplicate

# COMMAND ----------

result["stores_pass2"] = execute_entity(
    "stores", run_id="stage1-validate", batch_id="stage1-validate-p2"
)

# COMMAND ----------

tbl = spark.read.table(table_name("stores"))
result["silver_stores_count"] = tbl.count()
result["silver_stores_columns"] = tbl.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Assertions

# COMMAND ----------

leaked = [
    c for c in tbl.columns
    if c.startswith("_raw_") or c in ("_reject_reason", "_cast_error_column", "_rn", "_scd2_late")
    or c.startswith("_cur_") or c.startswith("_ref_")
]

p1, p2 = result["stores_pass1"], result["stores_pass2"]

out["checks"] = {
    "pass1_success": p1["status"] == "SUCCESS",
    "pass2_success": p2["status"] == "SUCCESS",
    "pass1_read_7": p1["rows_read"] == 7,
    "pass2_no_inserts": p2["rows_inserted"] == 0,
    "pass2_updates_7": p2["rows_updated"] == 7,
    "row_count_7": result["silver_stores_count"] == 7,
    "no_leaked_columns": not leaked,
    "no_quarantine": p1["rows_quarantined"] == 0 and p2["rows_quarantined"] == 0,
    "both_pass_batch_ids_differ": p1["batch_id"] != p2["batch_id"],
}
out["leaked_columns"] = leaked
out["all_passed"] = all(out["checks"].values()) and out["cleanup"]["probe_removed"]

dbutils.notebook.exit(json.dumps(out, default=str))
