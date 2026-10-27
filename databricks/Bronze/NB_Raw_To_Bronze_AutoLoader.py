# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze Layer - Raw to Bronze Delta Lake (Auto Loader)
# MAGIC
# MAGIC Ingests raw landing files from ADLS Gen2 (`raw` container) directly into **Bronze Delta Tables** using **Databricks Auto Loader (`cloudFiles`)**.
# MAGIC
# MAGIC ### Production Capabilities:
# MAGIC - **ACID Transactions**: Atomic appends without corrupt/partial files.
# MAGIC - **Audit Stamping**: Appends `_ingested_at`, `_source_file`, `_batch_id`, and `_run_id`.
# MAGIC - **Corrupt Data Handling**: Unparseable or malformed rows are isolated into `_rescued_data`.
# MAGIC - **Idempotent Streaming**: Checkpointed with `Trigger.AvailableNow` for efficient batch scheduling.
# MAGIC - **Unity Catalog Registration**: Registered under `{CATALOG}.bronze.*` with external ADLS paths.

# COMMAND ----------

dbutils.widgets.text("auth_mode", "auto")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("catalog", "nwretail_databricks_dev")
dbutils.widgets.text("storage_account", "nwretaildatalake65")

# COMMAND ----------

import json
import time
import uuid
from datetime import datetime
from pyspark.sql import functions as F

def _get_arg(name, default=""):
    try:
        val = dbutils.widgets.get(name)
        return val.strip() if val else default
    except Exception:
        return default

CATALOG = _get_arg("catalog", "nwretail_databricks_dev")
STORAGE_ACCOUNT = _get_arg("storage_account", "nwretaildatalake65")
RUN_ID = _get_arg("run_id") or str(uuid.uuid4())
BATCH_ID = _get_arg("batch_id") or f"bronze-{RUN_ID[:8]}"

RAW_PATH = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SCHEMA_STORE = f"{BRONZE_PATH}/_schemas"
CHECKPOINT_STORE = f"{BRONZE_PATH}/_checkpoints"

print(f"Bronze Ingestion starting - Run ID: {RUN_ID}, Batch ID: {BATCH_ID}")
print(f"Source RAW Path: {RAW_PATH}")
print(f"Target BRONZE Path: {BRONZE_PATH}")
print(f"Target Catalog: {CATALOG}.bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema Initialization

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze COMMENT 'Bronze layer - Raw landing tables stored as Delta Lake'")

# Entities definition: folder name, file format, target delta table
ENTITIES = [
    {"name": "stores", "folder": "stores", "format": "csv"},
    {"name": "customers", "folder": "customers", "format": "csv"},
    {"name": "products", "folder": "products", "format": "csv"},
    {"name": "sales_transactions", "folder": "sales_transactions", "format": "csv"},
    {"name": "returns", "folder": "returns", "format": "csv"},
    {"name": "inventory", "folder": "inventory", "format": "csv"}
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingestion Execution via Auto Loader

# COMMAND ----------

results = {
    "run_id": RUN_ID,
    "batch_id": BATCH_ID,
    "status": "SUCCESS",
    "timestamp": datetime.now().isoformat(),
    "tables": {},
    "errors": []
}

for entity in ENTITIES:
    entity_name = entity["name"]
    source_folder = f"{RAW_PATH}/{entity['folder']}"
    target_table = f"{CATALOG}.bronze.{entity_name}"
    target_location = f"{BRONZE_PATH}/{entity_name}"
    checkpoint_location = f"{CHECKPOINT_STORE}/{entity_name}"
    schema_location = f"{SCHEMA_STORE}/{entity_name}"
    
    print(f"\n--- Ingesting Entity: {entity_name} ---")
    print(f"Source: {source_folder}")
    print(f"Target: {target_table} ({target_location})")
    
    try:
        # Auto Loader streaming read with trigger available-now
        stream_reader = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", entity["format"])
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.inferColumnTypes", "false")  # Preserve raw strings in bronze
            .option("header", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(source_folder)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_run_id", F.lit(RUN_ID))
        )
        
        # Write to Delta table with Trigger.AvailableNow
        query = (
            stream_reader.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .option("path", target_location)
            .trigger(availableNow=True)
            .toTable(target_table)
        )
        
        query.awaitTermination()
        
        # Read final row count
        count = spark.table(target_table).count()
        results["tables"][entity_name] = count
        print(f"✓ Ingestion complete: {target_table} total rows = {count}")
        
    except Exception as exc:
        err_msg = f"Failed to ingest {entity_name}: {str(exc)}"
        print(f"✗ ERROR: {err_msg}")
        results["errors"].append(err_msg)
        results["status"] = "FAILED"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Final Summary & Exit Payload

# COMMAND ----------

print("\n================ Bronze Ingestion Summary ================")
print(json.dumps(results, indent=2))
print("==========================================================")

dbutils.notebook.exit(json.dumps(results))
