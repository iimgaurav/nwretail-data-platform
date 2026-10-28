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
import logging
import sys
import time
import uuid
from datetime import datetime
from pyspark.sql import functions as F

class DatabricksLogFormatter(logging.Formatter):
    """
    Production log formatter for Databricks / Azure Monitor.
    Emits structured, human-readable and machine-parseable log lines:
    ISO-Timestamp [LEVEL] [logger] [run_id] message
    """
    def __init__(self, run_id: str = "", batch_id: str = ""):
        super().__init__()
        self.run_id = run_id
        self.batch_id = batch_id
        
    def format(self, record):
        ts = self.formatTime(record, "%Y-%m-%dT%H:%M:%S")
        ctx = f" [run_id={self.run_id}]" if self.run_id else ""
        base = f"{ts} [{record.levelname:^7}] [{record.name}]{ctx} {record.getMessage()}"
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base

def get_notebook_logger(name: str, run_id: str = "", batch_id: str = "") -> logging.Logger:
    """
    Creates or configures a production-grade logger attached to sys.stdout.
    Avoids duplicate handlers across Databricks notebook cell re-runs.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(DatabricksLogFormatter(run_id=run_id, batch_id=batch_id))
    logger.addHandler(handler)
    logger.propagate = False
    return logger

def _get_arg(name, default=""):
    try:
        val = dbutils.widgets.get(name)
        return val.strip() if val else default
    except Exception:
        return default

CATALOG = _get_arg("catalog", "nwretail_databricks_dev")
STORAGE_ACCOUNT = _get_arg("storage_account", "nwretaildatalake65")
SOURCE_CONTAINER = _get_arg("source_container", "bronze")
RUN_ID = _get_arg("run_id") or str(uuid.uuid4())
BATCH_ID = _get_arg("batch_id") or f"bronze-{RUN_ID[:8]}"

SOURCE_PATH = f"abfss://{SOURCE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_DELTA_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_delta_tables"
SCHEMA_STORE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_delta_schemas"
CHECKPOINT_STORE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_delta_checkpoints"

logger = get_notebook_logger("nwretail.bronze.autoloader", run_id=RUN_ID, batch_id=BATCH_ID)
logger.info("Bronze Auto Loader initialization starting")
logger.info("Parameters: Catalog=%s.bronze, Source=%s, Target=%s", CATALOG, SOURCE_PATH, BRONZE_DELTA_PATH)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema Initialization
# MAGIC Ensures the Unity Catalog `bronze` schema exists.
# MAGIC
# MAGIC # COMMAND ----------

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
    source_folder = f"{SOURCE_PATH}/{entity['folder']}"
    target_table = f"{CATALOG}.bronze.{entity_name}"
    target_location = f"{BRONZE_DELTA_PATH}/{entity_name}"
    checkpoint_location = f"{CHECKPOINT_STORE}/{entity_name}"
    schema_location = f"{SCHEMA_STORE}/{entity_name}"
    
    logger.info("Ingesting entity: %s (Source: %s -> Target: %s)", entity_name, source_folder, target_table)
    
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
            .withColumn("_source_file", F.col("_metadata.file_path"))
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
        logger.info("Ingestion complete: %s (total_rows=%d)", target_table, count)
        
    except Exception as exc:
        err_msg = f"Failed to ingest {entity_name}: {str(exc)}"
        logger.error("Ingestion failure on entity %s: %s", entity_name, exc, exc_info=True)
        results["errors"].append(err_msg)
        results["status"] = "FAILED"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Final Summary & Exit Payload

# COMMAND ----------

logger.info("Bronze Ingestion Execution Summary:\n%s", json.dumps(results, indent=2))

if results["status"] == "FAILED":
    raise RuntimeError(f"Bronze Auto Loader Ingestion failed with errors: {results['errors']}")

dbutils.notebook.exit(json.dumps(results))

