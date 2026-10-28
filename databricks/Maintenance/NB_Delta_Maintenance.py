# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Delta Lake Maintenance - OPTIMIZE, Z-ORDER & VACUUM
# MAGIC
# MAGIC Weekly scheduled maintenance job to ensure peak query performance and storage hygiene:
# MAGIC - **Compacts Small Files**: Mitigates file fragmentation from continuous micro-appends.
# MAGIC - **Z-Orders High-Cardinality Keys**: Clusters data by query predicate keys (dates, stores, customers).
# MAGIC - **Vacuums Stale Snapshots**: Reclaims cloud storage costs while respecting retention policies (168 hours / 7 days).

# COMMAND ----------

dbutils.widgets.text("catalog", "nwretail_databricks_dev")
dbutils.widgets.text("retention_hours", "168")

# COMMAND ----------

import json
import logging
import sys
from datetime import datetime

class DatabricksLogFormatter(logging.Formatter):
    """
    Production log formatter for Databricks / Azure Monitor.
    Emits structured, human-readable and machine-parseable log lines:
    ISO-Timestamp [LEVEL] [logger] message
    """
    def format(self, record):
        ts = self.formatTime(record, "%Y-%m-%dT%H:%M:%S")
        base = f"{ts} [{record.levelname:^7}] [{record.name}] {record.getMessage()}"
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base

def get_notebook_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(DatabricksLogFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger

CATALOG = dbutils.widgets.get("catalog").strip() or "nwretail_databricks_dev"
RETENTION_HOURS = int(dbutils.widgets.get("retention_hours").strip() or "168")

logger = get_notebook_logger("nwretail.maintenance.delta")
logger.info("Delta Maintenance Job Starting (Catalog=%s, RetentionHours=%d)", CATALOG, RETENTION_HOURS)

# Definition of tables and their optimal Z-ORDER keys
MAINTENANCE_TARGETS = [
    # Gold layer dimensional and fact tables
    {"schema": "gold", "table": "fact_sales", "zorder": ["date_key", "store_key", "product_key"]},
    {"schema": "gold", "table": "fact_returns", "zorder": ["date_key", "store_key"]},
    {"schema": "gold", "table": "fact_inventory", "zorder": ["date_key", "store_key", "product_key"]},
    {"schema": "gold", "table": "dim_customer", "zorder": ["customer_key", "is_current"]},
    {"schema": "gold", "table": "dim_product", "zorder": ["product_key", "is_current"]},
    {"schema": "gold", "table": "dim_store", "zorder": ["store_key"]},
    # Silver layer tables
    {"schema": "silver", "table": "sales_transactions", "zorder": ["transaction_id", "transaction_date"]},
    {"schema": "silver", "table": "customers", "zorder": ["customer_id", "is_current"]},
    {"schema": "silver", "table": "products", "zorder": ["product_id", "is_current"]},
    {"schema": "silver", "table": "returns", "zorder": ["return_id", "return_date"]},
    {"schema": "silver", "table": "inventory", "zorder": ["store_id", "product_id"]},
    {"schema": "silver", "table": "stores", "zorder": ["store_id"]},
    # Bronze layer raw delta tables
    {"schema": "bronze", "table": "sales_transactions", "zorder": ["_ingested_at"]},
    {"schema": "bronze", "table": "customers", "zorder": ["_ingested_at"]},
    {"schema": "bronze", "table": "products", "zorder": ["_ingested_at"]},
    {"schema": "bronze", "table": "returns", "zorder": ["_ingested_at"]},
    {"schema": "bronze", "table": "inventory", "zorder": ["_ingested_at"]},
    {"schema": "bronze", "table": "stores", "zorder": ["_ingested_at"]}
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Execute OPTIMIZE, Z-ORDER & VACUUM

# COMMAND ----------

results = {
    "status": "SUCCESS",
    "timestamp": datetime.now().isoformat(),
    "completed": [],
    "skipped": [],
    "errors": []
}

for item in MAINTENANCE_TARGETS:
    full_table_name = f"{CATALOG}.{item['schema']}.{item['table']}"
    
    if not spark.catalog.tableExists(full_table_name):
        logger.warning("Skipping %s (table does not exist yet)", full_table_name)
        results["skipped"].append(full_table_name)
        continue
        
    logger.info("Processing maintenance for %s", full_table_name)
    try:
        # 1. OPTIMIZE with Z-ORDER
        zorder_clause = f"ZORDER BY ({', '.join(item['zorder'])})" if item.get("zorder") else ""
        optimize_sql = f"OPTIMIZE {full_table_name} {zorder_clause}"
        logger.info("Executing: %s", optimize_sql)
        spark.sql(optimize_sql)
        
        # 2. VACUUM historical files
        vacuum_sql = f"VACUUM {full_table_name} RETAIN {RETENTION_HOURS} HOURS"
        logger.info("Executing: %s", vacuum_sql)
        spark.sql(vacuum_sql)
        
        results["completed"].append(full_table_name)
        logger.info("Maintenance successfully completed for %s", full_table_name)
        
    except Exception as exc:
        err = f"Failed maintenance on {full_table_name}: {str(exc)}"
        logger.error("Maintenance failed on %s: %s", full_table_name, exc, exc_info=True)
        results["errors"].append(err)
        results["status"] = "PARTIAL"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Summary

# COMMAND ----------

logger.info("Delta Maintenance Summary:\n%s", json.dumps(results, indent=2))

dbutils.notebook.exit(json.dumps(results))

