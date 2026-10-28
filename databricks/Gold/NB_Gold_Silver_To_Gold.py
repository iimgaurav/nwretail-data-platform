# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold Layer - Silver to Gold Transformations
# MAGIC
# MAGIC Production-grade dimensional model with:
# MAGIC - Dimensions: dim_customer (SCD2 current), dim_product (SCD2 current), dim_store, dim_date
# MAGIC - Facts: fact_sales, fact_returns, fact_inventory
# MAGIC
# MAGIC All tables use Delta Lake stored in ADLS Gold container with Z-ordering, data quality checks, and audit columns.

# COMMAND ----------

dbutils.widgets.text("auth_mode", "auto")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("batch_id", "")

# COMMAND ----------

# MAGIC %run /Users/saddamhusain23@gmail.com/NorthWind_Retail/Silver/NB_Silver_Common

# COMMAND ----------

import json
import logging
import sys
from datetime import datetime, date, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class DatabricksLogFormatter(logging.Formatter):
    """
    Production log formatter for Databricks / Azure Monitor.
    Emits structured, human-readable and machine-parseable log lines:
    ISO-Timestamp [LEVEL] [logger] [run_id] message
    """
    def __init__(self, run_id: str = ""):
        super().__init__()
        self.run_id = run_id

    def format(self, record):
        ts = self.formatTime(record, "%Y-%m-%dT%H:%M:%S")
        ctx = f" [run_id={self.run_id}]" if self.run_id else ""
        base = f"{ts} [{record.levelname:^7}] [{record.name}]{ctx} {record.getMessage()}"
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base

def get_notebook_logger(name: str, run_id: str = "") -> logging.Logger:
    """
    Creates or configures a production-grade logger attached to sys.stdout.
    Avoids duplicate handlers across Databricks notebook cell re-runs.
    """
    _logger = logging.getLogger(name)
    _logger.setLevel(logging.INFO)
    _logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(DatabricksLogFormatter(run_id=run_id))
    _logger.addHandler(handler)
    _logger.propagate = False
    return _logger

CATALOG = CATALOG
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
GOLD_PATH = "abfss://gold@nwretaildatalake65.dfs.core.windows.net"

run_id = _arg("run_id") or str(uuid.uuid4())
batch_id = _arg("batch_id") or run_id
started = datetime.now()

logger = get_notebook_logger("nwretail.gold.transformation", run_id=run_id)
logger.info("Gold transformation starting (run_id=%s, target=%s)", run_id, GOLD_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Schema & Save Helper
# MAGIC Ensures schema exists and tables are written to external ADLS location.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD} COMMENT 'Gold layer - curated dimensional models for analytics'")

def save_gold_table(df, tbl_name, zorder_cols=None, comment=None):
    """
    Saves a Gold DataFrame as an external Delta table pointing directly to the ADLS Gold container.
    Automatically migrates any previously managed UC metastore tables to the external ADLS path.
    """
    full_table = f"{GOLD}.{tbl_name}"
    target_path = f"{GOLD_PATH}/{tbl_name}"
    
    try:
        desc = spark.sql(f"DESCRIBE EXTENDED {full_table}").collect()
        t_type = next((r.data_type for r in desc if r.col_name == "Type"), "")
        t_loc = next((r.data_type for r in desc if r.col_name == "Location"), "")
        if t_type == "MANAGED" or (t_loc and not t_loc.startswith(GOLD_PATH)):
            logger.info("Migrating %s from %s to ADLS %s", full_table, t_loc or 'default managed', target_path)
            spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    except Exception as e:
        logger.debug("Table pre-check for %s: %s", full_table, e)

    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("path", target_path) \
        .saveAsTable(full_table)
    
    if comment:
        spark.sql(f"ALTER TABLE {full_table} SET TBLPROPERTIES ('comment' = '{comment}')")
    if zorder_cols:
        spark.sql(f"OPTIMIZE {full_table} ZORDER BY ({', '.join(zorder_cols)})")
        
    row_count = df.count()
    logger.info("Table written: %s -> %s (rows=%d)", full_table, target_path, row_count)
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_date
# MAGIC Create date dimension for 2020-2030

# COMMAND ----------

start_date = date(2020, 1, 1)
end_date = date(2030, 12, 31)
dates = []
current = start_date
while current <= end_date:
    dates.append((current,))
    current += timedelta(days=1)

date_df = spark.createDataFrame(dates, ["date_key"])
date_dim = date_df.select(
    F.col("date_key"),
    F.year("date_key").alias("year"),
    F.quarter("date_key").alias("quarter"),
    F.month("date_key").alias("month"),
    F.date_format("date_key", "MMMM").alias("month_name"),
    F.dayofmonth("date_key").alias("day_of_month"),
    F.dayofweek("date_key").alias("day_of_week"),
    F.date_format("date_key", "EEEE").alias("day_name"),
    F.weekofyear("date_key").alias("week_of_year"),
    F.when(F.dayofweek("date_key").isin([1,7]), True).otherwise(False).alias("is_weekend"),
    F.when(F.month("date_key").isin([12,1,2]), "Winter")
     .when(F.month("date_key").isin([3,4,5]), "Spring")
     .when(F.month("date_key").isin([6,7,8]), "Summer")
     .otherwise("Fall").alias("season"),
    F.current_timestamp().alias("_load_timestamp"),
    F.lit(run_id).alias("_batch_id")
)

save_gold_table(date_dim, "dim_date", comment="Date dimension")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_store
# MAGIC Current stores only (non-SCD2)

# COMMAND ----------

dim_store = spark.table(f"{SILVER}.stores").select(
    F.col("store_id").alias("store_key"),
    F.col("store_name"),
    F.col("city"),
    F.col("region"),
    F.col("store_type"),
    F.col("manager_name"),
    F.col("open_date"),
    F.current_timestamp().alias("_load_timestamp"),
    F.lit(run_id).alias("_batch_id")
).distinct()

save_gold_table(dim_store, "dim_store", zorder_cols=["store_key"], comment="Store dimension")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_customer
# MAGIC SCD2 - only current versions

# COMMAND ----------

dim_customer = spark.table(f"{SILVER}.customers").filter(F.col("is_current") == True).select(
    F.col("customer_id").alias("customer_key"),
    F.col("name"),
    F.col("email"),
    F.col("country"),
    F.col("signup_date"),
    F.col("segment"),
    F.col("effective_from").alias("valid_from"),
    F.current_timestamp().alias("_load_timestamp"),
    F.lit(run_id).alias("_batch_id")
).distinct()

save_gold_table(dim_customer, "dim_customer", zorder_cols=["customer_key"], comment="Customer dimension - current version only")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_product
# MAGIC SCD2 - only current versions

# COMMAND ----------

dim_product = spark.table(f"{SILVER}.products").filter(F.col("is_current") == True).select(
    F.col("product_id").alias("product_key"),
    F.col("product_name"),
    F.col("category"),
    F.col("brand"),
    F.col("cost_price"),
    F.col("effective_from").alias("valid_from"),
    F.current_timestamp().alias("_load_timestamp"),
    F.lit(run_id).alias("_batch_id")
).distinct()

save_gold_table(dim_product, "dim_product", zorder_cols=["product_key"], comment="Product dimension - current version only")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: fact_sales
# MAGIC Sales transactions with dimensions

# COMMAND ----------

sales = spark.table(f"{SILVER}.sales_transactions")
cust = spark.table(f"{GOLD}.dim_customer")
prod = spark.table(f"{GOLD}.dim_product")
stor = spark.table(f"{GOLD}.dim_store")

fact_sales = sales.alias("s") \
    .join(cust.alias("c"), F.col("s.customer_id") == F.col("c.customer_key"), "left") \
    .join(prod.alias("p"), F.col("s.product_id") == F.col("p.product_key"), "left") \
    .join(stor.alias("st"), F.col("s.store_id") == F.col("st.store_key"), "left") \
    .select(
        F.col("s.transaction_id").alias("sales_transaction_key"),
        F.col("s.transaction_date").alias("date_key"),
        F.col("c.customer_key"),
        F.col("p.product_key"),
        F.col("st.store_key"),
        F.col("s.quantity"),
        F.col("s.price").alias("unit_price"),
        F.col("s.gross_amount"),
        (F.col("s.quantity") * F.col("p.cost_price")).alias("cost_amount"),
        (F.col("s.gross_amount") - (F.col("s.quantity") * F.col("p.cost_price"))).alias("profit_amount"),
        F.current_timestamp().alias("_load_timestamp"),
        F.lit(run_id).alias("_batch_id")
    )

save_gold_table(fact_sales, "fact_sales", zorder_cols=["date_key", "store_key", "product_key"], comment="Sales fact table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: fact_returns
# MAGIC Returns with sales context

# COMMAND ----------

returns = spark.table(f"{SILVER}.returns")
facts = spark.table(f"{GOLD}.fact_sales")

fact_returns = returns.alias("r") \
    .join(facts.alias("fs"), F.col("r.transaction_id") == F.col("fs.sales_transaction_key"), "left") \
    .select(
        F.col("r.return_id").alias("return_key"),
        F.col("r.transaction_id").alias("sales_transaction_key"),
        F.col("r.return_date").alias("date_key"),
        F.col("fs.customer_key"),
        F.col("fs.product_key"),
        F.col("fs.store_key"),
        F.col("r.quantity_returned"),
        F.col("r.refund_amount"),
        F.col("r.reason"),
        F.current_timestamp().alias("_load_timestamp"),
        F.lit(run_id).alias("_batch_id")
    )

save_gold_table(fact_returns, "fact_returns", zorder_cols=["date_key", "store_key"], comment="Returns fact table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: fact_inventory
# MAGIC Daily inventory snapshots

# COMMAND ----------

inv = spark.table(f"{SILVER}.inventory")

fact_inventory = inv.alias("i") \
    .join(prod.alias("p"), F.col("i.product_id") == F.col("p.product_key"), "left") \
    .join(stor.alias("st"), F.col("i.store_id") == F.col("st.store_key"), "left") \
    .select(
        F.col("i.snapshot_date").alias("date_key"),
        F.col("p.product_key"),
        F.col("st.store_key"),
        F.col("i.stock_on_hand"),
        F.col("i.reorder_level"),
        F.col("i.is_below_reorder"),
        (F.col("p.cost_price") * F.col("i.stock_on_hand")).alias("inventory_value"),
        F.current_timestamp().alias("_load_timestamp"),
        F.lit(run_id).alias("_batch_id")
    )

save_gold_table(fact_inventory, "fact_inventory", zorder_cols=["date_key", "store_key", "product_key"], comment="Inventory fact table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Analytics Views

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_sales_summary AS
SELECT 
  d.year, d.month, d.month_name, d.quarter,
  st.store_key, st.store_name, st.region, st.store_type,
  c.customer_key, c.segment, c.country,
  p.product_key, p.product_name, p.category, p.brand,
  SUM(fs.quantity) as total_quantity,
  SUM(fs.gross_amount) as total_sales,
  SUM(fs.cost_amount) as total_cost,
  SUM(fs.profit_amount) as total_profit,
  COUNT(DISTINCT fs.sales_transaction_key) as transaction_count
FROM {GOLD}.fact_sales fs
JOIN {GOLD}.dim_date d ON fs.date_key = d.date_key
LEFT JOIN {GOLD}.dim_store st ON fs.store_key = st.store_key
LEFT JOIN {GOLD}.dim_customer c ON fs.customer_key = c.customer_key
LEFT JOIN {GOLD}.dim_product p ON fs.product_key = p.product_key
GROUP BY d.year, d.month, d.month_name, d.quarter, st.store_key, st.store_name, st.region, st.store_type, 
         c.customer_key, c.segment, c.country, p.product_key, p.product_name, p.category, p.brand
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_product_performance AS
SELECT 
  p.product_key, p.product_name, p.category, p.brand, p.cost_price,
  COUNT(DISTINCT fs.sales_transaction_key) as transactions,
  SUM(fs.quantity) as units_sold,
  SUM(fs.gross_amount) as revenue,
  SUM(fs.profit_amount) as profit,
  AVG(fs.unit_price) as avg_selling_price,
  CASE WHEN SUM(fs.gross_amount) > 0 THEN SUM(fs.profit_amount)/SUM(fs.gross_amount) ELSE 0 END as profit_margin
FROM {GOLD}.fact_sales fs
LEFT JOIN {GOLD}.dim_product p ON fs.product_key = p.product_key
GROUP BY p.product_key, p.product_name, p.category, p.brand, p.cost_price
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.vw_store_performance AS
SELECT 
  st.store_key, st.store_name, st.city, st.region, st.store_type,
  COUNT(DISTINCT fs.sales_transaction_key) as transactions,
  COUNT(DISTINCT fs.customer_key) as unique_customers,
  SUM(fs.quantity) as units_sold,
  SUM(fs.gross_amount) as revenue,
  SUM(fs.profit_amount) as profit,
  AVG(fs.gross_amount) as avg_transaction_value
FROM {GOLD}.fact_sales fs
LEFT JOIN {GOLD}.dim_store st ON fs.store_key = st.store_key
GROUP BY st.store_key, st.store_name, st.city, st.region, st.store_type
""")

logger.info("Analytics views created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

result = {
    "run_id": run_id,
    "batch_id": batch_id,
    "status": "SUCCESS",
    "timestamp": datetime.now().isoformat(),
    "tables": {
        "dim_date": spark.table(f"{GOLD}.dim_date").count(),
        "dim_store": spark.table(f"{GOLD}.dim_store").count(),
        "dim_customer": spark.table(f"{GOLD}.dim_customer").count(),
        "dim_product": spark.table(f"{GOLD}.dim_product").count(),
        "fact_sales": spark.table(f"{GOLD}.fact_sales").count(),
        "fact_returns": spark.table(f"{GOLD}.fact_returns").count(),
        "fact_inventory": spark.table(f"{GOLD}.fact_inventory").count()
    }
}

logger.info("Gold Transformation Summary:\n%s", json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))