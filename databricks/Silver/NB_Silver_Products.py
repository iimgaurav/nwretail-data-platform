# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Products
# MAGIC
# MAGIC Thin wrapper around `NB_Silver_Common`. All trimming, casting, dedup, data-quality
# MAGIC rules and write logic are declared in `ENTITY_SPEC["products"]` - this notebook only
# MAGIC binds the entity name.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Business key | `product_id` |
# MAGIC | Write strategy | SCD2 (cost_price) |
# MAGIC | Upstream references | none |
# MAGIC | Bronze rows | 30 |
# MAGIC
# MAGIC Invoked by `NB_Bronze_To_Silver` with a shared `run_id` / `batch_id`, or run
# MAGIC standalone (identifiers are generated automatically).

# COMMAND ----------

# Widgets must be declared before %run: NB_Silver_Common resolves the storage auth
# mode at import time, and passed arguments only bind to declared widgets.
dbutils.widgets.text("auth_mode", "auto")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("batch_id", "")

# COMMAND ----------

# MAGIC %run ./NB_Silver_Common

# COMMAND ----------

import json

result = execute_entity(
    "products",
    run_id=_arg("run_id"),
    batch_id=_arg("batch_id"),
    notebook="NB_Silver_Products",
)

dbutils.notebook.exit(json.dumps(result, default=str))
