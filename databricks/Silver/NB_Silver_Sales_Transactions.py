# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Sales Transactions
# MAGIC
# MAGIC Thin wrapper around `NB_Silver_Common`. All trimming, casting, dedup, data-quality
# MAGIC rules and write logic are declared in `ENTITY_SPEC["sales_transactions"]` - this notebook only
# MAGIC binds the entity name.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Business key | `transaction_id` |
# MAGIC | Write strategy | MERGE |
# MAGIC | Upstream references | `customers`, `products`, `stores` |
# MAGIC | Bronze rows | 1,500 |
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
    "sales_transactions",
    run_id=_arg("run_id"),
    batch_id=_arg("batch_id"),
    notebook="NB_Silver_Sales_Transactions",
)

dbutils.notebook.exit(json.dumps(result, default=str))
