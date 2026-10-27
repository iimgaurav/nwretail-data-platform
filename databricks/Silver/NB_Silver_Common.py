# Databricks notebook source
# MAGIC %md
# MAGIC # NB_Silver_Common (v2)
# MAGIC
# MAGIC Reusable Bronze -> Silver framework for **NorthWind Retail**.
# MAGIC
# MAGIC Entity notebooks declare *what* to clean and validate in `ENTITY_SPEC`.
# MAGIC This notebook owns *how*: read, cast, dedupe, DQ, MERGE, SCD2, audit.
# MAGIC
# MAGIC ## Authentication
# MAGIC Primary path is Unity Catalog: external locations `loc_nwretail_bronze` and
# MAGIC `loc_nwretail_silver` via credential `cred_nwretail_adls_mi` (access connector MI).
# MAGIC No storage keys appear in source. `auth_mode` accepts `unity-catalog`,
# MAGIC `account-key` or `auto`; `auto` probes UC and **logs** any fallback.
# MAGIC An explicit mode never falls back - it raises.
# MAGIC
# MAGIC ## Known limitation: `_ingest_ts`
# MAGIC Bronze lands one immutable CSV per entity and carries no `_ingest_ts`. We derive
# MAGIC it from the source file's modification time and record `_ingest_ts_source`.
# MAGIC That is a proxy, not true ingestion order. The durable fix is for Bronze to stamp
# MAGIC `_ingest_ts` on write.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import time
import uuid
import json
from datetime import datetime

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType,
)
from delta.tables import DeltaTable

STORAGE_ACCOUNT  = "nwretaildatalake65"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SECRET_SCOPE = "nwretail"
SECRET_KEY   = "adls-account-key"

PIPELINE_NAME = "NorthWind_Retail"
LAYER         = "silver"

CATALOG = "nwretail_databricks_dev"

BRONZE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
OPS_PATH        = f"{SILVER_PATH}/_ops"
QUARANTINE_PATH = f"{SILVER_PATH}/_quarantine"

RUN_LOG_PATH    = f"{OPS_PATH}/run_log"
DQ_SUMMARY_PATH = f"{OPS_PATH}/data_quality_summary"
WATERMARK_PATH  = f"{OPS_PATH}/entity_watermarks"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Authentication (explicit, never silent)

# COMMAND ----------


def _arg(name, default=""):
    """Read a notebook parameter even when no widget has been declared."""
    try:
        return dbutils.widgets.get(name) or default
    except Exception:
        return default


def _probe_uc():
    try:
        dbutils.fs.ls(BRONZE_PATH)
        dbutils.fs.ls(SILVER_PATH)
        return True
    except Exception:
        return False


def _configure_auth():
    """Resolve the storage auth path. Returns (mode, human detail). No secrets printed."""
    mode = _arg("auth_mode", "auto")

    if mode == "unity-catalog":
        if not _probe_uc():
            raise RuntimeError(
                "auth_mode=unity-catalog but external locations are unreachable. "
                "Check GRANT READ/WRITE on loc_nwretail_bronze and loc_nwretail_silver, "
                "and that cred_nwretail_adls_mi holds Storage Blob Data Contributor "
                "on the bronze and silver containers."
            )
        return "unity-catalog", "external locations via cred_nwretail_adls_mi"

    if mode == "account-key":
        key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
        try:
            spark.conf.set(
                f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", key
            )
        except Exception as exc:
            raise RuntimeError(
                "auth_mode=account-key cannot be applied on this compute (serverless "
                "rejects storage credentials in Spark config). Use auth_mode="
                "'unity-catalog' with external locations instead."
            ) from exc
        return "account-key", f"secret scope {SECRET_SCOPE}/{SECRET_KEY}"

    if mode == "auto":
        if _probe_uc():
            return "unity-catalog", "external locations via cred_nwretail_adls_mi"
        print(
            "[auth] Unity Catalog probe failed - falling back to account key. "
            "This fallback is logged; investigate the external location grant."
        )
        key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
        try:
            spark.conf.set(
                f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", key
            )
        except Exception as exc:
            raise RuntimeError(
                "AUTH: both auth paths are unavailable. Unity Catalog external "
                "locations are unreachable AND storage credentials cannot be set "
                "on this compute. Fix the external location grants."
            ) from exc
        return "account-key", f"FALLBACK secret scope {SECRET_SCOPE}/{SECRET_KEY}"

    raise ValueError(f"Unknown auth_mode={mode!r}")


AUTH_MODE, AUTH_DETAIL = _configure_auth()
print(f"auth_mode  = {AUTH_MODE}")
print(f"auth_detail= {AUTH_DETAIL}")
print(f"bronze     = {BRONZE_PATH}")
print(f"silver     = {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Explicit bronze schemas (read everything as STRING, cast in silver)

# COMMAND ----------


def _st(*cols):
    return StructType([StructField(c, StringType(), True) for c in cols])


BRONZE_SCHEMAS = {
    "sales_transactions": _st(
        "transaction_id", "transaction_date", "customer_id", "product_id",
        "quantity", "price", "store", "region",
    ),
    "customers": _st(
        "customer_id", "name", "email", "country", "signup_date", "segment",
    ),
    "products": _st(
        "product_id", "product_name", "category", "brand", "cost_price",
    ),
    "stores": _st(
        "store_id", "store_name", "city", "region", "store_type",
        "manager_name", "open_date",
    ),
    "returns": _st(
        "return_id", "transaction_id", "return_date", "quantity_returned",
        "reason", "refund_amount",
    ),
    "inventory": _st(
        "snapshot_date", "product_id", "store_id", "stock_on_hand", "reorder_level",
    ),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardisation maps
# MAGIC Explicit dictionary lookups only. Unmapped values pass through untouched -
# MAGIC no blanket string replacement that could corrupt a valid business value.

# COMMAND ----------

COUNTRY_MAP = {
    "usa": "United States",
    "us": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom",
    "great britain": "United Kingdom",
    "u.k.": "United Kingdom",
    "deutschland": "Germany",
    "aus": "Australia",
}

SEGMENT_MAP = {
    "consumer": "Consumer",
    "corporate": "Corporate",
    "small business": "Small Business",
    "smb": "Small Business",
    "sme": "Small Business",
}

REGION_MAP = {
    "n": "North", "s": "South", "e": "East", "w": "West",
    "north": "North", "south": "South", "east": "East", "west": "West",
    "online": "Online", "e-commerce": "Online", "ecommerce": "Online",
}

STORE_TYPE_MAP = {
    "online": "Online",
    "physical": "Physical",
    "brick and mortar": "Physical",
    "store": "Physical",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Entity registry

# COMMAND ----------

ENTITY_SPEC = {
    "stores": {
        "file": "stores/stores.csv",
        "key": ["store_id"],
        "business_columns": [
            "store_id", "store_name", "city", "region", "store_type",
            "manager_name", "open_date",
        ],
        "upper": ["store_id"],
        "trim": ["store_name", "city", "region", "store_type", "manager_name"],
        "lower": [],
        "maps": {"region": REGION_MAP, "store_type": STORE_TYPE_MAP},
        "casts": {"open_date": "date"},
        "derived": {},
        "renames": {},
        "references": [],
        "scd2": None,
        "partition_by": [],
        "dq_rules": [
            ("STORE_ID_NOT_NULL", "store_id IS NOT NULL AND length(trim(store_id)) > 0"),
            ("STORE_NAME_NOT_NULL", "store_name IS NOT NULL AND length(trim(store_name)) > 0"),
            ("STORE_TYPE_VALID", "store_type IN ('Online','Physical')"),
            ("CITY_NOT_NULL", "city IS NOT NULL AND length(trim(city)) > 0"),
            ("OPEN_DATE_VALID", "open_date IS NOT NULL AND open_date <= current_date()"),
        ],
    },
    "customers": {
        "file": "customers/customers.csv",
        "key": ["customer_id"],
        "business_columns": [
            "customer_id", "name", "email", "country", "signup_date", "segment",
        ],
        "upper": ["customer_id"],
        "trim": ["name", "email", "country", "segment"],
        "lower": ["email"],
        "maps": {"country": COUNTRY_MAP, "segment": SEGMENT_MAP},
        "casts": {"signup_date": "date"},
        "derived": {},
        "renames": {},
        "references": [],
        "scd2": {"tracked": ["country", "segment"]},
        "partition_by": [],
        "dq_rules": [
            ("CUSTOMER_ID_NOT_NULL", "customer_id IS NOT NULL AND length(trim(customer_id)) > 0"),
            ("NAME_NOT_NULL", "name IS NOT NULL AND length(trim(name)) > 0"),
            ("EMAIL_NOT_NULL", "email IS NOT NULL AND length(email) > 0"),
            ("EMAIL_FORMAT_VALID", "email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'"),
            # Small Business is a real segment in the source - must not be quarantined.
            ("SEGMENT_VALID", "segment IN ('Consumer','Corporate','Small Business')"),
            ("COUNTRY_VALID", "country IS NOT NULL AND length(trim(country)) > 0"),
            ("SIGNUP_DATE_VALID", "signup_date IS NOT NULL AND signup_date <= current_date()"),
        ],
    },
    "products": {
        "file": "products/products.csv",
        "key": ["product_id"],
        "business_columns": [
            "product_id", "product_name", "category", "brand", "cost_price",
        ],
        "upper": ["product_id"],
        "trim": ["product_name", "category", "brand"],
        "lower": [],
        "maps": {},
        "casts": {"cost_price": "decimal(18,2)"},
        "derived": {},
        "renames": {},
        "references": [],
        "scd2": {"tracked": ["cost_price"]},
        "partition_by": [],
        "dq_rules": [
            ("PRODUCT_ID_NOT_NULL", "product_id IS NOT NULL AND length(trim(product_id)) > 0"),
            ("PRODUCT_NAME_NOT_NULL", "product_name IS NOT NULL AND length(trim(product_name)) > 0"),
            ("CATEGORY_NOT_NULL", "category IS NOT NULL AND length(trim(category)) > 0"),
            ("COST_PRICE_NON_NEGATIVE", "cost_price IS NOT NULL AND cost_price >= 0"),
        ],
    },
    "sales_transactions": {
        "file": "sales_transactions/sales_transactions.csv",
        "key": ["transaction_id"],
        "business_columns": [
            "transaction_id", "transaction_date", "customer_id", "product_id",
            "quantity", "price", "store_name", "region",
        ],
        "upper": ["transaction_id", "customer_id", "product_id"],
        "trim": ["store_name", "region"],
        "lower": [],
        "maps": {"region": REGION_MAP},
        "casts": {
            "transaction_date": "date",
            "quantity": "int",
            "price": "decimal(18,2)",
        },
        "derived": {"gross_amount": "quantity * price"},
        "renames": {"store": "store_name"},
        "references": [
            {"entity": "customers", "left_on": "customer_id",
             "right_on": "customer_id", "flag": "_customer_found",
             "select": {"signup_date": "_signup_date"}},
            {"entity": "products", "left_on": "product_id",
             "right_on": "product_id", "flag": "_product_found"},
            {"entity": "stores", "left_on": "store_name", "right_on": "store_name",
             "flag": "_store_found", "select": {"store_id": "store_id"}},
        ],
        "scd2": None,
        "partition_by": [],
        "dq_rules": [
            ("TRANSACTION_ID_NOT_NULL", "transaction_id IS NOT NULL AND length(trim(transaction_id)) > 0"),
            ("REQUIRED_FIELDS_NOT_NULL",
             "customer_id IS NOT NULL AND product_id IS NOT NULL "
             "AND store_name IS NOT NULL AND region IS NOT NULL"),
            ("QUANTITY_POSITIVE", "quantity IS NOT NULL AND quantity > 0"),
            ("PRICE_POSITIVE", "price IS NOT NULL AND price > 0"),
            ("TRANSACTION_DATE_VALID",
             "transaction_date IS NOT NULL AND transaction_date <= current_date()"),
            ("CUSTOMER_EXISTS", "_customer_found = true"),
            ("PRODUCT_EXISTS", "_product_found = true"),
            ("STORE_MAPPED", "_store_found = true AND store_id IS NOT NULL"),
            ("SALES_AFTER_SIGNUP", "transaction_date >= _signup_date"),
        ],
    },
    "returns": {
        "file": "returns/returns.csv",
        "key": ["return_id"],
        "business_columns": [
            "return_id", "transaction_id", "return_date",
            "quantity_returned", "reason", "refund_amount",
        ],
        "upper": ["return_id", "transaction_id"],
        "trim": ["reason"],
        "lower": ["reason"],
        "maps": {},
        "casts": {
            "return_date": "date",
            "quantity_returned": "int",
            "refund_amount": "decimal(18,2)",
        },
        "derived": {},
        "renames": {},
        "references": [
            {"entity": "sales_transactions", "left_on": "transaction_id",
             "right_on": "transaction_id", "flag": "_sale_found",
             "select": {"quantity": "_sold_quantity",
                        "transaction_date": "_sold_date",
                        "customer_id": "_sale_customer_id"}},
        ],
        "scd2": None,
        "partition_by": [],
        "dq_rules": [
            ("RETURN_ID_NOT_NULL", "return_id IS NOT NULL AND length(trim(return_id)) > 0"),
            ("TRANSACTION_ID_NOT_NULL", "transaction_id IS NOT NULL"),
            ("TRANSACTION_EXISTS", "_sale_found = true"),
            ("QUANTITY_RETURNED_POSITIVE",
             "quantity_returned IS NOT NULL AND quantity_returned > 0"),
            ("REFUND_NON_NEGATIVE", "refund_amount IS NOT NULL AND refund_amount >= 0"),
            ("RETURN_DATE_VALID", "return_date IS NOT NULL AND return_date <= current_date()"),
            ("RETURN_AFTER_SALE", "return_date >= _sold_date"),
            ("RETURN_QTY_LE_SOLD", "quantity_returned <= _sold_quantity"),
        ],
    },
    "inventory": {
        "file": "inventory/inventory.csv",
        "key": ["snapshot_date", "product_id", "store_id"],
        "business_columns": [
            "snapshot_date", "product_id", "store_id", "stock_on_hand", "reorder_level",
        ],
        "upper": ["product_id", "store_id"],
        "trim": [],
        "lower": [],
        "maps": {},
        "casts": {
            "snapshot_date": "date",
            "stock_on_hand": "int",
            "reorder_level": "int",
        },
        "derived": {
            "is_below_reorder":
                "CASE WHEN stock_on_hand <= reorder_level THEN 1 ELSE 0 END",
        },
        "renames": {},
        "references": [
            {"entity": "products", "left_on": "product_id",
             "right_on": "product_id", "flag": "_product_found"},
            {"entity": "stores", "left_on": "store_id",
             "right_on": "store_id", "flag": "_store_found"},
        ],
        "scd2": None,
        # 21 snapshots x 210 rows, ~263 KB total: partitioning would only create
        # tiny files. Deliberately unpartitioned.
        "partition_by": [],
        "dq_rules": [
            ("COMPOSITE_KEY_NOT_NULL",
             "snapshot_date IS NOT NULL AND product_id IS NOT NULL AND store_id IS NOT NULL"),
            ("STOCK_NON_NEGATIVE", "stock_on_hand IS NOT NULL AND stock_on_hand >= 0"),
            ("REORDER_NON_NEGATIVE", "reorder_level IS NOT NULL AND reorder_level >= 0"),
            ("SNAPSHOT_DATE_VALID", "snapshot_date IS NOT NULL AND snapshot_date <= current_date()"),
            ("PRODUCT_EXISTS", "_product_found = true"),
            ("STORE_EXISTS", "_store_found = true"),
        ],
    },
}

ENTITY_ORDER = [
    "stores", "customers", "products",
    "sales_transactions", "returns", "inventory",
]

# Canonical wrapper names, used for run_log.notebook when a caller does not pass one.
ENTITY_NOTEBOOK = {
    "stores": "NB_Silver_Stores",
    "customers": "NB_Silver_Customers",
    "products": "NB_Silver_Products",
    "sales_transactions": "NB_Silver_Sales_Transactions",
    "returns": "NB_Silver_Returns",
    "inventory": "NB_Silver_Inventory",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Names

# COMMAND ----------


def table_name(entity):
    return f"{CATALOG}.silver.{entity}"


def quarantine_table(entity):
    return f"{CATALOG}.silver_quarantine.{entity}"


def target_path(entity):
    return f"{SILVER_PATH}/{entity}/"


def bronze_path(entity):
    return f"{BRONZE_PATH}/{ENTITY_SPEC[entity]['file']}"


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Read + schema validation

# COMMAND ----------


def read_bronze(entity):
    df = (
        spark.read.schema(BRONZE_SCHEMAS[entity])
        .option("header", True)
        .option("enforceSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(bronze_path(entity))
    )
    print(f"[read] {entity}: {df.count():,} rows <- {bronze_path(entity)}")
    return df


def validate_schema(df, entity):
    expected = {f.name: f.dataType.simpleString() for f in BRONZE_SCHEMAS[entity].fields}
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    if missing or extra:
        raise ValueError(
            f"Schema mismatch for {entity}: missing={missing} unexpected={extra}. "
            "Review the source change, then update BRONZE_SCHEMAS."
        )
    print(f"[schema] {entity}: OK ({len(expected)} columns)")
    return True


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Metadata, cleaning, casting, hashing

# COMMAND ----------


def _ingest_timestamp(entity):
    """Proxy ingestion time from the source file mtime. See header caveat."""
    files = dbutils.fs.ls(bronze_path(entity))
    if not files:
        raise FileNotFoundError(f"No bronze file at {bronze_path(entity)}")
    ts = max(f.modificationTime for f in files) / 1000.0
    return datetime.fromtimestamp(ts)


def add_ingestion_metadata(df, entity, batch_id):
    ingest_ts = _ingest_timestamp(entity)
    key = ENTITY_SPEC[entity]["key"]
    return (
        df.withColumn("_ingest_ts", F.lit(ingest_ts).cast("timestamp"))
        .withColumn("_ingest_ts_source", F.lit("file_mtime"))
        .withColumn("_source_file", F.lit(bronze_path(entity)))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_load_timestamp", F.current_timestamp())
        .withColumn(
            "_source_key",
            F.concat_ws("|", *[F.col(k).cast("string") for k in key]),
        )
    )


def _apply_map(col_name, mapping):
    """Explicit map only; unmapped values fall through to the trimmed original."""
    expr = None
    for src, dst in mapping.items():
        cond = F.lower(F.trim(F.col(col_name))) == src
        expr = F.when(cond, F.lit(dst)) if expr is None else expr.when(cond, F.lit(dst))
    if expr is None:
        return F.trim(F.col(col_name))
    return expr.otherwise(F.trim(F.col(col_name)))


def trim_and_standardize(df, entity):
    spec = ENTITY_SPEC[entity]
    for src, dst in spec.get("renames", {}).items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)
    for c in spec["trim"]:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    for c in spec["upper"]:
        if c in df.columns:
            df = df.withColumn(c, F.upper(F.trim(F.col(c))))
    for c in spec["lower"]:
        if c in df.columns:
            df = df.withColumn(c, F.lower(F.trim(F.col(c))))
    for c, mapping in spec["maps"].items():
        if c in df.columns:
            df = df.withColumn(c, _apply_map(c, mapping))
    return df


def apply_casts(df, entity):
    """try_cast so a bad value becomes NULL *and* is recoverable via _raw_<col>."""
    spec = ENTITY_SPEC[entity]
    cast_cols = list(spec["casts"].keys())
    for c, typ in spec["casts"].items():
        df = df.withColumn(f"_raw_{c}", F.col(c))
        df = df.withColumn(c, F.expr(f"try_cast(`{c}` as {typ})"))

    if cast_cols:
        # concat_ws() ignores NULLs and yields "" when every cast succeeded; nullif()
        # turns that back into NULL so `_cast_error_column IS NULL` remains the
        # "no cast failure" predicate. Reporting every failing column (not just the
        # first) makes multi-column corruption diagnosable in one pass.
        expr = F.nullif(
            F.concat_ws(
                ",",
                *[
                    F.when(
                        F.col(f"_raw_{c}").isNotNull() & F.col(c).isNull(), F.lit(c)
                    )
                    for c in cast_cols
                ],
            ),
            F.lit(""),
        )
        df = df.withColumn("_cast_error_column", expr)

    for col, sql in spec.get("derived", {}).items():
        df = df.withColumn(col, F.expr(sql))
    return df


def drop_cast_artifacts(df, entity):
    drop = [f"_raw_{c}" for c in ENTITY_SPEC[entity]["casts"]]
    drop = [c for c in drop + ["_cast_error_column", "_reject_reason"] if c in df.columns]
    return df.drop(*drop)


def add_row_hash(df, entity):
    """SHA-256 over the business columns: fixed order, explicit null sentinel."""
    cols = ENTITY_SPEC[entity]["business_columns"]
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\\N")) for c in cols]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("\u0001", *parts), 256))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Deduplication

# COMMAND ----------


def deduplicate_exact(df, entity):
    """Stage A - collapse identical business rows on _row_hash."""
    before = df.count()
    out = df.dropDuplicates(["_row_hash"])
    removed = before - out.count()
    print(f"[dedupe-exact] {entity}: removed {removed:,}")
    return out, removed


def deduplicate_business_key(df, entity):
    """Stage B - latest row per key.

    Null-key rows are never silently dropped: they are returned separately and
    routed to quarantine by the caller.
    """
    key = ENTITY_SPEC[entity]["key"]
    key_present = F.lit(True)
    for k in key:
        key_present = key_present & F.col(k).isNotNull()

    with_key = df.filter(key_present)
    null_key = df.filter(~key_present)

    window = Window.partitionBy(*key).orderBy(
        F.col("_ingest_ts").desc_nulls_last(),
        F.col("_row_hash").desc(),
    )
    ranked = with_key.withColumn("_rn", F.row_number().over(window))
    out = ranked.filter(F.col("_rn") == 1).drop("_rn")

    removed = with_key.count() - out.count()
    n_null = null_key.count()
    print(f"[dedupe-key] {entity}: removed {removed:,}; null-key rows quarantined {n_null:,}")
    return out, null_key, removed


def _duplicate_row_count(df, cols, where=None):
    """Rows that participate in a duplicated attribute combination."""
    base = df.filter(where) if where is not None else df
    dups = base.groupBy(*cols).count().filter(F.col("count") > 1).select(*cols)
    return base.join(dups, cols, "inner").count()


def flag_business_attribute_conflicts(df, entity):
    """Business-attribute conflicts that must be reviewed, not silently deduped."""
    conflicts = {}
    if entity == "customers":
        conflicts["DUPLICATE_CUSTOMER_EMAIL"] = _duplicate_row_count(
            df, ["email"], F.col("email").isNotNull()
        )
    if entity == "returns":
        conflicts["DUPLICATE_RETURN_ATTRIBUTES"] = _duplicate_row_count(
            df, ["transaction_id", "return_date", "quantity_returned"]
        )
    return conflicts


# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Reference enrichment + DQ evaluation

# COMMAND ----------


def _attach_references(df, entity):
    """Broadcast-join silver references; adds <flag> booleans and lookup columns."""
    for ref in ENTITY_SPEC[entity]["references"]:
        ref_name = table_name(ref["entity"])
        if not spark.catalog.tableExists(ref_name):
            raise RuntimeError(
                f"Reference table {ref_name} is missing - run {ref['entity']} first."
            )
        right_on = ref["right_on"]
        sel = ref.get("select", {})

        cols = [right_on] + list(sel.keys())
        rdf = spark.read.table(ref_name)
        # SCD2 references hold every historical version, so an unfiltered join would
        # multiply fact rows once any tracked attribute changes. Facts must always see
        # the current version only.
        if ENTITY_SPEC[ref["entity"]]["scd2"]:
            rdf = rdf.filter(F.col("is_current"))
        rdf = rdf.select(*cols).distinct()
        rdf = rdf.withColumnRenamed(right_on, f"_ref_{right_on}")
        for src in sel:
            rdf = rdf.withColumnRenamed(src, f"_ref_sel_{src}")

        df = df.join(
            F.broadcast(rdf),
            df[ref["left_on"]] == rdf[f"_ref_{right_on}"],
            "left",
        )
        df = df.withColumn(ref["flag"], F.col(f"_ref_{right_on}").isNotNull())

        drop_cols = [f"_ref_{right_on}"]
        for src, dst in sel.items():
            df = df.withColumn(dst, F.col(f"_ref_sel_{src}"))
            drop_cols.append(f"_ref_sel_{src}")
        df = df.drop(*drop_cols)
    return df


def validate_business_rules(df, entity):
    """One aggregation pass: per-rule failure counts plus a per-row reason string."""
    rules = list(ENTITY_SPEC[entity]["dq_rules"])
    if "_cast_error_column" in df.columns:
        rules = [("TYPE_CAST_VALID", "_cast_error_column IS NULL")] + rules

    agg = [
        F.sum(F.when(~F.expr(cond), 1).otherwise(0)).alias(name)
        for name, cond in rules
    ]
    row = df.agg(*agg).first()
    counts = {name: int(row[name] or 0) for name, _ in rules}

    reason = F.concat_ws(
        ";", *[F.when(~F.expr(cond), F.lit(name)) for name, cond in rules]
    )
    return df.withColumn("_reject_reason", reason), counts


def split_valid_and_quarantine(df, entity):
    valid = df.filter(F.length(F.col("_reject_reason")) == 0)
    quarantined = df.filter(F.length(F.col("_reject_reason")) > 0)
    return valid, quarantined


# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Delta writes

# COMMAND ----------


def _ddl(schema):
    return ",\n  ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields)


def _ensure_table(df, name, path):
    if spark.catalog.tableExists(name):
        return False
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {name} (\n  {_ddl(df.schema)}\n) "
        f"USING DELTA LOCATION '{path}'"
    )
    return True


def _merge_metrics(entity):
    """Exact insert/update counts from Delta history - no extra scans."""
    try:
        hist = spark.sql(f"DESCRIBE HISTORY {table_name(entity)} LIMIT 1").first()
        metrics = hist["operationMetrics"] or {}
        return (
            int(metrics.get("numTargetRowsInserted", 0)),
            int(metrics.get("numTargetRowsUpdated", 0)),
        )
    except Exception:
        return (0, 0)


def merge_silver(df, entity):
    """Keyed Delta MERGE. Idempotent; never blanket-overwrites history."""
    name, path = table_name(entity), target_path(entity)
    key = ENTITY_SPEC[entity]["key"]

    if df.limit(1).count() == 0:
        print(f"[merge] {entity}: empty batch, nothing written")
        return 0, 0

    if _ensure_table(df, name, path):
        df.write.mode("append").format("delta").save(path)
        n = df.count()
        print(f"[merge] {entity}: initial load {n:,} rows")
        return n, 0

    updates = {c: f"s.`{c}`" for c in df.columns if c not in key}
    inserts = {c: f"s.`{c}`" for c in df.columns}
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in key)

    (
        DeltaTable.forName(spark, name).alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdate(set=updates)
        .whenNotMatchedInsert(values=inserts)
        .execute()
    )
    ins, upd = _merge_metrics(entity)
    print(f"[merge] {entity}: inserted={ins:,} updated={upd:,}")
    return ins, upd


def write_quarantine(df, entity, run_id, batch_id):
    """Append-only: a later run never destroys an earlier run's rejects."""
    n = df.count()
    if n == 0:
        print(f"[quarantine] {entity}: 0 rows rejected")
        return 0

    out = (
        df.withColumn("_reject_ts", F.current_timestamp())
        .withColumn("_quarantine_run_id", F.lit(run_id))
        .withColumn("_quarantine_batch_id", F.lit(batch_id))
    )
    path = f"{QUARANTINE_PATH}/{entity}/"
    _ensure_table(out, quarantine_table(entity), path)
    out.write.mode("append").option("mergeSchema", "true").format("delta").save(path)
    print(f"[quarantine] {entity}: appended {n:,} rows")
    return n


# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. SCD Type 2 (customers, products)

# COMMAND ----------


def _sweep_scd2_scratch(max_age_hours=24):
    """Drop scratch tables orphaned by a killed run. Best effort only."""
    base = f"{SILVER_PATH}/_tmp/scd2"
    cutoff = time.time() - max_age_hours * 3600
    try:
        for entry in dbutils.fs.ls(base):
            if entry.modificationTime / 1000.0 < cutoff:
                dbutils.fs.rm(entry.path, True)
                print(f"[scd2] swept stale scratch {entry.name}")
    except Exception:
        pass


def apply_scd2(df, entity, run_id):
    """Close changed current versions, then insert new ones. Idempotent.

    - one version per key per batch (df is already deduped)
    - late arrivals are quarantined, never back-dated silently
    - unchanged tracked attributes produce no new version
    """
    name, path = table_name(entity), target_path(entity)
    key = ENTITY_SPEC[entity]["key"]
    tracked = ENTITY_SPEC[entity]["scd2"]["tracked"]

    if df.limit(1).count() == 0:
        print(f"[scd2] {entity}: empty batch")
        return 0, 0, spark.createDataFrame([], df.schema)

    if not spark.catalog.tableExists(name):
        initial = (
            df.withColumn("effective_from", F.col("_ingest_ts"))
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )
        _ensure_table(initial, name, path)
        initial.write.mode("append").format("delta").save(path)
        n = initial.count()
        print(f"[scd2] {entity}: initial load {n:,} versions")
        return n, 0, spark.createDataFrame([], initial.schema)

    tgt = DeltaTable.forName(spark, name)

    current = (
        tgt.toDF()
        .filter(F.col("is_current"))
        .select(
            *[F.col(k).alias(f"_c_{k}") for k in key],
            F.col("effective_from").alias("_cur_effective_from"),
            *[F.col(c).alias(f"_cur_{c}") for c in tracked],
        )
    )

    joined = df.join(
        F.broadcast(current),
        *[df[k] == current[f"_c_{k}"] for k in key],
        "left",
    )

    late_cond = F.col("_cur_effective_from").isNotNull() & (
        F.col("_ingest_ts") < F.col("_cur_effective_from")
    )
    aux_cols = (
        [f"_c_{k}" for k in key]
        + ["_cur_effective_from"]
        + [f"_cur_{c}" for c in tracked]
    )

    # Materialise the whole joined set once. The close and the insert must read the
    # SAME snapshot: if the insert set is re-derived lazily after the merge, and the
    # caller built `df` from the target table, the merge's own writes silently
    # change the input and the replacement version is never inserted.
    tmp_path = f"{SILVER_PATH}/_tmp/scd2/{entity}_{uuid.uuid4().hex}"
    _sweep_scd2_scratch()
    joined.drop(*[f"_c_{k}" for k in key]).withColumn(
        "_scd2_late", late_cond
    ).write.mode("overwrite").format("delta").save(tmp_path)

    try:
        staged = spark.read.format("delta").load(tmp_path)

        late = staged.filter(F.col("_scd2_late")).drop(*aux_cols, "_scd2_late")
        on_time = staged.filter(~F.col("_scd2_late")).drop("_scd2_late")

        # A replacement version is due when the key is new, or when ANY tracked
        # attribute differs from the current version.
        attr_changed = F.lit(False)
        for c in tracked:
            attr_changed = attr_changed | ~F.col(c).eqNullSafe(F.col(f"_cur_{c}"))
        needs_version = (~F.col("_cur_effective_from").isNotNull()) | attr_changed

        to_insert = (
            on_time.filter(needs_version)
            .drop(*aux_cols)
            .withColumn("effective_from", F.col("_ingest_ts"))
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )
        merge_src = on_time.drop(*aux_cols)

        change_cond = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in tracked)
        match_cond = (
            " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in key)
            + " AND t.is_current AND (" + change_cond + ")"
        )

        (
            tgt.alias("t").merge(merge_src.alias("s"), match_cond)
            .whenMatchedUpdate(set={
                "effective_to": "s._ingest_ts",
                "is_current": "false",
            })
            .execute()
        )
        _, n_closed = _merge_metrics(entity)

        n_new = to_insert.count()
        if n_new:
            to_insert.write.mode("append").format("delta").save(path)

        n_late = late.count()
        # Materialise late rows before the scratch table is dropped - the caller
        # quarantines them after this function returns, so they cannot stay lazy.
        late_schema = late.schema
        late_rows = late.collect() if n_late else []
    finally:
        try:
            dbutils.fs.rm(tmp_path, True)
        except Exception as exc:
            print(f"[scd2] {entity}: could not drop scratch {tmp_path}: {exc}")

    late_out = (
        spark.createDataFrame(late_rows, late_schema)
        if late_rows
        else spark.createDataFrame([], late_schema)
    )

    print(f"[scd2] {entity}: new={n_new:,} closed={n_closed:,} late={n_late:,}")
    return n_new, n_closed, late_out


# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Audit tables (explicit schemas - never inferred)

# COMMAND ----------

RUN_LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("pipeline", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("entity", StringType(), True),
    StructField("notebook", StringType(), True),
    StructField("auth_mode", StringType(), True),
    StructField("source_path", StringType(), True),
    StructField("target_path", StringType(), True),
    StructField("status", StringType(), True),
    StructField("source_rows", LongType(), True),
    StructField("rows_read", LongType(), True),
    StructField("exact_duplicates_removed", LongType(), True),
    StructField("business_key_duplicates_removed", LongType(), True),
    StructField("rows_quarantined", LongType(), True),
    StructField("rows_written", LongType(), True),
    StructField("rows_inserted", LongType(), True),
    StructField("rows_updated", LongType(), True),
    StructField("run_timestamp", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("error_class", StringType(), True),
    StructField("error_message", StringType(), True),
])
RUN_LOG_COLUMNS = [f.name for f in RUN_LOG_SCHEMA.fields]

DQ_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("entity", StringType(), True),
    StructField("rule_name", StringType(), True),
    StructField("rule_status", StringType(), True),
    StructField("records_evaluated", LongType(), True),
    StructField("records_failed", LongType(), True),
    StructField("rejection_count", LongType(), True),
    StructField("execution_ts", TimestampType(), True),
])

WM_SCHEMA = StructType([
    StructField("entity", StringType(), True),
    StructField("watermark_ts", TimestampType(), True),
    StructField("run_id", StringType(), True),
    StructField("updated_ts", TimestampType(), True),
])


def _append_ops(df, name, path):
    _ensure_table(df, name, path)
    df.write.mode("append").option("mergeSchema", "true").format("delta").save(path)


def write_run_log(metrics):
    row = tuple(metrics.get(c) for c in RUN_LOG_COLUMNS)
    df = spark.createDataFrame([row], RUN_LOG_SCHEMA)
    _append_ops(df, f"{CATALOG}.silver_ops.run_log", RUN_LOG_PATH)
    print(f"[audit] run_log <- {metrics['entity']} status={metrics['status']}")


def write_quality_summary(metrics, rule_counts):
    total = int(metrics.get("rows_read") or 0)
    rows = [
        (
            metrics["run_id"], metrics["batch_id"], metrics["entity"], rule,
            "PASSED" if failed == 0 else "FAILED",
            total, int(failed), int(failed), datetime.now(),
        )
        for rule, failed in sorted(rule_counts.items())
    ]
    if not rows:
        return
    df = spark.createDataFrame(rows, DQ_SCHEMA)
    _append_ops(df, f"{CATALOG}.silver_ops.data_quality_summary", DQ_SUMMARY_PATH)


def write_watermark(entity, max_ts, run_id):
    if max_ts is None:
        return
    df = spark.createDataFrame([(entity, max_ts, run_id, datetime.now())], WM_SCHEMA)
    _append_ops(df, f"{CATALOG}.silver_ops.entity_watermarks", WATERMARK_PATH)


def validate_target_uniqueness(entity):
    """Fail the run on any structural violation of the target's key contract."""
    spec = ENTITY_SPEC[entity]
    key = spec["key"]
    df = spark.read.table(table_name(entity))

    if spec["scd2"]:
        cur = df.filter(F.col("is_current"))

        dup = cur.groupBy(*key).count().filter(F.col("count") > 1).count()
        if dup:
            raise RuntimeError(f"{entity}: {dup} keys have more than one current SCD2 row")

        orphan = (
            df.select(*key).distinct()
            .join(cur.select(*key), key, "left_anti")
            .count()
        )
        if orphan:
            raise RuntimeError(
                f"{entity}: {orphan} keys have no current SCD2 row - history was "
                "closed without opening a replacement version"
            )

        bad_open = cur.filter(F.col("effective_to").isNotNull()).count()
        if bad_open:
            raise RuntimeError(
                f"{entity}: {bad_open} rows are is_current but still have effective_to"
            )

        bad_range = df.filter(
            F.col("effective_to").isNotNull()
            & (F.col("effective_to") < F.col("effective_from"))
        ).count()
        if bad_range:
            raise RuntimeError(
                f"{entity}: {bad_range} rows have effective_to < effective_from"
            )

        print(f"[verify] {entity}: SCD2 invariants OK (exactly one current row per key)")
    else:
        dup = df.groupBy(*key).count().filter(F.col("count") > 1).count()
        if dup:
            raise RuntimeError(f"{entity}: {dup} duplicate business keys in silver")
        print(f"[verify] {entity}: business-key uniqueness OK")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. execute_entity - the single entry point

# COMMAND ----------


def _maybe_persist(df, label):
    """Cache when the compute allows it; never fail a load because of caching.

    Serverless rejects persist() unless the environment client is v5+, so this is
    opportunistic only. Data volumes here are KB-scale, so recomputation is cheap.
    """
    try:
        df = df.persist()
        df.count()
        print(f"[cache] {label}: cached")
        return df, True
    except Exception as exc:
        print(f"[cache] {label}: persist unavailable ({type(exc).__name__}) - uncached")
        return df, False


def execute_entity(entity, run_id=None, batch_id=None, notebook=None):
    """Complete bronze -> silver cycle. Always writes an audit row, even on failure."""
    run_id = run_id or _arg("run_id") or str(uuid.uuid4())
    batch_id = batch_id or _arg("batch_id") or run_id
    notebook = notebook or ENTITY_NOTEBOOK.get(entity, f"NB_Silver_{entity}")

    started = datetime.now()
    t0 = time.time()

    metrics = {c: None for c in RUN_LOG_COLUMNS}
    metrics.update(
        run_id=run_id, batch_id=batch_id, pipeline=PIPELINE_NAME, layer=LAYER,
        entity=entity, notebook=notebook, auth_mode=AUTH_MODE,
        source_path=bronze_path(entity), target_path=target_path(entity),
        run_timestamp=started, source_rows=0, rows_read=0,
        exact_duplicates_removed=0, business_key_duplicates_removed=0,
        rows_quarantined=0, rows_written=0, rows_inserted=0, rows_updated=0,
    )

    rule_counts = {}
    staged = None
    cached = False
    try:
        raw = read_bronze(entity)
        validate_schema(raw, entity)
        metrics["source_rows"] = metrics["rows_read"] = raw.count()

        staged = (
            raw
            .transform(lambda d: trim_and_standardize(d, entity))
            .transform(lambda d: apply_casts(d, entity))
            .transform(lambda d: add_ingestion_metadata(d, entity, batch_id))
            .transform(lambda d: add_row_hash(d, entity))
        )
        staged = _attach_references(staged, entity)
        staged, cached = _maybe_persist(staged, entity)

        staged, n_exact = deduplicate_exact(staged, entity)
        metrics["exact_duplicates_removed"] = n_exact

        staged, null_key, n_bk = deduplicate_business_key(staged, entity)
        metrics["business_key_duplicates_removed"] = n_bk

        conflicts = flag_business_attribute_conflicts(staged, entity)
        for rule, count in conflicts.items():
            if count:
                print(f"[conflict] {entity}: {rule} groups={count}")

        scored, rule_counts = validate_business_rules(staged, entity)
        if conflicts:
            rule_counts.update(conflicts)

        valid, quarantined = split_valid_and_quarantine(scored, entity)

        n_null_key = null_key.count()
        if n_null_key:
            rule_counts["NULL_BUSINESS_KEY"] = n_null_key
            null_key = (
                null_key
                .withColumn("_reject_reason", F.lit("NULL_BUSINESS_KEY"))
                .withColumn("_cast_error_column", F.lit(None).cast("string"))
            )
            quarantined = quarantined.unionByName(null_key, allowMissingColumns=True)

        metrics["rows_quarantined"] = write_quarantine(quarantined, entity, run_id, batch_id)

        valid = drop_cast_artifacts(valid, entity)

        if ENTITY_SPEC[entity]["scd2"]:
            inserted, updated, late = apply_scd2(valid, entity, run_id)
            metrics["rows_inserted"] = inserted
            metrics["rows_updated"] = updated
            n_late = late.count()
            if n_late:
                late = late.withColumn("_reject_reason", F.lit("LATE_ARRIVING_RECORD"))
                write_quarantine(late, entity, run_id, batch_id)
                metrics["rows_quarantined"] += n_late
        else:
            inserted, updated = merge_silver(valid, entity)
            metrics["rows_inserted"] = inserted
            metrics["rows_updated"] = updated

        metrics["rows_written"] = int(inserted) + int(updated)
        validate_target_uniqueness(entity)

        watermark = valid.agg(F.max("_ingest_ts")).first()[0]
        write_watermark(entity, watermark, run_id)

        metrics["status"] = "SUCCESS"
        print(
            f"[done] {entity}: written={metrics['rows_written']:,} "
            f"quarantined={metrics['rows_quarantined']:,}"
        )

    except Exception as exc:
        metrics["status"] = "FAILED"
        metrics["error_class"] = type(exc).__name__
        metrics["error_message"] = str(exc)[:2000]
        print(f"[FAILED] {entity}: {metrics['error_class']}: {metrics['error_message']}")
        raise

    finally:
        if staged is not None and cached:
            try:
                staged.unpersist()
            except Exception:
                pass
        metrics["duration_seconds"] = round(time.time() - t0, 3)
        try:
            write_run_log(metrics)
            write_quality_summary(metrics, rule_counts)
        except Exception as log_exc:
            print(f"[audit-warning] could not persist audit rows: {log_exc}")

    return {
        k: (v.isoformat() if isinstance(v, datetime) else v)
        for k, v in metrics.items()
    }


print("NB_Silver_Common v2 loaded.")
print(f"entities = {ENTITY_ORDER}")
