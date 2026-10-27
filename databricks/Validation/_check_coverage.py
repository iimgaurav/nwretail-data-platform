# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze -> Silver coverage check (temporary, read-only)
# MAGIC
# MAGIC Reports, per entity: bronze row count, silver table presence and row count,
# MAGIC plus the raw directory layout of both containers (catches data written to
# MAGIC storage without a Unity Catalog table).

# COMMAND ----------

# MAGIC %run ./Silver/NB_Silver_Common

# COMMAND ----------

import json

out = {"entities": {}, "bronze_root": [], "silver_root": []}


def _layout(path, depth=0):
    """Name/size listing, recursing one level into directories."""
    rows = []
    try:
        for f in dbutils.fs.ls(path):
            is_dir = f.name.endswith("/")
            rows.append({
                "name": f.name,
                "dir": is_dir,
                "size": f.size,
                "depth": depth,
            })
            if is_dir and depth < 2:
                rows.extend(_layout(f.path, depth + 1))
    except Exception as exc:
        rows.append({"name": f"<error: {exc}>", "dir": False, "size": 0, "depth": depth})
    return rows


out["bronze_root"] = _layout(BRONZE_PATH, 0)
out["silver_root"] = _layout(SILVER_PATH, 0)

# COMMAND ----------

for e in ENTITY_ORDER:
    info = {"file": ENTITY_SPEC[e]["file"]}

    try:
        info["bronze_rows"] = read_bronze(e).count()
    except Exception as exc:
        info["bronze_rows"] = None
        info["bronze_error"] = str(exc)[:200]

    try:
        info["bronze_mtime"] = str(_ingest_timestamp(e))
    except Exception:
        info["bronze_mtime"] = None

    info["silver_table_exists"] = spark.catalog.tableExists(table_name(e))
    if info["silver_table_exists"]:
        try:
            info["silver_rows"] = spark.read.table(table_name(e)).count()
        except Exception as exc:
            info["silver_rows"] = None
            info["silver_error"] = str(exc)[:200]
    else:
        info["silver_rows"] = None

    info["quarantine_table_exists"] = spark.catalog.tableExists(quarantine_table(e))
    if info["quarantine_table_exists"]:
        info["quarantine_rows"] = spark.read.table(quarantine_table(e)).count()
    else:
        info["quarantine_rows"] = None

    out["entities"][e] = info

# COMMAND ----------

rows = []
for e in ENTITY_ORDER:
    i = out["entities"][e]
    b, s = i["bronze_rows"], i["silver_rows"]
    rows.append({
        "entity": e,
        "bronze_rows": b,
        "silver_table": i["silver_table_exists"],
        "silver_rows": s,
        "moved": bool(i["silver_table_exists"] and b is not None and s == b),
        "lag": (b - s) if (b is not None and s is not None) else None,
    })

out["coverage"] = rows
out["entities_total"] = len(rows)
out["entities_moved"] = sum(1 for r in rows if r["moved"])
out["bronze_rows_total"] = sum(r["bronze_rows"] or 0 for r in rows)
out["silver_rows_total"] = sum(r["silver_rows"] or 0 for r in rows)
out["all_moved"] = out["entities_moved"] == out["entities_total"]

dbutils.notebook.exit(json.dumps(out, default=str))
