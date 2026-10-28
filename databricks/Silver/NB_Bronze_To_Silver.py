# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze -> Silver orchestrator
# MAGIC
# MAGIC Runs the six entity notebooks in dependency order under one shared `run_id` and
# MAGIC `batch_id`.
# MAGIC
# MAGIC **Order:** stores -> customers -> products -> sales_transactions -> returns -> inventory
# MAGIC
# MAGIC **Dependency guard:** an entity whose upstream reference data is not `SUCCESS` is
# MAGIC never processed - it is recorded as `SKIPPED` with the blocking dependency named.
# MAGIC This holds even when `continue_on_error=true`, because a transform over incomplete
# MAGIC reference data would silently produce wrong rows.
# MAGIC
# MAGIC **`continue_on_error`** (default `false` = fail-fast): when `false`, the first failure
# MAGIC stops processing and every remaining entity is logged as `SKIPPED`. When `true`,
# MAGIC independent entities keep running; dependents are still skipped.
# MAGIC
# MAGIC The notebook always finishes by emitting machine-readable JSON via
# MAGIC `dbutils.notebook.exit`. Downstream orchestration (ADF) must branch on `status`:
# MAGIC `SUCCESS` (all six), `PARTIAL` (some succeeded), `FAILED` (none succeeded).

# COMMAND ----------

dbutils.widgets.text("auth_mode", "auto")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("batch_id", "")
dbutils.widgets.dropdown("continue_on_error", "false", ["false", "true"])

# COMMAND ----------

# MAGIC %run ./NB_Silver_Common

# COMMAND ----------

import json
import time
import uuid

# _silver_logger is available from NB_Silver_Common via %run above
pipeline_started = time.time()
RUN_ID = _arg("run_id").strip() or str(uuid.uuid4())
BATCH_ID = _arg("batch_id").strip() or f"{RUN_ID}-silver"
CONTINUE_ON_ERROR = _arg("continue_on_error", "false").strip().lower() == "true"
CHILD_TIMEOUT = int(_arg("child_timeout_seconds", "3600") or 3600)

# Address siblings absolutely: relative notebook paths are resolved against the
# interactive working directory, which differs from the notebook's own folder.
try:
    _self_path = (
        dbutils.notebook.entry_point.getDbutils().notebook()
        .getContext().notebookPath().get()
    )
    NOTEBOOK_DIR = _self_path.rsplit("/", 1)[0]
except Exception:
    NOTEBOOK_DIR = "/Users/saddamhusain23@gmail.com/NorthWind_Retail/Silver"

# Dependencies are derived from the join definitions in ENTITY_SPEC, so the guard
# cannot drift away from what the transforms actually require.
DEPENDENCIES = {
    e: sorted({r["entity"] for r in ENTITY_SPEC[e]["references"]})
    for e in ENTITY_ORDER
}

_silver_logger.info("[orchestrator] run_id=%s batch_id=%s continue_on_error=%s", RUN_ID, BATCH_ID, CONTINUE_ON_ERROR)
for e in ENTITY_ORDER:
    _silver_logger.info("  entity=%-20s dependencies=%s", e, DEPENDENCIES[e] or 'none')

# COMMAND ----------


def _log_skipped(entity, reason):
    """Audit skipped entities too - a silent skip is indistinguishable from a bug."""
    m = {c: None for c in RUN_LOG_COLUMNS}
    m.update(
        run_id=RUN_ID, batch_id=BATCH_ID, pipeline=PIPELINE_NAME, layer=LAYER,
        entity=entity, notebook=ENTITY_NOTEBOOK[entity], auth_mode=AUTH_MODE,
        source_path=bronze_path(entity), target_path=target_path(entity),
        run_timestamp=datetime.now(), source_rows=0, rows_read=0,
        exact_duplicates_removed=0, business_key_duplicates_removed=0,
        rows_quarantined=0, rows_written=0, rows_inserted=0, rows_updated=0,
        status="SKIPPED", duration_seconds=0.0,
        error_class="SKIPPED", error_message=reason,
    )
    write_run_log(m)


def _recover_failed(entity, exc_message):
    """The child already wrote a FAILED audit row; read it back for real metrics."""
    try:
        rows = (
            spark.read.table(f"{CATALOG}.silver_ops.run_log")
            .filter((F.col("run_id") == RUN_ID) & (F.col("entity") == entity))
            .orderBy(F.col("run_timestamp").desc())
            .limit(1)
            .collect()
        )
        if rows:
            return {
                k: (v.isoformat() if hasattr(v, "isoformat") else v)
                for k, v in rows[0].asDict().items()
            }
    except Exception as exc:
        _silver_logger.warning("[orchestrator] could not read back audit row for %s: %s", entity, exc)
    return {"entity": entity, "status": "FAILED", "error_message": exc_message[:2000]}


# COMMAND ----------

results = []
status_by_entity = {}

for entity in ENTITY_ORDER:
    not_ready = [d for d in DEPENDENCIES[entity] if status_by_entity.get(d) != "SUCCESS"]

    if not_ready:
        reason = "dependency_not_ready:" + ",".join(not_ready)
    elif not CONTINUE_ON_ERROR and "FAILED" in status_by_entity.values():
        reason = "fail_fast: an earlier entity failed"
    else:
        reason = None

    if reason:
        status_by_entity[entity] = "SKIPPED"
        _log_skipped(entity, reason)
        results.append({"entity": entity, "status": "SKIPPED", "skip_reason": reason})
        _silver_logger.warning("[SKIPPED] entity=%s reason=%s", entity, reason)
        continue

    t0 = time.time()
    try:
        raw = dbutils.notebook.run(
            f"{NOTEBOOK_DIR}/{ENTITY_NOTEBOOK[entity]}",
            CHILD_TIMEOUT,
            {
                "run_id": RUN_ID,
                "batch_id": BATCH_ID,
                "auth_mode": AUTH_MODE,
            },
        )
        child = json.loads(raw) if raw else {"entity": entity, "status": "UNKNOWN"}
    except Exception as exc:
        child = _recover_failed(entity, str(exc))

    child["wall_seconds"] = round(time.time() - t0, 1)
    status_by_entity[entity] = child.get("status") or "UNKNOWN"
    results.append(child)
    _silver_logger.info("[%s] entity=%s wall_seconds=%s", status_by_entity[entity], entity, child.get("wall_seconds"))

# COMMAND ----------

counts = {}
for r in results:
    s = r.get("status") or "UNKNOWN"
    counts[s] = counts.get(s, 0) + 1

if counts.get("SUCCESS", 0) == len(results):
    overall = "SUCCESS"
elif counts.get("SUCCESS", 0) == 0:
    overall = "FAILED"
else:
    overall = "PARTIAL"


def _sum(field):
    return sum(int(r.get(field) or 0) for r in results)


failed = [r for r in results if r.get("status") == "FAILED"]
first_failure = failed[0] if failed else None

summary = {
    "run_id": RUN_ID,
    "batch_id": BATCH_ID,
    "pipeline": PIPELINE_NAME,
    "layer": LAYER,
    "auth_mode": AUTH_MODE,
    "continue_on_error": CONTINUE_ON_ERROR,
    "status": overall,
    "entities_total": len(results),
    "entities_succeeded": counts.get("SUCCESS", 0),
    "entities_failed": counts.get("FAILED", 0),
    "entities_skipped": counts.get("SKIPPED", 0),
    "duration_seconds": round(time.time() - pipeline_started, 1),
    # Pre-aggregated for the caller: Data Factory expressions cannot reduce an
    # array, so the audit stored procedure is fed these totals directly.
    "rows_read_total": _sum("rows_read"),
    "rows_written_total": _sum("rows_written"),
    "rows_quarantined_total": _sum("rows_quarantined"),
    "failed_entities": [r.get("entity") for r in failed],
    "skipped_entities": [r.get("entity") for r in results if r.get("status") == "SKIPPED"],
    "error_summary": (
        "{entity}: {cls}: {msg}".format(
            entity=first_failure.get("entity"),
            cls=first_failure.get("error_class") or "Error",
            msg=first_failure.get("error_message"),
        )
        if first_failure
        else None
    ),
    "results": results,
}

_silver_logger.info(
    "[orchestrator] %s: %d ok / %d failed / %d skipped in %ss",
    overall,
    summary["entities_succeeded"],
    summary["entities_failed"],
    summary["entities_skipped"],
    summary["duration_seconds"]
)
_silver_logger.info("Silver Pipeline Summary:\n%s", json.dumps(summary, default=str, indent=2))

dbutils.notebook.exit(json.dumps(summary, default=str))
