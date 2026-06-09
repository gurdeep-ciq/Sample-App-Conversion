"""
dagster_pipeline.py — full compact-dagster integration.

Each ingest is a real, persisted Dagster run:
  * ONE process-lifetime DagsterInstance (compact-dagster's dependency-free,
    dict-backed lite_memory storage — the fork ships no SQL/SQLite, by design),
    shared across requests so run / materialization / asset-check history
    accumulates and is queryable.
  * a proper `Definitions` per table with a file-backed IO manager **resource**
    that persists cleaned tables to ./warehouse/<table>/<partition>.json.
  * partitioned assets — each upload is a partition — so re-runs append a new
    partition rather than overwriting.
  * registered asset checks: schema-vs-registry and schema-drift-vs-previous-upload
    (the latter reads prior materializations from disk — native cross-run "refine").

The webserver/daemon/gRPC/SQL stacks the fork stripped are intentionally NOT used.
"""
from __future__ import annotations

import json
import os
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetKey,
    ConfigurableIOManager,
    DagsterEventType,
    DagsterInstance,
    Definitions,
    StaticPartitionsDefinition,
    TableColumn,
    TableSchema,
    asset,
    asset_check,
)

import entities as E
from profiling import apply_pipeline, snake

WAREHOUSE = os.path.join(os.path.dirname(__file__), "warehouse")
os.makedirs(WAREHOUSE, exist_ok=True)

# ONE shared instance for the process lifetime (compact lite_memory storage).
INSTANCE = DagsterInstance.ephemeral()
_UPLOADS: list[str] = []  # partition keys (uploads) seen this session

_SQL_TYPE = {"string": "TEXT", "integer": "INTEGER", "decimal": "NUMERIC",
             "date": "DATE", "time": "TIME", "timestamp": "TIMESTAMP",
             "number": "NUMERIC", "currency": "NUMERIC"}


# --------------------------------------------------------------------------
# IO manager resource — persists cleaned tables to the warehouse dir
# --------------------------------------------------------------------------
class TableIOManager(ConfigurableIOManager):
    base: str

    def handle_output(self, context, obj):
        table = context.asset_key.path[-1]
        pk = context.partition_key if context.has_partition_key else "all"
        d = os.path.join(self.base, table)
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, f"{pk}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, default=str)
        context.add_output_metadata({"warehouse_path": path, "row_count": len(obj)})

    def load_input(self, context):
        table = context.asset_key.path[-1]
        pk = context.partition_key if context.has_partition_key else "all"
        path = os.path.join(self.base, table, f"{pk}.json")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        return []


# --------------------------------------------------------------------------
# canonical conform (per sheet) — collision-by-confidence (from 955edb2)
# --------------------------------------------------------------------------
def _canonical_map(profile: dict, schema_cols: list[dict], registry: list[dict],
                   overrides: dict[str, str]) -> dict[str, str]:
    """Map each schema column's name -> canonical entity name (or itself when no
    confident/confirmed match).

    Collisions are resolved by confidence: when several source columns claim the
    same canonical entity (e.g. POS `Qty` and `Unit` both scoring as `quantity`),
    only the highest-confidence one takes the canonical name; the rest keep their
    own name. Without this, the conform step silently overwrote a row's value with
    whichever colliding column came last."""
    matches = {m["source"]: m for m in E.match_profile(profile, registry)}

    # 1) propose (own_name, target, confidence) for every included column.
    proposals: list[tuple[str, str, float]] = []
    for c in schema_cols:
        if not c.get("include"):
            continue
        own, src = c["name"], c.get("source", "")
        if src and "+" in src:                       # combined event_timestamp
            proposals.append((own, overrides.get(own) or "order_date", 2.0))
        elif src in overrides:
            proposals.append((own, overrides[src], 2.0))
        else:
            m = matches.get(src)
            if m and m["entity"] and m["entity"].get("entity"):
                proposals.append((own, m["entity"]["entity"], float(m["entity"].get("confidence", 0))))
            else:
                proposals.append((own, own, -1.0))   # no canonical match: keep own name

    # 2) highest-confidence claimant wins each target.
    winner: dict[str, tuple] = {}
    for own, target, conf in proposals:
        cur = winner.get(target)
        if cur is None or conf > cur[1]:
            winner[target] = (own, conf)

    # 3) winner gets the canonical name; everyone else keeps their own.
    name_map: dict[str, str] = {}
    for own, target, conf in proposals:
        name_map[own] = target if winner.get(target, (None,))[0] == own else own

    # 4) guarantee output names are unique (suffix any residual dup).
    seen: dict[str, int] = {}
    for own in list(name_map):
        nm = name_map[own]
        if nm in seen:
            seen[nm] += 1
            name_map[own] = f"{nm}_{seen[nm]}"
        else:
            seen[nm] = 1
    return name_map


def _conform(profile, rules, schema_cols, registry, overrides):
    res = apply_pipeline(profile, rules, schema_cols)
    name_map = _canonical_map(profile, schema_cols, registry, overrides)
    types = {name_map[c["name"]]: c["type"] for c in schema_cols
             if c.get("include") and c["name"] in name_map}
    rows = [{name_map[k]: v for k, v in r.items() if k in name_map} for r in res["rows"]]
    return {"rows": rows, "types": types, "name_map": name_map, "exec": res}


# --------------------------------------------------------------------------
# cross-run schema baseline (reads prior partitions' schema from disk)
# --------------------------------------------------------------------------
def _previous_schema_cols(table: str, current_pk: str) -> list[str]:
    d = os.path.join(WAREHOUSE, table)
    cols: list[str] = []
    if not os.path.isdir(d):
        return cols
    for fn in sorted(os.listdir(d)):
        if fn.endswith(".schema.json") and fn != f"{current_pk}.schema.json":
            try:
                with open(os.path.join(d, fn), encoding="utf-8") as f:
                    for c in json.load(f):
                        if c["name"] not in cols:
                            cols.append(c["name"])
            except (json.JSONDecodeError, OSError, KeyError):
                pass
    return cols


def _write_schema(table: str, pk: str, columns: list[dict]):
    d = os.path.join(WAREHOUSE, table)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"{pk}.schema.json"), "w", encoding="utf-8") as f:
        json.dump(columns, f)


# --------------------------------------------------------------------------
# build + run one table as a partitioned Dagster job on the shared instance
# --------------------------------------------------------------------------
def run_table_pipeline(table: str, members: list[dict], registry: list[dict],
                       upload_id: str) -> dict:
    table = snake(table)
    if upload_id not in _UPLOADS:
        _UPLOADS.append(upload_id)
    parts = StaticPartitionsDefinition(list(_UPLOADS))

    conformed = []
    for mb in members:
        c = _conform(mb["profile"], mb["rules"], mb["schema_cols"], registry, mb.get("overrides", {}))
        c["sheet"] = mb["sheet"]
        conformed.append(c)

    col_order, col_types = [], {}
    for c in conformed:
        for col in c["name_map"].values():
            if col not in col_types:
                col_order.append(col)
            col_types.setdefault(col, c["types"].get(col, "string"))

    table_schema = TableSchema(columns=[TableColumn(name=n, type=col_types[n]) for n in col_order])
    union_rows = [{col: r.get(col) for col in col_order} for c in conformed for r in c["rows"]]

    # source assets (one per member sheet) — lineage + per-sheet stats
    src_assets, src_keys = [], []
    for c in conformed:
        key = f"{table}__{snake(c['sheet'])}_src"
        src_keys.append(AssetKey(key))

        def _mk(rows=c["rows"], sheet=c["sheet"], ex=c["exec"], key=key):
            @asset(name=key, partitions_def=parts)
            def _src(context):
                context.add_output_metadata({"sheet": sheet, "rows_in": ex["inputCount"],
                                             "rows_kept": ex["keptCount"], "rows_dropped": ex["droppedCount"]})
                return rows
            return _src
        src_assets.append(_mk())

    @asset(name=table, deps=src_keys, partitions_def=parts, io_manager_key="warehouse_io")
    def _table(context):
        context.add_output_metadata({"dagster/column_schema": table_schema,
                                     "row_count": len(union_rows),
                                     "partition": context.partition_key,
                                     "source_sheets": ", ".join(c["sheet"] for c in conformed)})
        return union_rows

    # asset checks
    reg_names = {e["name"] for e in registry}
    known = [c for c in col_order if c in reg_names]
    new_cols = [c for c in col_order if c not in reg_names]
    prev_cols = _previous_schema_cols(table, upload_id)
    added = [c for c in col_order if c not in prev_cols] if prev_cols else []
    removed = [c for c in prev_cols if c not in col_order] if prev_cols else []

    @asset_check(name="schema_vs_registry", asset=_table)
    def _chk_reg(context):
        return AssetCheckResult(passed=True, metadata={
            "known_entities": len(known), "new_entities": len(new_cols),
            "new": ", ".join(new_cols) or "none"})

    @asset_check(name="schema_drift_vs_prev_upload", asset=_table)
    def _chk_drift(context):
        return AssetCheckResult(passed=(len(removed) == 0), metadata={
            "baseline_uploads": 1 if prev_cols else 0,
            "added": ", ".join(added) or "none",
            "removed": ", ".join(removed) or "none"})

    defs = Definitions(assets=[*src_assets, _table], asset_checks=[_chk_reg, _chk_drift],
                       resources={"warehouse_io": TableIOManager(base=WAREHOUSE)})
    job = defs.get_implicit_global_asset_job_def()
    result = job.execute_in_process(instance=INSTANCE, partition_key=upload_id)

    _write_schema(table, upload_id, [{"name": n, "type": col_types[n]} for n in col_order])

    # collect run summary from the shared instance
    asset_events = []
    for ev in result.get_asset_materialization_events():
        mat = ev.event_specific_data.materialization
        asset_events.append({"asset": mat.asset_key.to_user_string(),
                             "metadata": {k: _meta_value(v) for k, v in mat.metadata.items()}})
    checks = [{"name": c.check_name, "passed": c.passed,
               "metadata": {k: _meta_value(v) for k, v in (c.metadata or {}).items()}}
              for c in result.get_asset_check_evaluations()]

    return {
        "table": table,
        "schema": {"table": table, "columns": [{"name": n, "type": col_types[n]} for n in col_order]},
        "ddl": _ddl(table, col_order, col_types),
        "rows": union_rows,
        "rowCount": len(union_rows),
        "members": [{"sheet": c["sheet"], "rowsIn": c["exec"]["inputCount"],
                     "rowsKept": c["exec"]["keptCount"], "rowsDropped": c["exec"]["droppedCount"],
                     "dropped": c["exec"]["dropped"], "dq": c["exec"]["dq"]} for c in conformed],
        "schemaChange": {"known": known, "new": new_cols,
                         "verdict": "extends registry" if new_cols else "fully covered"},
        "crossRun": {"baselineUploads": 1 if prev_cols else 0, "added": added, "removed": removed,
                     "drift": "first upload" if not prev_cols
                              else ("breaking (columns removed)" if removed
                                    else ("new columns added" if added else "stable"))},
        "dagsterRun": {"success": result.success, "runId": result.run_id,
                       "partition": upload_id, "assets": asset_events, "checks": checks,
                       "warehousePath": os.path.join("warehouse", table, f"{upload_id}.json")},
    }


# --------------------------------------------------------------------------
# run history (queried from the shared instance)
# --------------------------------------------------------------------------
def run_history(limit: int = 50) -> list[dict]:
    out = []
    for run in INSTANCE.get_runs(limit=limit):
        tags = run.tags or {}
        out.append({"runId": run.run_id, "status": str(run.status.value),
                    "job": run.job_name, "partition": tags.get("dagster/partition")})
    return out


def run_detail(run_id: str) -> dict:
    materializations, checks = [], []
    for rec in INSTANCE.get_records_for_run(run_id).records:
        ev = rec.event_log_entry.dagster_event
        if ev is None:
            continue
        if ev.event_type == DagsterEventType.ASSET_MATERIALIZATION:
            mat = ev.event_specific_data.materialization
            materializations.append({"asset": mat.asset_key.to_user_string(),
                                     "metadata": {k: _meta_value(v) for k, v in mat.metadata.items()}})
        elif ev.event_type == DagsterEventType.ASSET_CHECK_EVALUATION:
            ace = ev.event_specific_data
            checks.append({"asset": ace.asset_key.to_user_string(), "check": ace.check_name,
                           "passed": ace.passed})
    run = INSTANCE.get_run_by_id(run_id)
    return {"runId": run_id, "status": str(run.status.value) if run else "UNKNOWN",
            "materializations": materializations, "checks": checks}


# --------------------------------------------------------------------------
def _meta_value(v):
    try:
        val = v.value
    except AttributeError:
        return str(v)
    if isinstance(val, TableSchema):
        return {"columns": [{"name": c.name, "type": c.type} for c in val.columns]}
    if isinstance(val, (int, float, str, bool)) or val is None:
        return val
    return str(val)


def _ddl(table, cols, types):
    body = ",\n".join(f"  {c} {_SQL_TYPE.get(types[c], 'TEXT')}" for c in cols)
    return f"CREATE TABLE {table} (\n{body}\n);"
