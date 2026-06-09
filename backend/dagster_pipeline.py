"""
dagster_pipeline.py — turns a profiled file + entity mappings into REAL Dagster
assets and executes them in-process via compact-dagster's materialize().

Per target table we build:
  <table>__<member>_src   (one per unioned sheet)  -> clean + conform to canonical names
  <table>                 (depends on all srcs)    -> union rows, emit TableSchema
  + an asset_check comparing the materialized schema to the canonical registry
    (this is the "refine schema across formats" signal).

The cleaned data is captured via an in-process sink (single interpreter, no IO
round-trip needed) so the API can return rows alongside the Dagster run events.
"""
from __future__ import annotations

from typing import Any

from dagster import (
    AssetCheckResult,
    AssetKey,
    MaterializeResult,
    TableColumn,
    TableSchema,
    asset,
    asset_check,
    materialize,
)

import entities as E
from profiling import apply_pipeline, snake

_SQL_TYPE = {"string": "TEXT", "integer": "INTEGER", "decimal": "NUMERIC",
             "date": "DATE", "time": "TIME", "timestamp": "TIMESTAMP",
             "number": "NUMERIC", "currency": "NUMERIC"}


def _canonical_map(profile: dict, schema_cols: list[dict], registry: list[dict],
                   overrides: dict[str, str]) -> dict[str, str]:
    """Map each schema column's name -> canonical entity name (or itself when
    no confident/confirmed match)."""
    matches = {m["source"]: m for m in E.match_profile(profile, registry)}
    name_map: dict[str, str] = {}
    for c in schema_cols:
        if not c.get("include"):
            continue
        src = c.get("source", "")
        if src and "+" in src:           # combined event_timestamp
            name_map[c["name"]] = overrides.get(c["name"]) or "order_date"
            continue
        if src in overrides:
            name_map[c["name"]] = overrides[src]
            continue
        m = matches.get(src)
        if m and m["entity"] and m["entity"].get("entity"):
            name_map[c["name"]] = m["entity"]["entity"]
        else:
            name_map[c["name"]] = c["name"]
    return name_map


def _conform(profile: dict, rules: list[dict], schema_cols: list[dict],
             registry: list[dict], overrides: dict[str, str]) -> dict:
    """Run the pipeline on one sheet and rename columns to canonical names."""
    res = apply_pipeline(profile, rules, schema_cols)
    name_map = _canonical_map(profile, schema_cols, registry, overrides)
    types = {name_map[c["name"]]: c["type"] for c in schema_cols
             if c.get("include") and c["name"] in name_map}
    rows = [{name_map[k]: v for k, v in r.items() if k in name_map} for r in res["rows"]]
    return {"rows": rows, "types": types, "name_map": name_map, "exec": res}


def run_table_pipeline(table: str, members: list[dict], registry: list[dict]) -> dict:
    """
    members: [{"sheet", "profile", "rules", "schema_cols", "overrides"}]
    Returns the materialized table: canonical schema, unioned rows, per-member
    execution stats, the Dagster run summary, and the schema-change verdict.
    """
    table = snake(table)
    sink: dict[str, Any] = {}

    conformed = []
    for mb in members:
        c = _conform(mb["profile"], mb["rules"], mb["schema_cols"], registry,
                     mb.get("overrides", {}))
        c["sheet"] = mb["sheet"]
        conformed.append(c)

    # canonical column order (union across members) + type per column
    col_order: list[str] = []
    col_types: dict[str, str] = {}
    for c in conformed:
        for col in c["name_map"].values():
            if col not in col_types:
                col_order.append(col)
            col_types.setdefault(col, c["types"].get(col, "string"))

    # ---- build Dagster assets ----
    src_assets = []
    for i, c in enumerate(conformed):
        key = f"{table}__{snake(c['sheet'])}_src"

        def _make(rows=c["rows"], sheet=c["sheet"], ex=c["exec"]):
            @asset(name=key)
            def _src():
                return MaterializeResult(metadata={
                    "sheet": sheet, "rows_in": ex["inputCount"],
                    "rows_kept": ex["keptCount"], "rows_dropped": ex["droppedCount"],
                })
            return _src
        src_assets.append(_make())

    table_schema = TableSchema(columns=[TableColumn(name=n, type=col_types[n]) for n in col_order])
    src_keys = [AssetKey(f"{table}__{snake(c['sheet'])}_src") for c in conformed]

    @asset(name=table, deps=src_keys)
    def _table():
        union_rows = []
        for c in conformed:
            for r in c["rows"]:
                union_rows.append({col: r.get(col) for col in col_order})
        sink["rows"] = union_rows
        return MaterializeResult(metadata={
            "dagster/column_schema": table_schema,
            "row_count": len(union_rows),
            "source_sheets": ", ".join(c["sheet"] for c in conformed),
            "columns": len(col_order),
        })

    # ---- schema-change check vs registry ("refine" signal) ----
    reg_names = {e["name"] for e in registry}
    known = [c for c in col_order if c in reg_names]
    new_cols = [c for c in col_order if c not in reg_names]

    @asset_check(name="schema_vs_registry", asset=_table)
    def _check():
        sink["schema_change"] = {"known": known, "new": new_cols,
                                 "verdict": "extends registry" if new_cols else "fully covered"}
        return AssetCheckResult(
            passed=True,
            metadata={"known_entities": len(known), "new_entities": len(new_cols),
                      "new": ", ".join(new_cols) or "none"},
        )

    result = materialize([*src_assets, _table, _check])

    # ---- collect run summary ----
    asset_events = []
    for ev in result.get_asset_materialization_events():
        mat = ev.event_specific_data.materialization
        asset_events.append({
            "asset": mat.asset_key.to_user_string(),
            "metadata": {k: _meta_value(v) for k, v in mat.metadata.items()},
        })

    return {
        "table": table,
        "schema": {"table": table,
                   "columns": [{"name": n, "type": col_types[n]} for n in col_order]},
        "ddl": _ddl(table, col_order, col_types),
        "rows": sink.get("rows", []),
        "rowCount": len(sink.get("rows", [])),
        "members": [{"sheet": c["sheet"], "rowsIn": c["exec"]["inputCount"],
                     "rowsKept": c["exec"]["keptCount"], "rowsDropped": c["exec"]["droppedCount"],
                     "dropped": c["exec"]["dropped"], "dq": c["exec"]["dq"]}
                    for c in conformed],
        "schemaChange": sink.get("schema_change", {"known": known, "new": new_cols}),
        "dagsterRun": {"success": result.success, "assets": asset_events},
    }


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


def _ddl(table: str, cols: list[str], types: dict[str, str]) -> str:
    body = ",\n".join(f"  {c} {_SQL_TYPE.get(types[c], 'TEXT')}" for c in cols)
    return f"CREATE TABLE {table} (\n{body}\n);"
