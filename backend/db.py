"""
db.py — database awareness (Phases C + E).

Connects to a real Postgres — your `DATABASE_URL` (e.g. Supabase) when set, else a
self-contained local Postgres via `pgserver` (zero system install). Provides:

  introspect()    — read information_schema: tables, columns, PKs, FKs
  ensure_tables() — FK-safe DDL: create missing dims first, then facts (+FK constraints);
                    existing tables are reused, never dropped
  load()          — upsert dimensions on their natural key (→ surrogate pk), then insert
                    facts with FKs resolved from each row (so FK constraints never fail)

The same target DB is introspected before each run, so a second upload reuses the
dimensions the first one created instead of duplicating them.
"""
from __future__ import annotations

import os
from typing import Optional

import psycopg

import config
from profiling import apply_pipeline

_PGSERVER = None
_DDATA = os.path.join(os.path.dirname(__file__), ".pgdata")

_PG_TYPE = {"integer": "integer", "bigint": "bigint", "numeric": "numeric",
            "text": "text", "date": "date", "timestamp": "timestamp", "boolean": "boolean"}


def dsn() -> str:
    """Return a Postgres DSN — the configured DATABASE_URL, or a local pgserver."""
    if config.has_db():
        return config.DATABASE_URL
    global _PGSERVER
    if _PGSERVER is None:
        import pgserver
        os.makedirs(_DDATA, exist_ok=True)
        _PGSERVER = pgserver.get_server(_DDATA)
    return _PGSERVER.get_uri()


def connect():
    return psycopg.connect(dsn(), autocommit=True)


def db_label() -> str:
    return "Supabase/Postgres (DATABASE_URL)" if config.has_db() else "local Postgres (pgserver)"


# --------------------------------------------------------------------------
# introspection
# --------------------------------------------------------------------------
def introspect(schema: str = "public") -> dict:
    out: dict = {"schema": schema, "tables": []}
    with connect() as c:
        tabs = [r[0] for r in c.execute(
            "select table_name from information_schema.tables "
            "where table_schema=%s and table_type='BASE TABLE' order by table_name", (schema,)).fetchall()]
        for t in tabs:
            cols = [{"name": r[0], "type": r[1], "nullable": r[2] == "YES"} for r in c.execute(
                "select column_name, data_type, is_nullable from information_schema.columns "
                "where table_schema=%s and table_name=%s order by ordinal_position", (schema, t)).fetchall()]
            pk = [r[0] for r in c.execute(
                "select kcu.column_name from information_schema.table_constraints tc "
                "join information_schema.key_column_usage kcu on tc.constraint_name=kcu.constraint_name "
                "where tc.table_schema=%s and tc.table_name=%s and tc.constraint_type='PRIMARY KEY'",
                (schema, t)).fetchall()]
            fks = [{"column": r[0], "ref_table": r[1], "ref_column": r[2]} for r in c.execute(
                "select kcu.column_name, ccu.table_name, ccu.column_name "
                "from information_schema.table_constraints tc "
                "join information_schema.key_column_usage kcu on tc.constraint_name=kcu.constraint_name "
                "join information_schema.constraint_column_usage ccu on tc.constraint_name=ccu.constraint_name "
                "where tc.table_schema=%s and tc.table_name=%s and tc.constraint_type='FOREIGN KEY'",
                (schema, t)).fetchall()]
            out["tables"].append({"name": t, "columns": cols, "pk": pk, "fks": fks})
    return out


def _ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


# --------------------------------------------------------------------------
# FK-safe DDL
# --------------------------------------------------------------------------
def ensure_tables(schema: dict) -> dict:
    """Create missing dimension tables (then facts with FK constraints), and for
    tables that already exist, ALTER ADD any new columns the proposal introduces.
    Existing tables/columns are never dropped or retyped."""
    existing = {t["name"]: {c["name"] for c in t["columns"]} for t in introspect()["tables"]}
    created, reused, altered, ddl_log = [], [], [], []

    def add_missing_columns(c, table, columns):
        for col in columns:
            if col["role"] == "pk" or col["name"] in existing.get(table, set()):
                continue
            ddl = f'ALTER TABLE {_ident(table)} ADD COLUMN {_ident(col["name"])} {_PG_TYPE.get(col["type"], "text")}'
            c.execute(ddl)
            ddl_log.append(ddl)
            altered.append(f'{table}.{col["name"]}')
            existing[table].add(col["name"])

    with connect() as c:
        # dimensions first (facts FK into them)
        for d in schema.get("dimensions", []):
            t = d["table"]
            if t in existing:
                reused.append(t)
                add_missing_columns(c, t, d["columns"])
                continue
            cols_sql = []
            for col in d["columns"]:
                if col["role"] == "pk":
                    cols_sql.append(f'{_ident(col["name"])} integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY')
                else:
                    cols_sql.append(f'{_ident(col["name"])} {_PG_TYPE.get(col["type"], "text")}')
            ddl = f'CREATE TABLE {_ident(t)} (\n  ' + ",\n  ".join(cols_sql) + "\n)"
            c.execute(ddl)
            ddl_log.append(ddl)
            created.append(t)
            existing[t] = {col["name"] for col in d["columns"]}

        # facts, with FK constraints to dims that now exist
        for f in schema.get("facts", []):
            t = f["table"]
            if t in existing:
                reused.append(t)
                add_missing_columns(c, t, f["columns"])
                continue
            cols_sql, fk_sql = [], []
            for col in f["columns"]:
                if col["role"] == "pk":
                    cols_sql.append(f'{_ident(col["name"])} integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY')
                    continue
                cols_sql.append(f'{_ident(col["name"])} {_PG_TYPE.get(col["type"], "text")}')
                if col["role"] == "fk" and col.get("references") and "." in col["references"]:
                    rt, rc = col["references"].split(".", 1)
                    if rt in existing:
                        fk_sql.append(f'FOREIGN KEY ({_ident(col["name"])}) REFERENCES {_ident(rt)} ({_ident(rc)})')
            ddl = f'CREATE TABLE {_ident(t)} (\n  ' + ",\n  ".join(cols_sql + fk_sql) + "\n)"
            c.execute(ddl)
            ddl_log.append(ddl)
            created.append(t)
            existing[t] = {col["name"] for col in f["columns"]}
    return {"created": created, "reused": reused, "altered": altered, "ddl": ddl_log}


# --------------------------------------------------------------------------
# source rows keyed by original header (post-clean)
# --------------------------------------------------------------------------
def cleaned_rows_by_sheet(profiled: dict) -> dict:
    """{sheet_name: [ {original_header: cleaned_value, canonical_name: value} ]}"""
    out = {}
    for s in profiled["sheets"]:
        if s["kind"] != "data" or not s.get("_profile"):
            continue
        schema_cols = s["schema"]["columns"]
        res = apply_pipeline(s["_profile"], s.get("rules", []), schema_cols)
        name2src = {c["name"]: c.get("source", "") for c in schema_cols if c.get("include")}
        rows = []
        for r in res["rows"]:
            o = {}
            for name, val in r.items():
                src = name2src.get(name, "")
                if src and "+" not in src:
                    o[src] = val
                o[name] = val
            rows.append(o)
        out[s["name"]] = rows
    return out


def _src_col(source: str) -> Optional[str]:
    """'Sheet.Column' -> 'Column' (handles dots in either part best-effort)."""
    if not source or "." not in source:
        return None
    return source.split(".", 1)[1]


def _coerce(v, pgtype: str):
    """Coerce a cleaned source value to its declared column type. Empty/unparseable
    numerics/dates become NULL so they never break an integer/date column."""
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    if pgtype in ("integer", "bigint"):
        try:
            return int(float(s.replace(",", "")))
        except ValueError:
            return None
    if pgtype == "numeric":
        try:
            return float(s.replace(",", "").replace("$", ""))
        except ValueError:
            return None
    if pgtype == "boolean":
        return s.lower() in ("1", "true", "t", "yes", "y")
    if pgtype in ("date", "timestamp"):
        return s
    return v  # text


# --------------------------------------------------------------------------
# FK-safe load
# --------------------------------------------------------------------------
def load(schema: dict, rows_by_sheet: dict) -> dict:
    """Upsert dimensions (natural key -> surrogate pk), then insert facts with FKs
    resolved from each source row. Dim rows are co-loaded from each fact's rows."""
    dims = {d["table"]: d for d in schema.get("dimensions", [])}
    report = {"dimensions": {}, "facts": {}, "fk_unresolved": 0}
    actual_cols = {t["name"]: {col["name"] for col in t["columns"]} for t in introspect()["tables"]}

    with connect() as c:
        for f in schema.get("facts", []):
            # which sheet(s) feed this fact?
            sheets = set(f.get("source_sheets") or [])
            for col in f["columns"]:
                src = col.get("source", "")
                if "." in src:
                    sheets.add(src.split(".", 1)[0])
            sheets = [s for s in sheets if s in rows_by_sheet] or list(rows_by_sheet.keys())

            pk_cache: dict = {}  # (dim_table, natkey_tuple) -> pk
            fact_cols = [col for col in f["columns"] if col["role"] != "pk"]
            inserted = 0
            for sheet in sheets:
                for row in rows_by_sheet[sheet]:
                    fact_rec = {}
                    for col in fact_cols:
                        if col["role"] == "fk" and col.get("references") and "." in col["references"]:
                            dim_t = col["references"].split(".", 1)[0]
                            dim = dims.get(dim_t)
                            fact_rec[col["name"]] = _upsert_dim(c, dim, row, pk_cache, actual_cols) if dim else None
                            if fact_rec[col["name"]] is None and dim:
                                report["fk_unresolved"] += 1
                        else:
                            sc = _src_col(col.get("source", ""))
                            raw = row.get(sc) if sc else row.get(col["name"])
                            fact_rec[col["name"]] = _coerce(raw, col.get("type", "text"))
                    _insert(c, f["table"], fact_rec, actual_cols.get(f["table"]))
                    inserted += 1
            report["facts"][f["table"]] = inserted

        # report dim counts
        for t in dims:
            try:
                report["dimensions"][t] = c.execute(f"select count(*) from {_ident(t)}").fetchone()[0]
            except Exception:
                report["dimensions"][t] = None
    return report


def _upsert_dim(c, dim: dict, row: dict, cache: dict, actual_cols: dict) -> Optional[int]:
    t = dim["table"]
    allowed = actual_cols.get(t, set())
    nk = [k for k in (dim.get("natural_key") or []) if k in allowed]
    attr_cols = [col for col in dim["columns"] if col["role"] != "pk" and col["name"] in allowed]
    pk_col = next((col["name"] for col in dim["columns"] if col["role"] == "pk"), None)
    if not pk_col:
        return None

    rec = {}
    for col in attr_cols:
        sc = _src_col(col.get("source", ""))
        raw = row.get(sc) if sc else row.get(col["name"])
        rec[col["name"]] = _coerce(raw, col.get("type", "text"))

    nk_vals = tuple(rec.get(k) for k in nk) if nk else tuple(rec.get(col["name"]) for col in attr_cols)
    if all(v is None for v in nk_vals):
        return None
    ck = (t, nk_vals)
    if ck in cache:
        return cache[ck]

    # look up by natural key
    if nk:
        where = " and ".join(f"{_ident(k)} is not distinct from %s" for k in nk)
        found = c.execute(f"select {_ident(pk_col)} from {_ident(t)} where {where}",
                          [rec.get(k) for k in nk]).fetchone()
        if found:
            cache[ck] = found[0]
            return found[0]

    cols = [k for k in rec]
    ph = ", ".join(["%s"] * len(cols))
    pk = c.execute(
        f"insert into {_ident(t)} ({', '.join(_ident(x) for x in cols)}) values ({ph}) returning {_ident(pk_col)}",
        [rec[k] for k in cols]).fetchone()[0]
    cache[ck] = pk
    return pk


def fk_orphans(schema: str = "public") -> int:
    """Total rows whose FK value has no matching parent — should always be 0."""
    total = 0
    with connect() as c:
        for t in introspect(schema)["tables"]:
            for fk in t["fks"]:
                total += c.execute(
                    f'select count(*) from {_ident(t["name"])} f '
                    f'left join {_ident(fk["ref_table"])} d '
                    f'on f.{_ident(fk["column"])}=d.{_ident(fk["ref_column"])} '
                    f'where f.{_ident(fk["column"])} is not null and d.{_ident(fk["ref_column"])} is null'
                ).fetchone()[0]
    return total


def _insert(c, table: str, rec: dict, allowed: Optional[set] = None):
    cols = [k for k in rec if (allowed is None or k in allowed)]
    if not cols:
        return
    ph = ", ".join(["%s"] * len(cols))
    c.execute(f"insert into {_ident(table)} ({', '.join(_ident(x) for x in cols)}) values ({ph})",
              [rec[k] for k in cols])
