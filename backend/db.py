"""
db.py — relational warehouse (SQLAlchemy): real tables, schema, and foreign keys.

Runs on local SQLite (FKs enabled) by default and on Supabase/Postgres by setting
DATABASE_URL — the same code, FKs enforced on both.

Layers:
  * Staging tables  `stg_<table>` — dynamic, schema-evolving, idempotent per upload.
  * Canonical star  `dim_customer / dim_product / dim_vendor` + `fact_sales`, FK-enforced;
    `rebuild_star()` upserts dims (from golden records) BEFORE facts.
  * AI autopilot star (LLM-proposed) — created dynamically under an `ai_` namespace by
    `ensure_tables()` / `load()` so it never collides with the canonical star above.

Database-awareness: `introspect()` / `ai_introspect()` report the live schema; writes
reflect existing tables and ADD COLUMN for new fields rather than failing.
"""
from __future__ import annotations

from sqlalchemy import (Column, Float, ForeignKey, Integer, MetaData, String, Table,
                        create_engine, event, inspect, text)
from sqlalchemy.schema import CreateTable

import config
from profiling import apply_pipeline, snake

ENGINE = create_engine(config.DATABASE_URL, future=True)

# SQLite enforces foreign keys only when asked, per connection.
if ENGINE.dialect.name == "sqlite":
    @event.listens_for(ENGINE, "connect")
    def _fk_on(dbapi_conn, _rec):  # noqa: ANN001
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

_META = MetaData()

# ---- star schema (fixed) -------------------------------------------------
dim_customer = Table("dim_customer", _META,
    Column("golden_id", String, primary_key=True),
    Column("name", String), Column("region", String), Column("channel", String),
    Column("sales_rep", String), Column("address_1", String), Column("city", String),
    Column("postal_code", String), Column("sources", String))
dim_product = Table("dim_product", _META,
    Column("golden_id", String, primary_key=True),
    Column("name", String), Column("product_code", String), Column("unit_size", String),
    Column("bottle_size", String), Column("vendor", String), Column("sources", String))
dim_vendor = Table("dim_vendor", _META,
    Column("golden_id", String, primary_key=True),
    Column("name", String), Column("sources", String))
fact_sales = Table("fact_sales", _META,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("upload_id", String), Column("source_table", String),
    Column("order_date", String), Column("quantity", Float), Column("amount", Float),
    Column("unit_price", Float), Column("channel", String), Column("region", String),
    Column("customer_golden_id", String, ForeignKey("dim_customer.golden_id")),
    Column("product_golden_id", String, ForeignKey("dim_product.golden_id")),
    Column("vendor_golden_id", String, ForeignKey("dim_vendor.golden_id")))


def ensure_star():
    _META.create_all(ENGINE)  # checkfirst — idempotent


# ---- type handling -------------------------------------------------------
# Recognize BOTH the profiler's vocabulary (number/decimal/currency) and the LLM
# tool's vocabulary (numeric/bigint/...) so measures land as real numbers, not TEXT.
_NUM = {"number", "decimal", "currency", "numeric", "float", "double"}
_INT = {"integer", "int", "bigint", "smallint"}
_DDL = {"sqlite": {"num": "REAL", "int": "INTEGER", "text": "TEXT"},
        "default": {"num": "DOUBLE PRECISION", "int": "BIGINT", "text": "TEXT"}}


def _sa_type(t: str):
    if t in _NUM:
        return Float
    if t in _INT:
        return Integer
    return String


def _ddl_type(t: str) -> str:
    d = _DDL.get(ENGINE.dialect.name, _DDL["default"])
    return d["num"] if t in _NUM else (d["int"] if t in _INT else d["text"])


def _coerce(v, t: str):
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return None
    if t in _NUM:
        try:
            return float(str(v).replace(",", "").replace("$", "").strip())
        except ValueError:
            return None
    if t in _INT:
        try:
            return int(float(str(v).replace(",", "").strip()))
        except (ValueError, TypeError):
            return None
    return str(v)


# ---- staging tables (dynamic, schema-evolving, idempotent) ---------------
def staging_columns(table: str) -> list[str]:
    tn = "stg_" + snake(table)
    insp = inspect(ENGINE)
    if not insp.has_table(tn):
        return []
    return [c["name"] for c in insp.get_columns(tn) if c["name"] not in ("_row", "upload_id")]


def write_staging(table: str, col_order: list[str], col_types: dict[str, str],
                  rows: list[dict], upload_id: str) -> dict:
    tn = "stg_" + snake(table)
    insp = inspect(ENGINE)
    if not insp.has_table(tn):
        cols = [Column("_row", Integer, primary_key=True, autoincrement=True),
                Column("upload_id", String)]
        cols += [Column(c, _sa_type(col_types.get(c, "string"))) for c in col_order]
        Table(tn, MetaData(), *cols).create(ENGINE)
        added = list(col_order)
    else:  # schema evolution: ADD COLUMN for any new fields
        existing = {c["name"] for c in insp.get_columns(tn)}
        added = [c for c in col_order if c not in existing]
        for c in added:
            with ENGINE.begin() as cx:
                cx.execute(text(f'ALTER TABLE {tn} ADD COLUMN "{c}" {_ddl_type(col_types.get(c, "string"))}'))
    t = Table(tn, MetaData(), autoload_with=ENGINE)
    payload = [{**{c: _coerce(r.get(c), col_types.get(c, "string")) for c in col_order},
                "upload_id": upload_id} for r in rows]
    with ENGINE.begin() as cx:
        cx.execute(t.delete().where(t.c.upload_id == upload_id))  # idempotent re-ingest
        if payload:
            cx.execute(t.insert(), payload)
    return {"table": tn, "rows": len(payload), "addedColumns": added}


def list_staging() -> list[dict]:
    insp = inspect(ENGINE)
    out = []
    for tn in insp.get_table_names():
        if not tn.startswith("stg_"):
            continue
        t = Table(tn, MetaData(), autoload_with=ENGINE)
        with ENGINE.connect() as cx:
            rows = [dict(r._mapping) for r in cx.execute(t.select())]
        uploads = sorted({r.get("upload_id") for r in rows if r.get("upload_id")})
        out.append({"table": tn[4:], "rowCount": len(rows), "uploads": len(uploads),
                    "partitions": uploads})
    return out


def read_staging(table: str) -> list[dict]:
    tn = "stg_" + snake(table)
    insp = inspect(ENGINE)
    if not insp.has_table(tn):
        return []
    t = Table(tn, MetaData(), autoload_with=ENGINE)
    with ENGINE.connect() as cx:
        return [{k: v for k, v in dict(r._mapping).items() if k != "_row"}
                for r in cx.execute(t.select())]


# ---- star-schema rebuild (FK-safe: dims before facts) --------------------
def _index(master: dict) -> dict:
    import mastering as M
    by_id, by_name = {}, {}
    for rec in master["records"]:
        for i in rec["ids"]:
            by_id[str(i)] = rec["goldenId"]
        for n in rec["nameVariants"]:
            by_name[M._norm(n)] = rec["goldenId"]
    return {"by_id": by_id, "by_name": by_name}


def _fact_staging() -> list[str]:
    """Staging tables that are sales facts: named sales* or carrying a money measure."""
    return [t["table"] for t in list_staging()
            if t["table"].startswith("sales") or "amount" in set(staging_columns(t["table"]))]


def rebuild_star() -> dict:
    """Repopulate dims + facts from staging. Idempotent: full rebuild each call."""
    import mastering as M
    ensure_star()
    masters = {e: M.compute_master(e) for e in ("customer", "product", "vendor")}
    idx = {e: _index(masters[e]) for e in masters}

    def dim_row(e, rec):
        a = rec["attributes"]
        base = {"golden_id": rec["goldenId"], "name": rec["name"], "sources": ",".join(rec["sources"])}
        if e == "customer":
            base.update({k: a.get(k) for k in ("region", "channel", "sales_rep", "address_1", "city", "postal_code")})
        elif e == "product":
            base.update({"product_code": (rec["ids"][0] if rec["ids"] else None),
                         "unit_size": a.get("unit_size"), "bottle_size": a.get("bottle_size"),
                         "vendor": a.get("vendor")})
        return base

    def fact_row(r, stg):
        cid = idx["customer"]["by_id"].get(str(r.get("customer_id"))) \
            or idx["customer"]["by_name"].get(M._norm(r.get("customer"))) if (r.get("customer_id") or r.get("customer")) else None
        pid = idx["product"]["by_id"].get(str(r.get("product_code"))) \
            or idx["product"]["by_name"].get(M._norm(r.get("product"))) if (r.get("product_code") or r.get("product")) else None
        vid = idx["vendor"]["by_name"].get(M._norm(r.get("vendor"))) if r.get("vendor") else None
        return {"upload_id": r.get("upload_id"), "source_table": stg,
                "order_date": r.get("order_date"),
                "quantity": _coerce(r.get("quantity"), "number"),
                "amount": _coerce(r.get("amount"), "number"),
                "unit_price": _coerce(r.get("unit_price"), "number"),
                "channel": r.get("channel"), "region": r.get("region"),
                "customer_golden_id": cid, "product_golden_id": pid, "vendor_golden_id": vid}

    with ENGINE.begin() as cx:
        cx.execute(fact_sales.delete())                       # child first
        for e, tbl in (("customer", dim_customer), ("product", dim_product), ("vendor", dim_vendor)):
            cx.execute(tbl.delete())
            rows = [dim_row(e, rec) for rec in masters[e]["records"]]
            if rows:
                cx.execute(tbl.insert(), rows)
        facts = [fact_row(r, stg) for stg in _fact_staging() for r in read_staging(stg)]
        if facts:
            cx.execute(fact_sales.insert(), facts)
    return {"dim_customer": len(masters["customer"]["records"]),
            "dim_product": len(masters["product"]["records"]),
            "dim_vendor": len(masters["vendor"]["records"]),
            "fact_sales": len(facts)}


# ---- introspection + queries ---------------------------------------------
def introspect() -> dict:
    insp = inspect(ENGINE)
    tables = {}
    for tn in insp.get_table_names():
        fks = [{"columns": fk["constrained_columns"],
                "references": fk["referred_table"], "refColumns": fk["referred_columns"]}
               for fk in insp.get_foreign_keys(tn)]
        tables[tn] = {"columns": [{"name": c["name"], "type": str(c["type"])} for c in insp.get_columns(tn)],
                      "foreignKeys": fks}
    return {"backend": config.db_backend(), "url": _safe_url(), "tables": tables}


def query(sql: str, params: dict | None = None, limit: int = 1000) -> list[dict]:
    with ENGINE.connect() as cx:
        res = cx.execute(text(sql), params or {})
        return [dict(r._mapping) for r in res.fetchmany(limit)]


def insert_fact_raw(row: dict):
    """Direct fact insert — used by tests to prove FK enforcement."""
    with ENGINE.begin() as cx:
        cx.execute(fact_sales.insert(), [row])


def reset_warehouse() -> dict:
    """Drop EVERY table (ai_*, stg_*, dims, facts) and recreate the empty canonical
    star. Wipes all previously loaded data. reflect()+drop_all() honors FK order."""
    names = inspect(ENGINE).get_table_names()
    md = MetaData()
    md.reflect(bind=ENGINE)
    md.drop_all(bind=ENGINE)
    ensure_star()  # recreate empty dim_*/fact_sales
    return {"dropped": sorted(names)}


# ---- export helpers ------------------------------------------------------
def resolve_table(name: str) -> str | None:
    """Map a logical name to a real table: exact, or the ai_/stg_ namespaced form.
    Returns None if no such table (also guards against SQL injection — only real
    table names are ever interpolated)."""
    real = set(inspect(ENGINE).get_table_names())
    for cand in (name, _AI + snake(name), "stg_" + snake(name)):
        if cand in real:
            return cand
    return None


def export_rows(name: str) -> tuple[str, list[dict]] | None:
    tn = resolve_table(name)
    if not tn:
        return None
    return tn, query(f'SELECT * FROM "{tn}"', limit=1_000_000)


def export_ddl(names: list[str]) -> str:
    """CREATE TABLE statements (FK constraints included) for the given tables,
    compiled to the active dialect — the schema you could replay on Supabase."""
    md = MetaData()
    out = []
    for n in names:
        tn = resolve_table(n)
        if not tn:
            continue
        t = Table(tn, md, autoload_with=ENGINE)
        out.append(str(CreateTable(t).compile(ENGINE)).strip().rstrip(";") + ";")
    return "\n\n".join(out)


def _safe_url() -> str:
    u = config.DATABASE_URL
    if "@" in u:  # hide credentials
        return u.split("@", 1)[0].split("//", 1)[0] + "//***@" + u.split("@", 1)[1]
    return u


# ==========================================================================
# AI autopilot — LLM-proposed DYNAMIC star, isolated under an `ai_` prefix so it
# never collides with the canonical star above. Used by /automate + codegen.
# Runs on the same SQLAlchemy engine (SQLite locally / Supabase via DATABASE_URL).
# ==========================================================================
_AI = "ai_"
UPLOAD_COL = "_upload_id"   # hidden per-upload tag on fact tables → idempotent re-ingest


def db_label() -> str:
    return config.db_label()


def _ai(name: str) -> str:
    return _AI + snake(name)


def _pk_ddl() -> str:
    return ("INTEGER PRIMARY KEY AUTOINCREMENT" if ENGINE.dialect.name == "sqlite"
            else "integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY")


def ai_introspect() -> dict:
    """Existing AI-autopilot tables (ai_ prefix stripped) for the LLM's DB-awareness."""
    insp = inspect(ENGINE)
    tables = []
    for tn in insp.get_table_names():
        if not tn.startswith(_AI):
            continue
        cols = [{"name": c["name"], "type": str(c["type"]), "nullable": c.get("nullable", True)}
                for c in insp.get_columns(tn)]
        pk = [c["name"] for c in insp.get_columns(tn) if c.get("primary_key")]
        fks = [{"column": fk["constrained_columns"][0],
                "ref_table": fk["referred_table"][len(_AI):] if fk["referred_table"].startswith(_AI) else fk["referred_table"],
                "ref_column": fk["referred_columns"][0]}
               for fk in insp.get_foreign_keys(tn) if fk.get("constrained_columns")]
        tables.append({"name": tn[len(_AI):], "columns": cols, "pk": pk, "fks": fks})
    return {"schema": config.db_backend(), "tables": tables}


def _src_col(source: str):
    return source.split(".", 1)[1] if source and "." in source else None


def ensure_tables(schema: dict) -> dict:
    """Create/extend the LLM-proposed star (ai_ namespace), FK-safe: dims first, then
    facts with FK constraints; existing tables get ALTER ADD COLUMN for new fields."""
    insp = inspect(ENGINE)
    existing = {tn[len(_AI):]: {c["name"] for c in insp.get_columns(tn)}
                for tn in insp.get_table_names() if tn.startswith(_AI)}
    created, reused, altered, ddl = [], [], [], []

    def addcols(cx, table, columns):
        for col in columns:
            if col["role"] == "pk" or col["name"] in existing.get(table, set()):
                continue
            s = f'ALTER TABLE {_ai(table)} ADD COLUMN "{col["name"]}" {_ddl_type(col["type"])}'
            cx.execute(text(s)); ddl.append(s); altered.append(f'{table}.{col["name"]}'); existing[table].add(col["name"])

    def ensure_upload_col(cx, t):
        # facts carry a hidden _upload_id so a re-run can replace its own rows (idempotent).
        if UPLOAD_COL not in existing.get(t, set()):
            cx.execute(text(f'ALTER TABLE {_ai(t)} ADD COLUMN "{UPLOAD_COL}" TEXT'))
            existing.setdefault(t, set()).add(UPLOAD_COL)

    def create(cx, t, columns, is_fact):
        colsql, fksql = [], []
        for col in columns:
            if col["role"] == "pk":
                colsql.append(f'"{col["name"]}" {_pk_ddl()}'); continue
            colsql.append(f'"{col["name"]}" {_ddl_type(col["type"])}')
            if is_fact and col["role"] == "fk" and col.get("references") and "." in col["references"]:
                rt, rc = col["references"].split(".", 1)
                if rt in existing:
                    fksql.append(f'FOREIGN KEY ("{col["name"]}") REFERENCES {_ai(rt)} ("{rc}")')
        if is_fact:
            colsql.append(f'"{UPLOAD_COL}" TEXT')
        s = f'CREATE TABLE {_ai(t)} (\n  ' + ",\n  ".join(colsql + fksql) + "\n)"
        cx.execute(text(s)); ddl.append(s); created.append(t)
        existing[t] = {c["name"] for c in columns} | ({UPLOAD_COL} if is_fact else set())

    with ENGINE.begin() as cx:
        for d in schema.get("dimensions", []):
            t = d["table"]
            if t in existing:
                reused.append(t); addcols(cx, t, d["columns"])
            else:
                create(cx, t, d["columns"], False)
        for f in schema.get("facts", []):
            t = f["table"]
            if t in existing:
                reused.append(t); addcols(cx, t, f["columns"]); ensure_upload_col(cx, t)
            else:
                create(cx, t, f["columns"], True)
    return {"created": created, "reused": reused, "altered": altered, "ddl": ddl}


def load(schema: dict, rows_by_sheet: dict, upload_id: str | None = None) -> dict:
    """Upsert dims (natural key -> surrogate pk), then insert FK-resolved facts — into
    the ai_ namespace, FK-safe. Idempotent: when `upload_id` is given, a fact table's
    rows from a prior run of the same upload are deleted before re-insert, so re-running
    /automate on the same file does not double-count (dims dedupe via natural key)."""
    dims = {d["table"]: d for d in schema.get("dimensions", [])}
    report = {"dimensions": {}, "facts": {}, "fk_unresolved": 0}
    insp = inspect(ENGINE)
    actual = {tn[len(_AI):]: {c["name"] for c in insp.get_columns(tn)}
              for tn in insp.get_table_names() if tn.startswith(_AI)}
    types_by_table = {}
    for grp in (schema.get("dimensions", []), schema.get("facts", [])):
        for tbl in grp:
            types_by_table[tbl["table"]] = {c["name"]: c.get("type", "text") for c in tbl["columns"]}

    def _new_pk(cx, table, rec, pkc):
        cols = list(rec.keys())
        body = ", ".join(f'"{c}"' for c in cols)
        binds = ", ".join(f":{c}" for c in cols)
        if ENGINE.dialect.name == "sqlite":
            cx.execute(text(f'INSERT INTO {_ai(table)} ({body}) VALUES ({binds})'), rec)
            return cx.execute(text("SELECT last_insert_rowid()")).fetchone()[0]
        return cx.execute(text(f'INSERT INTO {_ai(table)} ({body}) VALUES ({binds}) RETURNING "{pkc}"'), rec).fetchone()[0]

    with ENGINE.begin() as cx:
        cache = {}

        def upsert_dim(dim, row):
            t = dim["table"]; allowed = actual.get(t, set()); types = types_by_table.get(t, {})
            nk = [k for k in (dim.get("natural_key") or []) if k in allowed]
            pkc = next((c["name"] for c in dim["columns"] if c["role"] == "pk"), None)
            if not pkc:
                return None
            rec = {c["name"]: _coerce(row.get(_src_col(c.get("source", "")) or c["name"]), types.get(c["name"], "text"))
                   for c in dim["columns"] if c["role"] != "pk" and c["name"] in allowed}
            nkvals = tuple(rec.get(k) for k in nk) if nk else tuple(rec.values())
            if all(v is None for v in nkvals):
                return None
            ck = (t, nkvals)
            if ck in cache:
                return cache[ck]
            if nk:
                conds, params = [], {}
                for i, k in enumerate(nk):
                    v = rec.get(k)
                    if v is None:
                        conds.append(f'"{k}" IS NULL')
                    else:
                        conds.append(f'"{k}" = :p{i}'); params[f"p{i}"] = v
                found = cx.execute(text(f'SELECT "{pkc}" FROM {_ai(t)} WHERE ' + " AND ".join(conds) + " LIMIT 1"),
                                   params).fetchone()
                if found:
                    cache[ck] = found[0]; return found[0]
            if not rec:
                return None
            pk = _new_pk(cx, t, rec, pkc)
            cache[ck] = pk; return pk

        for f in schema.get("facts", []):
            sheets = set(f.get("source_sheets") or [])
            for col in f["columns"]:
                if "." in col.get("source", ""):
                    sheets.add(col["source"].split(".", 1)[0])
            sheets = [s for s in sheets if s in rows_by_sheet] or list(rows_by_sheet.keys())
            allowedf = actual.get(f["table"], set()); types = types_by_table.get(f["table"], {})
            # idempotency: drop this upload's prior rows before re-inserting.
            if upload_id and UPLOAD_COL in allowedf:
                cx.execute(text(f'DELETE FROM {_ai(f["table"])} WHERE "{UPLOAD_COL}" = :u'), {"u": upload_id})
            inserted = 0
            for sheet in sheets:
                for row in rows_by_sheet[sheet]:
                    rec = {}
                    for col in f["columns"]:
                        if col["role"] == "pk" or col["name"] not in allowedf:
                            continue
                        if col["role"] == "fk" and col.get("references") and "." in col["references"]:
                            dim = dims.get(col["references"].split(".", 1)[0])
                            rec[col["name"]] = upsert_dim(dim, row) if dim else None
                            if rec[col["name"]] is None and dim:
                                report["fk_unresolved"] += 1
                        else:
                            rec[col["name"]] = _coerce(row.get(_src_col(col.get("source", "")) or col["name"]),
                                                       types.get(col["name"], "text"))
                    if upload_id and UPLOAD_COL in allowedf:
                        rec[UPLOAD_COL] = upload_id
                    cols = [c for c in rec]
                    if cols:
                        body = ", ".join(f'"{c}"' for c in cols)
                        binds = ", ".join(f":{c}" for c in cols)
                        cx.execute(text(f'INSERT INTO {_ai(f["table"])} ({body}) VALUES ({binds})'), rec)
                        inserted += 1
            report["facts"][f["table"]] = inserted

        for t in dims:
            try:
                report["dimensions"][t] = cx.execute(text(f'SELECT count(*) FROM {_ai(t)}')).fetchone()[0]
            except Exception:
                report["dimensions"][t] = None
    return report


def fk_orphans() -> int:
    """Total orphan rows across the ai_ namespace's foreign keys — should be 0."""
    insp = inspect(ENGINE)
    total = 0
    with ENGINE.connect() as cx:
        for tn in insp.get_table_names():
            if not tn.startswith(_AI):
                continue
            for fk in insp.get_foreign_keys(tn):
                if not fk.get("constrained_columns"):
                    continue
                col = fk["constrained_columns"][0]; rt = fk["referred_table"]; rc = fk["referred_columns"][0]
                total += cx.execute(text(
                    f'SELECT count(*) FROM {tn} f LEFT JOIN {rt} d ON f."{col}"=d."{rc}" '
                    f'WHERE f."{col}" IS NOT NULL AND d."{rc}" IS NULL')).fetchone()[0]
    return total


def cleaned_rows_by_sheet(profiled: dict, apply_filters: bool = False) -> dict:
    """{sheet_name: [ {original_header: cleaned_value, canonical_name: value} ]}

    By default NO rows are dropped — every source row loads (column casts/transforms
    still apply). Set apply_filters=True to also run the heuristic row filters
    (drop blank/internal/zero rows); off by default so exports match the source count."""
    out = {}
    for s in profiled["sheets"]:
        if s["kind"] != "data" or not s.get("_profile"):
            continue
        schema_cols = s["schema"]["columns"]
        rules = s.get("rules", [])
        if not apply_filters:  # keep column ops (cast/combine/derive/drop_column); skip row droppers
            rules = [{**r, "enabled": r.get("enabled") and r.get("kind") not in ("filter", "dedupe")}
                     for r in rules]
        res = apply_pipeline(s["_profile"], rules, schema_cols)
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
