"""
db.py — relational warehouse (SQLAlchemy): real tables, schema, and foreign keys.

Replaces the local-JSON warehouse. Runs on local SQLite (FKs enabled) by default and
on Supabase/Postgres by setting DATABASE_URL — the same code, FKs enforced on both.

Two layers live here:
  * Staging tables  `stg_<table>` — one per ingested table, dynamic schema, created and
    ALTER-evolved on the fly; idempotent per upload (rows for an upload_id are replaced).
  * Star schema     `dim_customer / dim_product / dim_vendor` + `fact_sales`, with real
    FOREIGN KEY constraints from the fact to each dimension. `rebuild_star()` upserts the
    dimensions (from golden records) BEFORE inserting facts, so FK targets always exist.

Database-awareness: `introspect()` reports the live schema (tables, columns, FKs); writes
reflect existing tables and ADD COLUMN for new fields rather than failing.
"""
from __future__ import annotations

from sqlalchemy import (Column, Float, ForeignKey, Integer, MetaData, String, Table,
                        create_engine, event, inspect, text)

import config
from profiling import snake

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
_NUM = {"number", "decimal", "currency"}
_SA = {"integer": Integer}
_DDL = {"sqlite": {"num": "REAL", "int": "INTEGER", "text": "TEXT"},
        "default": {"num": "DOUBLE PRECISION", "int": "INTEGER", "text": "TEXT"}}


def _sa_type(t: str):
    if t in _NUM:
        return Float
    return _SA.get(t, String)


def _ddl_type(t: str) -> str:
    d = _DDL.get(ENGINE.dialect.name, _DDL["default"])
    return d["num"] if t in _NUM else (d["int"] if t == "integer" else d["text"])


def _coerce(v, t: str):
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return None
    if t in _NUM:
        try:
            return float(str(v).replace(",", "").replace("$", "").strip())
        except ValueError:
            return None
    if t == "integer":
        try:
            return int(float(v))
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
    """Staging tables that are sales facts: named sales* (the union namer's fact label)
    or carrying a money measure. Excludes product/customer/vendor dimension tables even
    when they have a quantity-like attribute."""
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


def _safe_url() -> str:
    u = config.DATABASE_URL
    if "@" in u:  # hide credentials
        return u.split("@", 1)[0].split("//", 1)[0] + "//***@" + u.split("@", 1)[1]
    return u
