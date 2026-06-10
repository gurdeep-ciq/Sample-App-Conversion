"""
app.py — FastAPI backend for Pipeline Studio (cloud + database architecture).

Stateless and cloud-backed:
  * Uploaded files are stored in object storage (S3, or a local-dir fallback) keyed by
    content hash — never held in process memory. `/ingest` re-reads the file from storage,
    so there is no in-memory cache to lose or to block horizontal scaling.
  * Cleaned data is written to a real relational database (Supabase/Postgres, or local
    SQLite) as staging tables plus a star schema (dim_* + fact_sales with FOREIGN KEYS).

Endpoints
  GET  /health                 -> liveness + dagster/db/storage backends
  GET  /registry               -> canonical entities
  POST /registry/confirm       -> persist confirmed source->entity mappings (learned aliases)
  POST /profile  (file upload) -> store file in S3, profile all sheets, propose schema/rules
  POST /ingest   (json plan)   -> run Dagster pipeline(s), write DB staging, rebuild star schema
  GET  /tables                 -> staging tables in the DB
  GET  /tables/{name}          -> rows of one staging table (optionally enriched)
  GET  /master/{entity}        -> golden records (customer/product/vendor)
  GET  /schema                 -> live DB introspection (tables, columns, FOREIGN KEYS)
  GET  /sales                  -> fact_sales joined to its dimensions (FKs in action)
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile

import dagster
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import codegen
import config
import db as DB
import entities as E
import llm
import mastering as M
import profiling as P
import storage
from dagster_pipeline import run_detail, run_history, run_table_pipeline

app = FastAPI(title="Pipeline Studio backend", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DB.ensure_star()  # create dim_*/fact_sales (with FKs) on startup if absent


def _strip(obj):
    if isinstance(obj, dict):
        return {k: _strip(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [_strip(v) for v in obj]
    return obj


def _key(file_id: str, name: str) -> str:
    return f"{config.S3_PREFIX}/{file_id}/{name}"


def _profile_bytes(data: bytes, ext: str, filename: str) -> dict:
    """Parse + profile a file from raw bytes via a transient temp file (no caching)."""
    fd, path = tempfile.mkstemp(suffix=ext)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        return P.profile_workbook(path, filename)
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


@app.get("/health")
def health():
    return {"ok": True, "dagster": dagster.__version__, "engine": "compact-dagster (in-process)",
            "llm": config.has_llm(), "model": config.ANTHROPIC_MODEL if config.has_llm() else None,
            "db": config.db_backend(), "db_label": config.db_label(),
            "storage": config.storage_backend(), "db_configured": config.has_db()}


@app.get("/registry")
def get_registry():
    return {"entities": E.load_registry()}


class Confirmation(BaseModel):
    source: str
    entity: str
    role: str | None = None
    type: str | None = None


class ConfirmBody(BaseModel):
    confirmations: list[Confirmation]


@app.post("/registry/confirm")
def confirm(body: ConfirmBody):
    return {"entities": E.confirm_mappings([c.model_dump() for c in body.confirmations])}


@app.post("/profile")
async def profile(file: UploadFile = File(...)):
    data = await file.read()
    filename = file.filename or "upload.xlsx"
    ext = os.path.splitext(filename)[1] or ".xlsx"
    try:
        wb = _profile_bytes(data, ext, filename)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}")
    if not any(s["kind"] == "data" and s.get("_profile") for s in wb["sheets"]):
        raise HTTPException(status_code=422,
                            detail="No tabular data sheets found — the file looks empty or is all metadata.")

    # content hash = stable id (idempotent) and storage key; store file in S3 / local fallback.
    file_id = hashlib.sha1(data).hexdigest()[:12]
    storage.put_file(_key(file_id, "raw" + ext), data)
    storage.put_file(_key(file_id, "meta.json"), json.dumps({"filename": filename, "ext": ext}).encode())

    registry = E.load_registry()
    for s in wb["sheets"]:
        if s["kind"] == "data" and s.get("_profile"):
            s["entityMatches"] = E.match_profile(s["_profile"], registry)
    union = E.union_groups(wb["sheets"], registry)
    return {"fileId": file_id, "fileName": wb["fileName"], "storedAt": storage.url(_key(file_id, "raw" + ext)),
            "sheets": _strip(wb["sheets"]), "unionGroups": union, "registry": registry}


class IngestTable(BaseModel):
    table: str
    members: list[str]
    overrides: dict[str, str] = {}
    ruleOverrides: dict[str, list] | None = None


class IngestBody(BaseModel):
    fileId: str
    tables: list[IngestTable]


@app.post("/ingest")
def ingest(body: IngestBody):
    # stateless: re-read the file from storage and re-profile (no in-memory cache).
    try:
        meta = json.loads(storage.get_file(_key(body.fileId, "meta.json")))
        raw = storage.get_file(_key(body.fileId, "raw" + meta["ext"]))
    except KeyError:
        raise HTTPException(status_code=404, detail="Unknown fileId — re-run /profile.")
    wb = _profile_bytes(raw, meta["ext"], meta["filename"])
    registry = E.load_registry()
    by_name = {s["name"]: s for s in wb["sheets"]}
    upload_id = P.snake(meta["filename"]) + "_" + body.fileId

    results = []
    for t in body.tables:
        members = []
        for sheet_name in t.members:
            s = by_name.get(sheet_name)
            if not s or s["kind"] != "data":
                continue
            rules = (t.ruleOverrides or {}).get(sheet_name) or s["rules"]
            members.append({"sheet": sheet_name, "profile": s["_profile"], "rules": rules,
                            "schema_cols": s["schema"]["columns"], "overrides": t.overrides})
        if members:
            results.append(run_table_pipeline(t.table, members, registry, upload_id))

    star = DB.rebuild_star()  # upsert dims (golden records) then facts — FK-safe
    return {"tables": results, "uploadId": upload_id, "star": star}


class AutomateBody(BaseModel):
    fileId: str
    business_rules: str = ""


@app.post("/automate")
def automate(body: AutomateBody):
    """Full automation: the LLM proposes a star schema (aware of the existing AI
    warehouse), we generate a Dagster pipeline, run it, and FK-safely load into the
    database (Supabase when DATABASE_URL is set, else local SQLite). Stateless: the
    file is re-read from storage. The only human input is `business_rules`."""
    if not config.has_llm():
        raise HTTPException(status_code=400, detail="ANTHROPIC_API_KEY not set in backend/.env")
    try:
        meta = json.loads(storage.get_file(_key(body.fileId, "meta.json")))
        raw = storage.get_file(_key(body.fileId, "raw" + meta["ext"]))
    except KeyError:
        raise HTTPException(status_code=404, detail="Unknown fileId — re-run /profile.")
    wb = _profile_bytes(raw, meta["ext"], meta["filename"])
    registry = E.load_registry()
    for s in wb["sheets"]:
        if s["kind"] == "data" and s.get("_profile"):
            s["entityMatches"] = E.match_profile(s["_profile"], registry)

    before = DB.ai_introspect()
    try:
        schema = llm.propose_star_schema(wb, body.business_rules, existing_db_schema=before)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"LLM schema proposal failed: {e}")
    rows = DB.cleaned_rows_by_sheet(wb)
    upload_id = P.snake(meta["filename"]) + "_" + body.fileId
    run = codegen.build_and_run(schema, rows, meta["filename"], upload_id)
    after = DB.ai_introspect()
    before_names = {t["name"] for t in before["tables"]}
    return {
        "fileName": wb.get("fileName"), "db": DB.db_label(),
        "schema": {k: v for k, v in schema.items() if not k.startswith("_")},
        "usage": schema.get("_usage"),
        "dbBefore": sorted(before_names),
        "dbAfter": [{"name": t["name"], "columns": len(t["columns"]), "fks": len(t["fks"]),
                     "new": t["name"] not in before_names} for t in after["tables"]],
        "run": run,
    }


@app.get("/runs")
def runs():
    return {"runs": run_history()}


@app.get("/runs/{run_id}")
def run(run_id: str):
    return run_detail(run_id)


@app.get("/tables")
def tables():
    return {"tables": M.list_tables()}


@app.get("/tables/{name}")
def table(name: str, limit: int = 500, enrich: bool = False):
    return M.read_table(name, limit=limit, enrich=enrich)


@app.get("/master")
def masters():
    return {"entities": [{"entity": e, **{k: v for k, v in M.compute_master(e).items() if k != "records"}}
                         for e in M.ENTITY_SPECS]}


@app.get("/master/{entity}")
def master(entity: str):
    if entity not in M.ENTITY_SPECS:
        raise HTTPException(status_code=404, detail=f"Unknown entity '{entity}'.")
    return M.compute_master(entity)


@app.get("/schema")
def schema():
    """Live database introspection — tables, columns, and FOREIGN KEYS."""
    return DB.introspect()


@app.get("/sales")
def sales(limit: int = 200):
    """fact_sales joined to its dimensions — the relational model in action."""
    rows = DB.query(
        """
        SELECT f.id, f.order_date, f.quantity, f.amount, f.source_table,
               c.name AS customer, c.region, c.channel,
               p.name AS product, p.product_code,
               v.name AS vendor
        FROM fact_sales f
        LEFT JOIN dim_customer c ON f.customer_golden_id = c.golden_id
        LEFT JOIN dim_product  p ON f.product_golden_id  = p.golden_id
        LEFT JOIN dim_vendor   v ON f.vendor_golden_id   = v.golden_id
        ORDER BY f.id
        """, limit=limit)
    return {"rowCount": len(rows), "rows": rows}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
