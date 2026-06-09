"""
app.py — FastAPI backend for Pipeline Studio.

Endpoints
  GET  /health                 -> liveness + dagster version
  GET  /registry               -> current canonical entities
  POST /registry/confirm       -> persist confirmed source->entity mappings (learned aliases)
  POST /profile  (file upload) -> profile ALL sheets, propose schema/rules, suggest entity
                                  matches, propose union groups; returns a fileId for /ingest
  POST /ingest   (json plan)   -> run the Dagster pipeline(s) for the chosen tables and return
                                  cleaned data + TableSchema + run events + schema-change verdict

The uploaded file's parsed profile is cached in-process (keyed by fileId) so /ingest
can run without re-uploading. Pure heuristics + Dagster; no LLM calls.
"""
from __future__ import annotations

import os
import tempfile
import uuid

import dagster
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import entities as E
import profiling as P
from dagster_pipeline import run_detail, run_history, run_table_pipeline

app = FastAPI(title="Pipeline Studio backend", version="1.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# in-process cache: fileId -> profiled workbook (includes _profile per sheet)
_CACHE: dict[str, dict] = {}


def _strip(obj):
    """Recursively drop private (_-prefixed) keys so the workbook is JSON-safe."""
    if isinstance(obj, dict):
        return {k: _strip(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [_strip(v) for v in obj]
    return obj


@app.get("/health")
def health():
    return {"ok": True, "dagster": dagster.__version__, "engine": "compact-dagster (in-process)"}


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
    reg = E.confirm_mappings([c.model_dump() for c in body.confirmations])
    return {"entities": reg}


@app.post("/profile")
async def profile(file: UploadFile = File(...)):
    suffix = os.path.splitext(file.filename or "upload.xlsx")[1] or ".xlsx"
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(await file.read())
        wb = P.profile_workbook(path, file.filename)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}")
    finally:
        try:
            os.remove(path)
        except OSError:
            pass

    registry = E.load_registry()
    # attach entity matches per data sheet
    for s in wb["sheets"]:
        if s["kind"] == "data" and s.get("_profile"):
            s["entityMatches"] = E.match_profile(s["_profile"], registry)
    union = E.union_groups(wb["sheets"], registry)

    file_id = uuid.uuid4().hex
    _CACHE[file_id] = wb

    return {
        "fileId": file_id,
        "fileName": wb["fileName"],
        "sheets": _strip(wb["sheets"]),
        "unionGroups": union,
        "registry": registry,
    }


class IngestTable(BaseModel):
    table: str
    members: list[str]                       # sheet names to union into this table
    overrides: dict[str, str] = {}           # source column -> canonical entity (confirmed)
    ruleOverrides: dict[str, list] | None = None  # optional: sheet -> edited rules list


class IngestBody(BaseModel):
    fileId: str
    tables: list[IngestTable]


@app.post("/ingest")
def ingest(body: IngestBody):
    wb = _CACHE.get(body.fileId)
    if not wb:
        raise HTTPException(status_code=404, detail="Unknown fileId — re-run /profile.")
    registry = E.load_registry()
    by_name = {s["name"]: s for s in wb["sheets"]}
    # each upload is one Dagster partition
    upload_id = P.snake(wb.get("fileName", "upload")) + "_" + body.fileId[:6]

    results = []
    for t in body.tables:
        members = []
        for sheet_name in t.members:
            s = by_name.get(sheet_name)
            if not s or s["kind"] != "data":
                continue
            rules = (t.ruleOverrides or {}).get(sheet_name) or s["rules"]
            members.append({
                "sheet": sheet_name,
                "profile": s["_profile"],
                "rules": rules,
                "schema_cols": s["schema"]["columns"],
                "overrides": t.overrides,
            })
        if not members:
            continue
        results.append(run_table_pipeline(t.table, members, registry, upload_id))

    return {"tables": results, "uploadId": upload_id}


@app.get("/runs")
def runs():
    return {"runs": run_history()}


@app.get("/runs/{run_id}")
def run(run_id: str):
    return run_detail(run_id)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
