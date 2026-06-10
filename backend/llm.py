"""
llm.py — real-LLM (Claude) capabilities for full automation.

  propose_star_schema  — analyze profiled source data → canonical star schema
                         (dimension + fact tables), optionally integrating into an
                         existing DB schema (FK-aware) and honoring business rules.

Output is forced through tool-use. To stay reliable on Haiku (which mangles deeply
nested tool inputs), the tool returns two FLAT arrays — `tables` and `columns` —
which we reassemble into nested dimensions/facts in Python.

Uses the model in config.ANTHROPIC_MODEL (Haiku by default).
"""
from __future__ import annotations

import json
from typing import Optional

import anthropic

import config

_client: Optional[anthropic.Anthropic] = None


def client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        if not config.has_llm():
            raise RuntimeError("ANTHROPIC_API_KEY not set (backend/.env)")
        _client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    return _client


def _sheets_payload(profiled: dict) -> list[dict]:
    """Compact, token-light view of profiled sheets for the model."""
    out = []
    for s in profiled["sheets"]:
        if s["kind"] != "data":
            continue
        ent_by_src = {}
        for m in s.get("entityMatches", []):
            ent_by_src[m["source"]] = (m.get("entity") or {}).get("entity")
        cols = [{"name": c["source"], "type": c["type"], "role": c["role"],
                 "entity": ent_by_src.get(c["source"]), "sample": c.get("sample", [])[:3]}
                for c in s["columns"]]
        out.append({"sheet": s["name"], "rows": s["rowCount"], "format": s.get("formatGuess"),
                    "columns": cols})
    return out


# FLAT tool schema — two arrays of flat objects (reliable on Haiku).
_SCHEMA_TOOL = {
    "name": "emit_star_schema",
    "description": "Return the proposed canonical star schema as two flat lists.",
    "input_schema": {
        "type": "object",
        "properties": {
            "tables": {
                "type": "array",
                "description": "Every fact and dimension table.",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "snake_case; dim_/fact_ prefix"},
                        "kind": {"type": "string", "enum": ["dimension", "fact"]},
                        "grain": {"type": "string", "description": "one row per ..."},
                        "natural_key": {"type": "array", "items": {"type": "string"},
                                        "description": "business key columns (dimensions)"},
                        "exists_in_db": {"type": "boolean",
                                         "description": "true if already present in the provided DB schema"},
                    },
                    "required": ["name", "kind", "grain"],
                },
            },
            "columns": {
                "type": "array",
                "description": "Every column, tagged with its table.",
                "items": {
                    "type": "object",
                    "properties": {
                        "table": {"type": "string"},
                        "name": {"type": "string"},
                        "type": {"type": "string",
                                 "enum": ["integer", "bigint", "numeric", "text", "date", "timestamp", "boolean"]},
                        "role": {"type": "string",
                                 "enum": ["pk", "fk", "measure", "attribute", "degenerate"]},
                        "source": {"type": "string", "description": "'sheet.column' from the data, or '' if surrogate/derived"},
                        "references": {"type": "string", "description": "'dim_table.pk_col' when role=fk"},
                    },
                    "required": ["table", "name", "type", "role"],
                },
            },
            "notes": {"type": "string"},
        },
        "required": ["tables", "columns"],
    },
}


def _coerce(obj):
    """Defensive: parse any string-encoded JSON the model emits for array fields."""
    if isinstance(obj, str):
        t = obj.strip()
        if t[:1] in ("[", "{"):
            try:
                return _coerce(json.loads(t))
            except json.JSONDecodeError:
                return obj
        return obj
    if isinstance(obj, list):
        return [_coerce(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _coerce(v) for k, v in obj.items()}
    return obj


def _assemble(flat: dict) -> dict:
    """Reassemble the flat (tables, columns) lists into nested dimensions/facts."""
    cols_by_table: dict[str, list] = {}
    for c in flat.get("columns", []):
        if not isinstance(c, dict) or "table" not in c:
            continue
        entry = {"name": c.get("name"), "type": c.get("type", "text"), "role": c.get("role", "attribute"),
                 "source": c.get("source", "")}
        if c.get("references"):
            entry["references"] = c["references"]
        cols_by_table.setdefault(c["table"], []).append(entry)

    dims, facts = [], []
    for t in flat.get("tables", []):
        if not isinstance(t, dict) or "name" not in t:
            continue
        entry = {"table": t["name"], "grain": t.get("grain", ""),
                 "columns": cols_by_table.get(t["name"], [])}
        if t.get("natural_key"):
            entry["natural_key"] = t["natural_key"]
        if t.get("exists_in_db"):
            entry["exists_in_db"] = True
        (dims if t.get("kind") == "dimension" else facts).append(entry)
    return {"dimensions": dims, "facts": facts, "notes": flat.get("notes", "")}


def propose_star_schema(profiled: dict, business_rules: str = "",
                        existing_db_schema: Optional[dict] = None) -> dict:
    """Analyze profiled source data and propose a canonical star schema.
    When `existing_db_schema` is provided, reuse/extend its tables (FK-aware)."""
    sheets = _sheets_payload(profiled)
    sys = (
        "You are a senior analytics engineer designing a dimensional (star-schema) "
        "warehouse. Given profiled source spreadsheet data, propose canonical fact and "
        "dimension TABLES and their COLUMNS as two flat lists. Rules: snake_case; each "
        "dimension gets a surrogate integer pk (role=pk, source='') plus a natural_key of "
        "business columns; facts hold measures and fk columns referencing dimension pks "
        "(role=fk, references='dim_x.x_key'); set every column's 'source' to its "
        "'sheet.column' when it comes from the data. Group compatible source sheets into "
        "shared facts/dims. Prefer FOUR-to-EIGHT total tables; do not over-normalize. "
        "Every dimension's natural_key MUST list real business columns that each have a "
        "non-empty 'source' — NEVER put a surrogate/pk column in natural_key. For a date "
        "dimension, the natural_key is the actual date column (e.g. date_value sourced from "
        "the date), not the surrogate date_key. Keep distinct grains in distinct fact tables; "
        "do not merge unrelated sources into one fact."
    )
    if existing_db_schema:
        sys += (
            " An EXISTING database schema is provided. REUSE existing tables and their exact "
            "column names/types/pk where the source maps onto them (set exists_in_db=true); "
            "only add new tables when nothing fits. Never choose a type that would violate an "
            "existing foreign key."
        )
    user = {"business_rules": business_rules or "(none)",
            "source_sheets": sheets,
            "existing_db_schema": existing_db_schema or "(none — greenfield)"}
    msg = client().messages.create(
        model=config.ANTHROPIC_MODEL, max_tokens=8192, system=sys,
        tools=[_SCHEMA_TOOL], tool_choice={"type": "tool", "name": "emit_star_schema"},
        messages=[{"role": "user", "content": json.dumps(user)}],
    )
    block = next((b for b in msg.content if getattr(b, "type", None) == "tool_use"), None)
    if block is None:
        raise RuntimeError("LLM did not return a schema")
    schema = _assemble(_coerce(block.input))
    schema["_usage"] = {"in": msg.usage.input_tokens, "out": msg.usage.output_tokens,
                        "model": config.ANTHROPIC_MODEL}
    return schema
