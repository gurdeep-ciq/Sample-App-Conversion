"""
mastering.py — read the accumulated warehouse, build golden records, enrich facts.

This closes three gaps the per-file pipeline left open:

  * Query surface  — `list_tables()` / `read_table()` concatenate every persisted
    partition of a table so the warehouse is actually readable.
  * Entity mastering (golden records) — `compute_master()` harvests entity
    instances (customer / product / vendor) from EVERY table+partition and dedupes
    them: first by a normalized natural key (id, else name), then a conservative
    fuzzy-name pass to fold differently-coded variants of the same entity
    (e.g. "Sera Luce - Venetian Spritz" vs 'Sera Luce "Venetian Spritz" Cocktail').
  * Joins / enrichment — `enrich_rows()` attaches the matched golden id to each
    fact row, so a sale ties back to one canonical customer/product across formats.

Pure stdlib + rapidfuzz. No LLM. Fuzzy merging is deliberately conservative; it is
best-effort and reported as such (cross-format records with no shared key or name
overlap will not merge — that is the part that genuinely needs an LLM).
"""
from __future__ import annotations

import hashlib
import json
import os
import re

from rapidfuzz import fuzz

from dagster_pipeline import WAREHOUSE

# Entities we master, and where to find their id / name / attributes on a row.
ENTITY_SPECS = {
    "customer": {"id": "customer_id", "name": "customer",
                 "attrs": ["region", "channel", "sales_rep", "address_1", "city", "postal_code"]},
    "product":  {"id": "product_code", "name": "product",
                 "attrs": ["unit_size", "bottle_size", "unit_price", "vendor", "package_format"]},
    "vendor":   {"id": None, "name": "vendor", "attrs": ["region"]},
}
_NAME_FUZZ = 90  # token_set_ratio threshold for folding name-only variants


def _norm(s) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(s).lower()).strip()


def _is_blank(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


# --------------------------------------------------------------------------
# warehouse readers (the query surface)
# --------------------------------------------------------------------------
def _partitions(table: str) -> list[str]:
    d = os.path.join(WAREHOUSE, table)
    if not os.path.isdir(d):
        return []
    return sorted(fn[:-5] for fn in os.listdir(d)
                  if fn.endswith(".json") and not fn.endswith(".schema.json"))


def list_tables() -> list[dict]:
    if not os.path.isdir(WAREHOUSE):
        return []
    out = []
    for table in sorted(os.listdir(WAREHOUSE)):
        if table.startswith("_") or not os.path.isdir(os.path.join(WAREHOUSE, table)):
            continue
        parts = _partitions(table)
        rows = sum(len(_read_partition(table, p)) for p in parts)
        out.append({"table": table, "partitions": parts, "uploads": len(parts), "rowCount": rows})
    return out


def _read_partition(table: str, pk: str) -> list[dict]:
    path = os.path.join(WAREHOUSE, table, f"{pk}.json")
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []


def read_table(table: str, limit: int | None = None, enrich: bool = False) -> dict:
    """All rows of a table across every partition (each row tagged with its upload)."""
    rows: list[dict] = []
    for pk in _partitions(table):
        for r in _read_partition(table, pk):
            rows.append({**r, "_partition": pk})
    if enrich:
        rows = enrich_rows(rows)
    total = len(rows)
    if limit is not None:
        rows = rows[:limit]
    return {"table": table, "rowCount": total, "returned": len(rows), "rows": rows}


def _all_rows() -> list[tuple[str, dict]]:
    """(table, row) for every row in the warehouse — the mastering input."""
    out = []
    for t in list_tables():
        for pk in t["partitions"]:
            for r in _read_partition(t["table"], pk):
                out.append((t["table"], r))
    return out


# --------------------------------------------------------------------------
# golden records
# --------------------------------------------------------------------------
def _golden_id(entity: str, key: str) -> str:
    return entity[:3] + "_" + hashlib.sha1(f"{entity}:{key}".encode()).hexdigest()[:10]


def compute_master(entity: str) -> dict:
    """Dedupe all instances of one entity across the whole warehouse."""
    spec = ENTITY_SPECS.get(entity)
    if not spec:
        return {"entity": entity, "records": [], "error": "unknown entity"}

    id_f, name_f, attrs = spec["id"], spec["name"], spec["attrs"]
    buckets: dict[str, dict] = {}  # natural key -> aggregating record

    for table, row in _all_rows():
        idv = row.get(id_f) if id_f else None
        namev = row.get(name_f)
        if _is_blank(idv) and _is_blank(namev):
            continue
        key = _norm(idv) if not _is_blank(idv) else "name:" + _norm(namev)
        b = buckets.setdefault(key, {"key": key, "names": {}, "ids": set(),
                                     "attrs": {}, "sources": set(), "count": 0})
        b["count"] += 1
        b["sources"].add(table)
        if not _is_blank(idv):
            b["ids"].add(str(idv))
        if not _is_blank(namev):
            b["names"][str(namev)] = b["names"].get(str(namev), 0) + 1
        for a in attrs:
            if a not in b["attrs"] and not _is_blank(row.get(a)):
                b["attrs"][a] = row.get(a)

    # conservative fuzzy fold: merge name-keyed buckets (no id) whose canonical
    # names are near-identical — catches differently-spelled same entity.
    records = list(buckets.values())
    name_only = [r for r in records if r["key"].startswith("name:")]
    merged_into: dict[str, str] = {}
    for i in range(len(name_only)):
        for j in range(i + 1, len(name_only)):
            a, b = name_only[i], name_only[j]
            if a["key"] in merged_into or b["key"] in merged_into:
                continue
            na, nb = _canonical_name(a), _canonical_name(b)
            if na and nb and fuzz.token_set_ratio(_norm(na), _norm(nb)) >= _NAME_FUZZ:
                _absorb(a, b)
                merged_into[b["key"]] = a["key"]
    records = [r for r in records if r["key"] not in merged_into]

    out = []
    for r in records:
        out.append({
            "goldenId": _golden_id(entity, r["key"]),
            "name": _canonical_name(r),
            "ids": sorted(r["ids"]),
            "nameVariants": sorted(r["names"]),
            "attributes": r["attrs"],
            "sources": sorted(r["sources"]),
            "instanceCount": r["count"],
        })
    out.sort(key=lambda x: (-x["instanceCount"], x["name"] or ""))
    return {"entity": entity, "recordCount": len(out), "records": out}


def _canonical_name(rec: dict) -> str:
    """Most frequent name; ties broken by longest (most descriptive)."""
    if not rec["names"]:
        return ""
    return sorted(rec["names"].items(), key=lambda kv: (kv[1], len(kv[0])), reverse=True)[0][0]


def _absorb(into: dict, other: dict):
    into["count"] += other["count"]
    into["ids"] |= other["ids"]
    into["sources"] |= other["sources"]
    for n, c in other["names"].items():
        into["names"][n] = into["names"].get(n, 0) + c
    for a, v in other["attrs"].items():
        into["attrs"].setdefault(a, v)


# --------------------------------------------------------------------------
# enrichment (join facts -> golden ids)
# --------------------------------------------------------------------------
def enrich_rows(rows: list[dict]) -> list[dict]:
    """Attach golden customer/product ids to fact rows via the master indexes."""
    indexes = {e: _master_index(e) for e in ("customer", "product")}
    out = []
    for r in rows:
        e = dict(r)
        for entity in ("customer", "product"):
            spec, idx = ENTITY_SPECS[entity], indexes[entity]
            idv, namev = r.get(spec["id"]), r.get(spec["name"])
            gid = None
            if not _is_blank(idv):
                gid = idx["by_id"].get(str(idv))
            if gid is None and not _is_blank(namev):
                gid = idx["by_name"].get(_norm(namev))
            if gid:
                e[f"{entity}_golden_id"] = gid
        out.append(e)
    return out


def _master_index(entity: str) -> dict:
    m = compute_master(entity)
    by_id, by_name = {}, {}
    for rec in m["records"]:
        for i in rec["ids"]:
            by_id[i] = rec["goldenId"]
        for n in rec["nameVariants"]:
            by_name[_norm(n)] = rec["goldenId"]
    return {"by_id": by_id, "by_name": by_name}
