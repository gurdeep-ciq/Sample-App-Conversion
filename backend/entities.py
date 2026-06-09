"""
entities.py — canonical entity registry + cross-format column matching.

Solves "refine schema across formats for the same entities": different source
column titles (CustName / Customer Name / Account) resolve to one canonical
entity, so data joins regardless of the vendor's naming. Matches are *suggested*
with confidence; the UI confirms/overrides, and confirmations are persisted as
learned aliases so future files auto-resolve.
"""
from __future__ import annotations

import json
import os
import re
from typing import Optional

from rapidfuzz import fuzz

REGISTRY_PATH = os.path.join(os.path.dirname(__file__), "registry.json")
MATCH_THRESHOLD = 0.62

# Seed registry — generic sales/transaction entities. `expect` drives type-compat
# scoring; `aliases` grows over time from confirmed mappings.
_SEED = [
    {"name": "order_date", "role": "timestamp", "type": "date", "expect": "date",
     "aliases": ["date", "order date", "delivery date", "sale date", "transaction date", "period", "event date", "event timestamp"]},
    {"name": "customer", "role": "dimension", "type": "string", "expect": "text",
     "aliases": ["customer", "customer name", "cust name", "custname", "account", "account name", "buyer", "client", "store", "outlet", "ship to"]},
    {"name": "customer_id", "role": "id", "type": "string", "expect": "id",
     "aliases": ["customer id", "customer code", "customer key", "account id", "account code", "account number", "cust id", "customer group id"]},
    {"name": "product", "role": "dimension", "type": "string", "expect": "text",
     "aliases": ["product", "item", "description", "product description", "wine", "product name", "item name", "brand"]},
    {"name": "product_code", "role": "id", "type": "string", "expect": "id",
     "aliases": ["sku", "upc", "ean", "gtin", "barcode", "product code", "item code", "supplier's sku", "code", "product key"]},
    {"name": "quantity", "role": "measure", "type": "decimal", "expect": "number",
     "aliases": ["qty", "quantity", "units", "cases", "sold", "number of sales", "qty sold"]},
    {"name": "unit_price", "role": "measure", "type": "decimal", "expect": "number",
     "aliases": ["price", "unit price", "fob", "msrp", "unit msrp", "cost", "default selling price"]},
    {"name": "amount", "role": "measure", "type": "decimal", "expect": "number",
     "aliases": ["amount", "total", "net sales", "gross sales", "revenue", "dollars sold", "extended", "sale amount", "sale msrp", "line total"]},
    {"name": "region", "role": "dimension", "type": "string", "expect": "text",
     "aliases": ["state", "region", "account region", "market", "territory", "cust sales market", "province"]},
    {"name": "postal_code", "role": "id", "type": "string", "expect": "id",
     "aliases": ["zip", "zip code", "zipcode", "postal", "postal code", "post code"]},
    {"name": "sales_rep", "role": "dimension", "type": "string", "expect": "text",
     "aliases": ["sales rep", "salesperson", "rep", "sales representative", "rep code", "account rep"]},
    {"name": "channel", "role": "dimension", "type": "string", "expect": "text",
     "aliases": ["division", "channel", "premise", "on premise", "off premise", "account type", "customer division"]},
    {"name": "vendor", "role": "dimension", "type": "string", "expect": "text",
     "aliases": ["importer", "supplier", "supplier name", "distributor", "vendor", "data vendor"]},
]


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(s).lower()).strip()


def load_registry() -> list[dict]:
    if os.path.exists(REGISTRY_PATH):
        try:
            with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    save_registry(_SEED)
    return [dict(e) for e in _SEED]


def save_registry(registry: list[dict]) -> None:
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2)


def _type_bonus(entity: dict, col: dict) -> float:
    """Multiplier that rewards/penalizes type agreement between a candidate
    entity and the actual column shape."""
    exp = entity.get("expect")
    ctype = col.get("type")
    if exp == "number":
        return 1.0 if ctype in ("number", "currency") else 0.7
    if exp == "date":
        return 1.0 if ctype in ("date", "time") else 0.72
    if exp == "id":
        # only reward genuine identifiers, not any text column
        strong = col.get("leadingZero") or col.get("bigIntNum") or col.get("role") == "id"
        return 1.0 if strong else 0.8
    if exp == "text":
        if ctype == "boolean":
            return 0.4   # a yes/no flag is never a name/region/channel value
        return 1.0 if ctype == "string" else 0.8
    return 0.95


def _sim(a: str, b: str) -> float:
    """Length-aware similarity. token_sort penalizes extra tokens (so 'group'
    does not score 100 against 'customer group id'); partial gives mild credit
    for genuine substrings. Exact-alias hits are handled separately, higher."""
    return (0.7 * fuzz.token_sort_ratio(a, b) + 0.3 * fuzz.partial_ratio(a, b)) / 100.0


def match_column(source_name: str, col: dict, registry: list[dict]) -> Optional[dict]:
    """Best canonical entity for one source column, with alternatives.
    Returns None when nothing clears the threshold (i.e. a new/unknown entity)."""
    n = _norm(source_name)
    scored = []
    for ent in registry:
        names = [_norm(ent["name"])] + [_norm(a) for a in ent.get("aliases", [])]
        if n in names:
            base, via = 0.99, "alias"
        else:
            base = max((_sim(n, x) for x in names), default=0.0)
            via = "fuzzy"
        score = round(base * _type_bonus(ent, col), 4)
        scored.append((score, via, ent["name"]))
    scored.sort(key=lambda t: (t[0], t[1] == "alias"), reverse=True)
    if not scored:
        return None
    top_score, top_via, top_name = scored[0]
    alts = [{"entity": nm, "confidence": sc} for sc, _, nm in scored[1:4]]
    if top_score < MATCH_THRESHOLD:
        return {"entity": None, "confidence": top_score, "via": "none",
                "suggestNew": True, "alternatives": [{"entity": nm, "confidence": sc} for sc, _, nm in scored[:3]]}
    return {"entity": top_name, "confidence": top_score, "via": top_via, "alternatives": alts}


def match_profile(profile: dict, registry: list[dict]) -> list[dict]:
    """Attach an entity match to every column of a sheet profile."""
    out = []
    for c in profile["columns"]:
        m = match_column(c["source"], c, registry)
        out.append({"source": c["source"], "canonical": c["canonical"],
                    "type": c["type"], "role": c["role"], "entity": m})
    return out


def matched_entity_set(col_matches: list[dict]) -> set[str]:
    return {m["entity"]["entity"] for m in col_matches
            if m["entity"] and m["entity"].get("entity")}


def union_groups(sheets: list[dict], registry: list[dict], threshold: float = 0.85) -> list[dict]:
    """Group data sheets whose canonical-entity sets overlap enough to be unioned
    into one table. Uses Jaccard similarity over matched entities (union-find).

    The threshold is deliberately high: only near-identical schemas (e.g. the same
    vendor's monthly exports) should be stacked. Sheets of a relational workbook —
    a product master, a customer master, and a sales fact — overlap partially but
    have different grains and must stay separate tables, not be unioned."""
    data = [s for s in sheets if s["kind"] == "data" and s.get("_profile")]
    ent_sets = {s["name"]: matched_entity_set(match_profile(s["_profile"], registry)) for s in data}

    parent = {s["name"]: s["name"] for s in data}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        parent[find(a)] = find(b)

    names = list(ent_sets)
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a, b = ent_sets[names[i]], ent_sets[names[j]]
            if not a or not b:
                continue
            jac = len(a & b) / len(a | b)
            if jac >= threshold:
                union(names[i], names[j])

    groups: dict[str, list[str]] = {}
    for nm in names:
        groups.setdefault(find(nm), []).append(nm)

    from profiling import snake
    out, used = [], {}
    for members in groups.values():
        shared = set.intersection(*[ent_sets[m] for m in members]) if members else set()
        # Name by dominant shape. A sales/transaction table needs an actual
        # measure (amount/quantity) tied to a customer/product — a bare date
        # column (e.g. a supplier's start/end date) must NOT read as "sales".
        # A sale has money (amount), or a quantity booked against an identified
        # account (customer_id). A product master has quantity-like attributes but
        # no amount and no customer_id → it stays "products".
        is_sale = ("amount" in shared) or ("quantity" in shared and "customer_id" in shared)
        if is_sale:
            table = "sales"
        elif "customer" in shared and "product" not in shared:
            table = "customers"
        elif "product" in shared:
            table = "products"
        elif "vendor" in shared:
            table = "vendors"
        else:
            table = snake(members[0])
        # guarantee uniqueness across groups
        if table in used:
            used[table] += 1
            table = f"{table}_{used[table]}"
        else:
            used[table] = 1
        out.append({"table": table, "members": members,
                    "sharedEntities": sorted(shared),
                    "union": len(members) > 1,
                    "reason": (f"share {len(shared)} canonical entities" if len(members) > 1
                               else "distinct schema")})
    return out


def confirm_mappings(confirmations: list[dict]) -> list[dict]:
    """Persist confirmed source->entity mappings as learned aliases.
    `confirmations` = [{"source": "CustName", "entity": "customer"}, ...].
    Unknown entity names create a new canonical entity. Returns updated registry."""
    registry = load_registry()
    by_name = {e["name"]: e for e in registry}
    for c in confirmations:
        src, ent = c.get("source"), c.get("entity")
        if not src or not ent:
            continue
        if ent not in by_name:
            new = {"name": ent, "role": c.get("role", "dimension"),
                   "type": c.get("type", "string"), "expect": c.get("expect", "text"),
                   "aliases": []}
            registry.append(new)
            by_name[ent] = new
        alias = _norm(src)
        existing = {_norm(a) for a in by_name[ent].get("aliases", [])}
        if alias and alias not in existing and alias != _norm(ent):
            by_name[ent].setdefault("aliases", []).append(src.strip().lower())
    save_registry(registry)
    return registry
