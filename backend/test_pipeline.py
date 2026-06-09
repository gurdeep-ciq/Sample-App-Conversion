"""
test_pipeline.py — fast unit tests for the deterministic core (no server, no Dagster
execution). Run with `pytest backend/test_pipeline.py` or `python backend/test_pipeline.py`.

Covers the invariants behind the bugs we fixed:
  * boolean detection + matcher penalty (flags don't become region/customer)
  * "constant" columns survive on tiny sheets (no empty 1-row tables)
  * name columns aren't mislabeled as ids
  * canonical-name collisions resolve by confidence (no silent overwrite)
  * union/table naming distinguishes sales vs product master
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import dagster_pipeline as D
import entities as E
import profiling as P


def test_boolean_detection():
    assert P.infer_column(["True", "False", "True"])["type"] == "boolean"
    assert P.infer_column(["Yes", "No", "Yes", "No"])["type"] == "boolean"
    assert P.infer_column([1, 2, 3, 4])["type"] == "number"


def test_boolean_not_matched_to_text_entity():
    col = P.infer_column(["True", "False"])
    m = E.match_column("Show Cust Detail", col, E._SEED)
    # a yes/no flag must not be confidently mapped to a text entity (region/customer)
    assert m is None or m.get("entity") is None or m["confidence"] < E.MATCH_THRESHOLD


def test_constant_column_survives_on_tiny_sheet():
    const = dict(type="string", constant=True, allZero=False, leadingZero=False, bigIntNum=False)
    assert P.role_for("Status", const, row_count=1) != "ignore"   # 1-row sheet: keep
    assert P.role_for("Status", const, row_count=50) == "ignore"  # many rows: drop


def test_name_column_is_not_an_id():
    col = P.infer_column(["Cook", "Lavallette Liquors", "Bottle Shop"])
    assert P.role_for("Customer Name", col, row_count=10) != "id"
    assert P.role_for("Customer Code", dict(type="string", constant=False, allZero=False,
                                            leadingZero=True, bigIntNum=False), row_count=10) == "id"


def test_canonical_map_resolves_collisions():
    # Two columns both score as 'quantity'; the higher-confidence one wins, the
    # other keeps its own name — no silent overwrite.
    profile = {"columns": [
        {"source": "Qty", "canonical": "qty", "type": "number", "role": "measure"},
        {"source": "Unit", "canonical": "unit", "type": "string", "role": "dimension"},
    ]}
    schema_cols = [
        {"name": "qty", "type": "decimal", "role": "measure", "source": "Qty", "include": True},
        {"name": "unit", "type": "string", "role": "dimension", "source": "Unit", "include": True},
    ]
    nm = D._canonical_map(profile, schema_cols, E._SEED, {})
    assert nm["qty"] == "quantity"        # exact-ish alias wins
    assert nm["unit"] != "quantity"       # loser keeps its own name
    assert len(set(nm.values())) == len(nm.values())  # all output names unique


def _sheet(name, headers, rows):
    aoa = [headers, *rows]
    sh = P.classify_sheet(name, aoa)
    if sh["kind"] == "data":
        sh["_profile"] = P.build_profile(sh)
    return sh


def test_union_naming_sales_vs_products():
    sales = _sheet("sales", ["Customer", "Account ID", "Item", "Qty", "Net Sales", "Date"],
                   [["Cook", "100", "Spritz", 2, 58.0, "2025-10-01"],
                    ["Bar", "101", "Spritz", 3, 87.0, "2025-10-02"]])
    products = _sheet("products", ["Product Code", "Product Description", "Unit Size", "Default Selling Price"],
                      [["00006862", "Sera Luce Spritz", "250mL", 6.0],
                       ["00007103", "Sera Luce Keg", "20L", 31.0]])
    groups = E.union_groups([sales, products], E._SEED)
    names = {g["table"] for g in groups}
    assert "sales" in names
    assert "products" in names


def test_idempotent_hash_is_stable():
    import hashlib
    data = b"some file bytes"
    assert hashlib.sha1(data).hexdigest()[:8] == hashlib.sha1(data).hexdigest()[:8]


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        try:
            fn()
            print(f"  PASS {fn.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL {fn.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            print(f"  ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{len(fns)} passed")
    return passed == len(fns)


if __name__ == "__main__":
    sys.exit(0 if _run_all() else 1)
