# Pipeline Studio — backend

A small FastAPI service that turns a messy uploaded spreadsheet/CSV into a clean,
canonical, **Dagster-executed** ingestion pipeline. No LLM calls — pure heuristics
plus the **compact-dagster** in-process execution engine.

## What it does (the three goals)

1. **Compact Dagster runs the pipeline — full integration (not an LLM).**
   Each table is compiled into a partitioned Dagster `Definitions` (source assets →
   union table) and executed on **one process-lifetime `DagsterInstance`** shared
   across requests, so run / materialization / asset-check history accumulates and is
   queryable (`/runs`). A **file-backed IO manager resource** persists each cleaned
   table to `warehouse/<table>/<partition>.json`; **each upload is a partition**, so
   re-runs append rather than overwrite. Two registered **asset checks** run per table:
   `schema_vs_registry` and `schema_drift_vs_prev_upload` (the latter compares against
   prior uploads on disk — native cross-run "refine"). See `dagster_pipeline.py`.

   Compact-dagster ships no SQL/SQLite or webserver (the fork stripped them), so the
   instance uses its dependency-free dict-backed `lite_memory` storage; we deliberately
   do **not** re-add those stacks.

2. **Refine schema across formats for the same entities.**
   A canonical **entity registry** (`registry.json`, seeded from `entities.py`)
   maps differently-named source columns to one entity — `CustName`, `Customer Name`,
   and `Account` all resolve to `customer` — using fuzzy + alias matching with
   type-compat scoring. Matches are *suggested with confidence*; confirming one
   (`POST /registry/confirm`) persists it as a learned alias so future files
   auto-resolve. Each ingest reports a schema-change verdict (known vs new entities).

3. **All sheets handled; compatible ones unioned.**
   Every sheet is profiled (metadata/sidecar sheets are detected and skipped, not
   the data). Sheets whose canonical-entity sets overlap enough are **unioned** into
   one table; distinct sheets become their own tables. See `entities.union_groups`.

## Modules

| file | responsibility |
|------|----------------|
| `profiling.py` | parse file, classify ALL sheets, detect header row, infer column types, generate cleanup rules + per-sheet schema, and the shared pipeline executor |
| `entities.py` | canonical entity registry, fuzzy/alias column matching, union-group detection, learned-alias persistence |
| `dagster_pipeline.py` | build Dagster assets per table, conform columns to canonical names, union, `materialize()`, schema-change check |
| `app.py` | FastAPI endpoints |

## Endpoints

| method | path | purpose |
|--------|------|---------|
| GET  | `/health` | liveness + dagster version |
| GET  | `/registry` | current canonical entities |
| POST | `/registry/confirm` | persist confirmed `source -> entity` mappings |
| POST | `/profile` | upload a file → all-sheet profiles, proposed schema/rules, entity-match suggestions, union groups, `fileId` |
| POST | `/ingest` | run the Dagster pipeline(s) for chosen tables → cleaned rows, TableSchema, run id + partition, asset-check results, schema-change + cross-run drift, warehouse path |
| GET  | `/runs` | run history from the shared instance (run id, status, partition) |
| GET  | `/runs/{id}` | materializations + asset-check evaluations for a run |

## Run it

```powershell
# from the repo root
.\run-backend.ps1
```

This reuses **compact-dagster's** `.venv` (expected at `../../compact-dagster/.venv`
relative to the repo) and installs `requirements.txt` extras if missing. The server
listens on http://127.0.0.1:8000.

To run against a dedicated venv instead:

```bash
uv venv backend/.venv
uv pip install --python backend/.venv/Scripts/python.exe -e <path-to>/compact-dagster/python_modules/dagster
uv pip install --python backend/.venv/Scripts/python.exe -r backend/requirements.txt
backend/.venv/Scripts/python backend/app.py
```

## Smoke test

```bash
python backend/smoke_test.py "C:\path\to\file1.xlsx" "C:\path\to\file2.xlsx"
```

Profiles + ingests each file via the running server and prints a summary.
