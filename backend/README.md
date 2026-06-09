# Pipeline Studio — backend

A small FastAPI service that turns a messy uploaded spreadsheet/CSV into a clean,
canonical, **Dagster-executed** ingestion pipeline. No LLM calls — pure heuristics
plus the **compact-dagster** in-process execution engine.

## What it does (the three goals)

1. **Compact Dagster generates & runs the pipeline (not an LLM).**
   Each target table is compiled into real Dagster assets and executed in-process
   via `materialize()`. The run emits a `TableSchema` and an asset check; the API
   returns the actual Dagster run events. See `dagster_pipeline.py`.

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
| POST | `/ingest` | run the Dagster pipeline(s) for chosen tables → cleaned rows, TableSchema, run events, schema-change verdict |

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
