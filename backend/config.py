"""
config.py — central environment configuration.

Everything that ties the app to the cloud is read from env here, with safe local
fallbacks so the app runs and is verifiable without any cloud credentials:

  DATABASE_URL   Postgres/Supabase URL (postgresql+psycopg://user:pass@host/db).
                 Unset -> local SQLite file (FKs enabled), so tables/FKs still work.
  S3_BUCKET      S3 bucket for uploaded files (+ AWS_* creds / AWS_REGION).
                 Unset -> local ./_filestore directory (same put/get interface).

Set the env vars (e.g. in a .env file) to point at real Supabase + S3 — no code change.
"""
from __future__ import annotations

import os

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except Exception:  # pragma: no cover
    pass

_HERE = os.path.dirname(__file__)

# --- database -------------------------------------------------------------
# Default to a local SQLite file so the relational schema + FKs are real and
# verifiable here; switch to Supabase by exporting DATABASE_URL.
DATABASE_URL = os.environ.get("DATABASE_URL") or f"sqlite:///{os.path.join(_HERE, 'warehouse.db')}"

# --- file storage ---------------------------------------------------------
S3_BUCKET = os.environ.get("S3_BUCKET")           # unset -> local fallback
S3_PREFIX = os.environ.get("S3_PREFIX", "uploads")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
LOCAL_FILESTORE = os.environ.get("LOCAL_FILESTORE", os.path.join(_HERE, "_filestore"))


def storage_backend() -> str:
    return "s3" if S3_BUCKET else "local"


def db_backend() -> str:
    return "postgres" if DATABASE_URL.startswith(("postgres", "postgresql")) else \
           ("sqlite" if DATABASE_URL.startswith("sqlite") else "other")
