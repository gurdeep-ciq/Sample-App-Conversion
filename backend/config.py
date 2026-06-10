"""
config.py — central environment configuration (secrets + cloud).

Loaded from backend/.env (gitignored, never committed). Safe local fallbacks so the
app runs and is verifiable without any cloud credentials:

  ANTHROPIC_API_KEY  real LLM (Claude) for schema introspection / fact-dim modeling / code-gen
  ANTHROPIC_MODEL    model id (default Haiku)
  DATABASE_URL       Supabase/Postgres URL; unset -> local SQLite file (FKs enabled)
  S3_BUCKET          S3 bucket for uploaded files (+ AWS_* / AWS_REGION); unset -> local ./_filestore

Set the env vars to point at real Supabase + S3 — no code change. Nothing here is
logged or echoed; status() reports presence only.
"""
from __future__ import annotations

import os

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except Exception:  # pragma: no cover
    pass

_HERE = os.path.dirname(__file__)

# --- LLM ------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001").strip()

# --- database -------------------------------------------------------------
# Supabase/Postgres via DATABASE_URL; else a local SQLite file so the relational
# schema + FKs are real and verifiable here.
DATABASE_URL = os.environ.get("DATABASE_URL") or f"sqlite:///{os.path.join(_HERE, 'warehouse.db')}"

# --- file storage ---------------------------------------------------------
S3_BUCKET = os.environ.get("S3_BUCKET")           # unset -> local fallback
S3_PREFIX = os.environ.get("S3_PREFIX", "uploads")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
LOCAL_FILESTORE = os.environ.get("LOCAL_FILESTORE", os.path.join(_HERE, "_filestore"))


def has_llm() -> bool:
    return bool(ANTHROPIC_API_KEY)


def storage_backend() -> str:
    return "s3" if S3_BUCKET else "local"


def db_backend() -> str:
    return "postgres" if DATABASE_URL.startswith(("postgres", "postgresql")) else \
           ("sqlite" if DATABASE_URL.startswith("sqlite") else "other")


def has_db() -> bool:
    """True when pointed at a real (cloud) database rather than the local SQLite fallback."""
    return db_backend() == "postgres"


def db_label() -> str:
    return {"postgres": "Supabase/Postgres", "sqlite": "local SQLite"}.get(
        db_backend(), DATABASE_URL.split(":", 1)[0])


def _mask(s: str) -> str:
    return f"set (…{s[-4:]})" if s else "(unset)"


def status() -> dict:
    """Presence-only status — never returns secret values."""
    return {"llm": has_llm(), "model": ANTHROPIC_MODEL if has_llm() else None,
            "db": db_backend(), "db_label": db_label(), "storage": storage_backend(),
            "anthropic_key": _mask(ANTHROPIC_API_KEY)}
