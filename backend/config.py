"""
config.py — secrets + runtime configuration.

Loads from backend/.env (gitignored, never committed). Two secrets:
  ANTHROPIC_API_KEY   real LLM for schema introspection / fact-dim modeling / code-gen
  DATABASE_URL        Postgres/Supabase connection string for database awareness

Nothing here is logged or echoed. `status()` reports only whether each is *present*.
"""
from __future__ import annotations

import os

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except Exception:  # python-dotenv optional; env vars may be set externally
    pass

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
# default model; override with ANTHROPIC_MODEL in .env
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6").strip()


def has_llm() -> bool:
    return bool(ANTHROPIC_API_KEY)


def has_db() -> bool:
    return bool(DATABASE_URL)


def _mask(s: str) -> str:
    if not s:
        return "(unset)"
    return f"set ({len(s)} chars, …{s[-4:]})"


def status() -> dict:
    """Presence-only status — never returns the secret values."""
    return {
        "llm": has_llm(),
        "model": ANTHROPIC_MODEL if has_llm() else None,
        "db": has_db(),
        "anthropic_key": _mask(ANTHROPIC_API_KEY),
        "database_url": "set" if DATABASE_URL else "(unset)",
    }
