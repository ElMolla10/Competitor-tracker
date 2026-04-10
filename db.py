"""
db.py — Supabase client and all database operations.
All functions are async-compatible (supabase-py uses sync I/O internally,
so they are wrapped to keep the calling interface clean).
"""

import os
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Client initialisation
# ---------------------------------------------------------------------------

def _get_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise EnvironmentError(
            "SUPABASE_URL and SUPABASE_KEY must be set in the environment / .env file."
        )
    return create_client(url, key)


# Module-level singleton — created once, reused for the lifetime of the process.
_client: Client | None = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = _get_client()
    return _client


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_all_companies() -> list[dict]:
    """Return every row from the companies table."""
    response = get_client().table("companies").select("*").execute()
    return response.data or []


def save_snapshot(company_id: str, content: str) -> dict:
    """Insert a new snapshot row and return the created record."""
    payload = {
        "company_id": company_id,
        "content": content,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }
    response = get_client().table("snapshots").insert(payload).execute()
    row = response.data[0] if response.data else {}
    logger.debug("Saved snapshot %s for company %s", row.get("id"), company_id)
    return row


def get_latest_two_snapshots(company_id: str) -> list[dict]:
    """
    Return the two most-recent snapshots for a company, newest first.
    Returns an empty list if none exist, or a one-item list if only one exists.
    """
    response = (
        get_client()
        .table("snapshots")
        .select("*")
        .eq("company_id", company_id)
        .order("scraped_at", desc=True)
        .limit(2)
        .execute()
    )
    return response.data or []


def save_change(
    company_id: str,
    summary: str,
    prev_snapshot_id: str,
    new_snapshot_id: str,
) -> dict:
    """Insert a change record and return the created row."""
    payload = {
        "company_id": company_id,
        "change_summary": summary,
        "previous_snapshot_id": prev_snapshot_id,
        "new_snapshot_id": new_snapshot_id,
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }
    response = get_client().table("changes").insert(payload).execute()
    row = response.data[0] if response.data else {}
    logger.debug("Saved change %s for company %s", row.get("id"), company_id)
    return row


def upsert_company(name: str, category: str, pricing_url: str) -> dict:
    """
    Insert a company if it doesn't exist yet (matched by pricing_url).
    Returns the existing or newly-created row.
    """
    # Check for existing entry first to avoid duplicates
    existing = (
        get_client()
        .table("companies")
        .select("*")
        .eq("pricing_url", pricing_url)
        .execute()
    )
    if existing.data:
        return existing.data[0]

    payload = {
        "name": name,
        "category": category,
        "pricing_url": pricing_url,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    response = get_client().table("companies").insert(payload).execute()
    row = response.data[0] if response.data else {}
    logger.info("Registered new company: %s", name)
    return row
