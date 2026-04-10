"""
main.py — Orchestrator for one full scrape-and-diff cycle.

Flow:
  1. Load targets from targets.json
  2. Ensure every target has a matching row in the companies table
  3. Scrape each pricing page
  4. Save a new snapshot to Supabase
  5. Compare with the previous snapshot
  6. If a meaningful change is found, save it to the changes table
  7. Print a run summary
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

import db
from differ import compute_diff
from scraper import scrape_all

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

load_dotenv()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("main")

# Path to targets file — always relative to this script's location
TARGETS_FILE = Path(__file__).parent / "targets.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_targets() -> list[dict]:
    """Read targets.json and return the list of company dicts."""
    with TARGETS_FILE.open() as fh:
        targets = json.load(fh)
    logger.info("Loaded %d targets from %s", len(targets), TARGETS_FILE)
    return targets


def sync_companies(targets: list[dict]) -> dict[str, str]:
    """
    Ensure every target has a row in the companies table.
    Returns a mapping of pricing_url → company_id.
    """
    url_to_id: dict[str, str] = {}
    for target in targets:
        row = db.upsert_company(
            name=target["name"],
            category=target["category"],
            pricing_url=target["url"],
        )
        url_to_id[target["url"]] = row["id"]
    return url_to_id


# ---------------------------------------------------------------------------
# Core run logic
# ---------------------------------------------------------------------------

async def run_once() -> None:
    """Execute one full scrape cycle and return summary stats."""
    run_start = datetime.now(timezone.utc)
    logger.info("=== Run started at %s ===", run_start.strftime("%Y-%m-%d %H:%M:%S UTC"))

    targets = load_targets()
    url_to_id = sync_companies(targets)

    # Scrape all pages (returns url → text | None)
    scraped: dict[str, str | None] = await scrape_all(targets)

    pages_scraped = 0
    pages_failed = 0
    changes_detected = 0

    for target in targets:
        url = target["url"]
        company_id = url_to_id[url]
        company_name = target["name"]
        content = scraped.get(url)

        # --- Scrape outcome ---
        if content is None:
            logger.warning("[%s] Scrape failed or blocked — skipping snapshot", company_name)
            pages_failed += 1
            continue

        pages_scraped += 1

        # --- Save new snapshot ---
        new_snap = db.save_snapshot(company_id, content)
        new_snap_id = new_snap.get("id")
        logger.info("[%s] Snapshot saved (%d chars)", company_name, len(content))

        # --- Diff against previous snapshot ---
        two_snaps = db.get_latest_two_snapshots(company_id)

        if len(two_snaps) < 2:
            logger.info("[%s] First snapshot — nothing to compare yet", company_name)
            continue

        # Ordered newest-first by the query; index 0 = new, index 1 = previous
        prev_snap = two_snaps[1]
        prev_snap_id = prev_snap.get("id")
        prev_content = prev_snap.get("content", "")

        diff_summary = compute_diff(prev_content, content)

        if diff_summary is None:
            logger.info("[%s] No meaningful change detected", company_name)
        else:
            changes_detected += 1
            db.save_change(company_id, diff_summary, prev_snap_id, new_snap_id)
            logger.info("[%s] CHANGE DETECTED — saved to changes table", company_name)
            print(
                f"\n{'='*60}\n"
                f"CHANGE DETECTED: {company_name}\n"
                f"URL: {url}\n"
                f"{'-'*60}\n"
                f"{diff_summary[:800]}"
                f"\n{'='*60}\n"
            )

    # ---------------------------------------------------------------------------
    # Run summary
    # ---------------------------------------------------------------------------
    run_end = datetime.now(timezone.utc)
    duration = (run_end - run_start).total_seconds()

    print(
        f"\n{'*'*60}\n"
        f"Run complete at {run_end.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"Duration      : {duration:.1f}s\n"
        f"Pages scraped : {pages_scraped}/{len(targets)}\n"
        f"Pages failed  : {pages_failed}\n"
        f"Changes found : {changes_detected}\n"
        f"{'*'*60}\n"
    )
    logger.info(
        "Summary — scraped: %d, failed: %d, changes: %d, duration: %.1fs",
        pages_scraped,
        pages_failed,
        changes_detected,
        duration,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(run_once())
