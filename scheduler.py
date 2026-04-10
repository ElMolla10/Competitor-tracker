"""
scheduler.py — APScheduler wrapper for automated daily runs.

Schedule: every day at 03:00 Cairo time (Africa/Cairo = UTC+2/UTC+3 DST).
Also exposes `trigger_now()` for ad-hoc / testing runs.
"""

import asyncio
import logging
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# Inline import to avoid circular issues — main sets up logging at module level
def _run_job() -> None:
    """Synchronous wrapper called by APScheduler."""
    from main import run_once  # noqa: PLC0415
    asyncio.run(run_once())


def trigger_now() -> None:
    """Manually trigger one scrape cycle — useful during development."""
    logger.info("Manual trigger invoked")
    _run_job()


def start_scheduler() -> None:
    """
    Start the blocking scheduler.
    This function does not return until the process is killed or interrupted.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    scheduler = BlockingScheduler(timezone="Africa/Cairo")

    scheduler.add_job(
        _run_job,
        trigger=CronTrigger(hour=3, minute=0, timezone="Africa/Cairo"),
        id="daily_scrape",
        name="Daily pricing page scrape",
        misfire_grace_time=3600,   # allow up to 1 hour late start
        coalesce=True,             # skip missed runs if multiple stack up
    )

    logger.info(
        "Scheduler started — next run at 03:00 Cairo time. "
        "Press Ctrl+C to stop."
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Competitor price tracker scheduler")
    parser.add_argument(
        "--now",
        action="store_true",
        help="Run one scrape cycle immediately, then exit",
    )
    args = parser.parse_args()

    if args.now:
        trigger_now()
    else:
        start_scheduler()
