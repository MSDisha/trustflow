
"""
TrustFlow — NYC 311 Refresh Ingestion

Purpose:
Re-read recently created NYC 311 requests to capture
their latest observed state.

This script:
- Automatically selects a rolling date window
- Downloads current API observations
- Uses safe keyset pagination
- Validates record counts and duplicate IDs
- Preserves immutable Bronze snapshots
- Records ingestion metadata
- Skips already completed daily snapshots

It does NOT update Silver tables directly.
"""

import argparse
import json
import logging

from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from nyc_311 import (
    create_session,
    api_get,
    count_records,
    save_json,
    now,
)

SOURCE = "nyc_311"
PAGE_SIZE = 1000

BRONZE_DIR = Path("data/bronze/nyc_311/refresh")
METADATA_DIR = Path("data/metadata/nyc_311/refresh")

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_window(lookback_days):
    """Automatically determine the refresh window."""

    if lookback_days < 1:
        raise ValueError("lookback_days must be positive")

    today_nyc = datetime.now(
        ZoneInfo("America/New_York")
    ).date()

    start = today_nyc - timedelta(days=lookback_days)

    return start, today_nyc


def make_filter(start, end):
    """Select requests by their original creation date."""

    return (
        f"created_date >= '{start.isoformat()}T00:00:00' "
        f"AND created_date < '{end.isoformat()}T00:00:00'"
    )


def refresh_requests(session, lookback_days):
    """Create one immutable refresh snapshot per NYC day."""

    start, end = get_window(lookback_days)

    # The snapshot is identified by the execution date.
    snapshot_date = end.isoformat()

    final_dir = BRONZE_DIR / snapshot_date
    temp_dir = BRONZE_DIR / f"{snapshot_date}.incomplete"

    metadata_path = (
        METADATA_DIR / f"{snapshot_date}.json"
    )

    if final_dir.exists() and metadata_path.exists():
        previous = json.loads(
            metadata_path.read_text(encoding="utf-8")
        )

        if previous.get("status") == "SUCCESS":
            logger.info(
                "Refresh snapshot already completed: %s",
                snapshot_date
            )
            return

    if final_dir.exists() or temp_dir.exists():
        raise RuntimeError(
            "Unfinished or conflicting refresh files exist. "
            "Inspect the batch before retrying."
        )

    temp_dir.mkdir(parents=True, exist_ok=False)

    where = make_filter(start, end)

    metadata = {
        "source": SOURCE,
        "mode": "refresh",
        "date": snapshot_date,
        "status": "RUNNING",
        "window_start": start.isoformat(),
        "window_end_exclusive": end.isoformat(),
        "started_at": now(),
        "observed_at": None,
        "finished_at": None,
        "expected_records": None,
        "records_downloaded": 0,
        "pages_downloaded": 0,
        "error": None,
    }

    save_json(metadata_path, metadata)

    try:
        logger.info(
            "Refreshing requests created from %s to %s",
            start,
            end
        )

        expected = count_records(session, where)
        metadata["expected_records"] = expected

        if expected == 0:
            metadata["status"] = "PENDING_SOURCE"
            logger.warning("No records currently available")
            temp_dir.rmdir()
            return

        # The snapshot observation time is processing
        # metadata, not a source last-modified timestamp.
        metadata["observed_at"] = now()

        last_date = None
        last_key = None

        seen_ids = set()
        page = 0

        while True:
            cursor_where = where

            if last_date is not None:
                safe_date = last_date.replace("'", "''")
                safe_key = last_key.replace("'", "''")

                cursor_where += (
                    f" AND (created_date > '{safe_date}' "
                    f"OR (created_date = '{safe_date}' "
                    f"AND unique_key > '{safe_key}'))"
                )

            records = api_get(
                session,
                {
                    "$where": cursor_where,
                    "$order": (
                        "created_date ASC, unique_key ASC"
                    ),
                    "$limit": PAGE_SIZE,
                },
            )

            if not records:
                break

            for record in records:
                key = record.get("unique_key")
                created = record.get("created_date")

                if not key or not created:
                    raise ValueError(
                        "Missing required pagination fields"
                    )

                if key in seen_ids:
                    raise ValueError(
                        f"Duplicate request ID: {key}"
                    )

                seen_ids.add(key)

            new_cursor = (
                records[-1]["created_date"],
                records[-1]["unique_key"],
            )

            if last_date is not None:
                if new_cursor <= (last_date, last_key):
                    raise ValueError(
                        "Pagination cursor did not advance"
                    )

            last_date, last_key = new_cursor

            page += 1

            filename = f"page_{page:06d}.json"

            save_json(
                temp_dir / filename,
                records,
            )

            metadata["records_downloaded"] += len(records)
            metadata["pages_downloaded"] = page

            save_json(metadata_path, metadata)

            logger.info(
                "Refresh %s | Page %s | Total %s",
                snapshot_date,
                page,
                metadata["records_downloaded"],
            )

            if len(records) < PAGE_SIZE:
                break

        expected_after = count_records(session, where)

        if (
            metadata["records_downloaded"] != expected
            or expected != expected_after
        ):
            raise ValueError(
                "Refresh count reconciliation failed: "
                f"before={expected}, "
                f"downloaded={metadata['records_downloaded']}, "
                f"after={expected_after}"
            )

        # Publish only after validation passes.
        temp_dir.rename(final_dir)

        metadata["status"] = "SUCCESS"

        logger.info(
            "Refresh complete: %s | Records: %s",
            snapshot_date,
            metadata["records_downloaded"],
        )

    except Exception as error:
        metadata["status"] = "FAILED"
        metadata["error"] = str(error)

        logger.exception("NYC 311 refresh failed")
        raise

    finally:
        metadata["finished_at"] = now()
        save_json(metadata_path, metadata)


def main():
    parser = argparse.ArgumentParser(
        description="TrustFlow NYC 311 refresh ingestion"
    )

    parser.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help="Creation-date window to refresh (default: 14)"
    )

    args = parser.parse_args()

    with create_session() as session:
        refresh_requests(
            session,
            args.lookback_days
        )


if __name__ == "__main__":
    main()
