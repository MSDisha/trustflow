
import argparse
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

BRONZE_DIR = Path("data/bronze/nyc_311/historical")
METADATA_DIR = Path("data/metadata/nyc_311/historical")

PAGE_SIZE = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def now():
    return datetime.now(timezone.utc).isoformat()


def create_session():
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    session = requests.Session()
    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry),
    )
    return session


def api_get(session, params):
    response = session.get(
        API_URL,
        params=params,
        timeout=60,
    )
    response.raise_for_status()

    result = response.json()

    if not isinstance(result, list):
        raise ValueError("Expected a JSON list from NYC API")

    return result


def day_filter(day):
    next_day = day + timedelta(days=1)

    return (
        f"created_date >= '{day.isoformat()}T00:00:00' "
        f"AND created_date < '{next_day.isoformat()}T00:00:00'"
    )


def count_records(session, where):
    rows = api_get(
        session,
        {
            "$select": "count(*) AS n",
            "$where": where,
        },
    )

    return int(rows[0]["n"])


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = path.with_suffix(path.suffix + ".tmp")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

    temporary_path.replace(path)


def ingest_day(session, day):
    day_name = day.isoformat()

    final_dir = BRONZE_DIR / day_name
    temp_dir = BRONZE_DIR / f"{day_name}.incomplete"

    metadata_path = METADATA_DIR / f"{day_name}.json"

    # Skip days already completed successfully.
    if final_dir.is_dir() and metadata_path.is_file():
        previous = json.loads(
            metadata_path.read_text(encoding="utf-8")
        )

        if previous.get("status") == "SUCCESS":
            logger.info(
                "Skipping completed day: %s",
                day_name,
            )
            return

    # Do not overwrite partial or untracked files.
    if final_dir.exists() or temp_dir.exists():
        raise RuntimeError(
            f"Unfinished or untracked files found for {day_name}. "
            "Inspect them before retrying."
        )

    temp_dir.mkdir(parents=True, exist_ok=False)

    where = day_filter(day)

    metadata = {
        "source": "nyc_311",
        "date": day_name,
        "status": "RUNNING",
        "started_at": now(),
        "finished_at": None,
        "expected_records": None,
        "records_downloaded": 0,
        "pages_downloaded": 0,
        "error": None,
    }

    save_json(metadata_path, metadata)

    try:
        expected = count_records(session, where)

        metadata["expected_records"] = expected

        last_date = None
        last_key = None

        seen_ids = set()
        page = 0

        while True:
            cursor_where = where

            # Continue after the last record, not an offset.
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
                    "$order": "created_date ASC, unique_key ASC",
                    "$limit": PAGE_SIZE,
                },
            )

            if not records:
                break

            # Validate pagination keys and duplicate IDs.
            for record in records:
                key = record.get("unique_key")
                created = record.get("created_date")

                if not key or not created:
                    raise ValueError(
                        "Missing unique_key or created_date"
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
                "%s | Page %s | Downloaded %s | Total %s",
                day_name,
                page,
                len(records),
                metadata["records_downloaded"],
            )

            if len(records) < PAGE_SIZE:
                break

        # Compare counts before and after extraction.
        expected_after = count_records(session, where)

        if (
            metadata["records_downloaded"] != expected
            or expected != expected_after
        ):
            raise ValueError(
                "Count mismatch: "
                f"downloaded={metadata['records_downloaded']}, "
                f"API_before={expected}, "
                f"API_after={expected_after}"
            )

        # Publish complete Bronze data only after checks.
        temp_dir.rename(final_dir)

        metadata["status"] = "SUCCESS"

        logger.info(
            "Completed %s: %s records",
            day_name,
            metadata["records_downloaded"],
        )

    except Exception as error:
        metadata["status"] = "FAILED"
        metadata["error"] = str(error)

        logger.exception(
            "Failed %s. Partial files retained.",
            day_name,
        )

        raise

    finally:
        metadata["finished_at"] = now()
        save_json(metadata_path, metadata)


def main():
    parser = argparse.ArgumentParser(
        description="NYC 311 historical Bronze ingestion"
    )

    parser.add_argument(
        "--start-date",
        required=True,
        type=date.fromisoformat,
    )

    parser.add_argument(
        "--end-date",
        required=True,
        type=date.fromisoformat,
        help="Exclusive end date",
    )

    args = parser.parse_args()

    if args.start_date >= args.end_date:
        parser.error(
            "--end-date must be later than --start-date"
        )

    with create_session() as session:
        day = args.start_date

        while day < args.end_date:
            ingest_day(session, day)
            day += timedelta(days=1)


if __name__ == "__main__":
    main()
