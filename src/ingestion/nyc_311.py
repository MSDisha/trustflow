
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

BRONZE_DIR = Path("data/bronze/nyc_311")
METADATA_DIR = Path("data/metadata/nyc_311")

PAGE_SIZE = 100
MAX_PAGES = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def current_time():
    return datetime.now(timezone.utc).isoformat()

def create_session():
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )

    session = requests.Session()

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("https://", adapter)

    return session


def fetch_data():
    all_records = []

    with create_session() as session:
        for page in range(MAX_PAGES):
            offset = page * PAGE_SIZE

            params = {
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$order": "created_date DESC, unique_key DESC",
            }

            response = session.get(
                API_URL,
                params=params,
                timeout=30,
            )

            response.raise_for_status()

            records = response.json()
            all_records.extend(records)

            logger.info(
                "Page %s downloaded: %s records",
                page + 1,
                len(records),
            )

            if len(records) < PAGE_SIZE:
                break

    return all_records


def save_json(data, folder, filename):
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / filename

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    return file_path


def run_ingestion():
    run_id = str(uuid4())
    started_at = current_time()

    metadata = {
        "run_id": run_id,
        "source": "nyc_311",
        "started_at": started_at,
        "finished_at": None,
        "status": "RUNNING",
        "records_downloaded": 0,
        "pages_downloaded": 0,
        "raw_file": None,
        "error": None,
    }

    try:
        logger.info("Starting NYC 311 ingestion")

        records = fetch_data()

        timestamp = datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )

        filename = f"nyc_311_{timestamp}_{run_id[:8]}.json"

        raw_file = save_json(
            records,
            BRONZE_DIR,
            filename,
        )

        metadata["status"] = "SUCCESS"
        metadata["records_downloaded"] = len(records)
        metadata["pages_downloaded"] = (
            (len(records) + PAGE_SIZE - 1) // PAGE_SIZE
        )
        metadata["raw_file"] = str(raw_file)

        logger.info("Successfully downloaded %s records", len(records))

    except Exception as error:
        metadata["status"] = "FAILED"
        metadata["error"] = str(error)

        logger.exception("Ingestion failed")

        raise

    finally:
        metadata["finished_at"] = current_time()

        save_json(
            metadata,
            METADATA_DIR,
            f"{run_id}.json",
        )

        logger.info("Run metadata saved")


if __name__ == "__main__":
    run_ingestion()
