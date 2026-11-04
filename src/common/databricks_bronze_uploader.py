
"""
TrustFlow — NYC 311 Bronze uploader.

Uploads completed local historical batches into
a Databricks Volume.

- Automatically discovers completed dates
- Validates local page counts and record counts
- Verifies remote files using SHA-256
- Safely reuses matching uploaded files
- Publishes ingestion metadata LAST
- Never overwrites conflicting Bronze data
"""

import argparse
import hashlib
import json
import logging
from pathlib import Path

from databricks.sdk import WorkspaceClient


SOURCE = "nyc_311"

LOCAL_BRONZE = Path(
    "data/bronze/nyc_311/historical"
)

LOCAL_METADATA = Path(
    "data/metadata/nyc_311/historical"
)

VOLUME_ROOT = (
    "/Volumes/workspace/default/"
    "trustflow_bronze/nyc_311"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def sha256_file(path):
    """Calculate a local file's checksum."""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024), b""
        ):
            digest.update(chunk)

    return digest.hexdigest()


def sha256_remote(client, remote_path):
    """Verify the actual bytes stored in Databricks."""
    digest = hashlib.sha256()

    response = client.files.download(remote_path)

    with response.contents as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            digest.update(chunk)

    return digest.hexdigest()


def remote_files(client, directory):
    """Return existing remote file names."""
    return {
        Path(item.path).name
        for item in client.files.list_directory_contents(
            directory
        )
    }


def ensure_same_file(client, local_path, remote_path):
    """
    Upload a missing file, or verify an existing file.
    Never overwrite conflicting content.
    """

    parent = str(Path(remote_path).parent)

    client.files.create_directory(parent)

    existing_names = remote_files(client, parent)

    if local_path.name not in existing_names:
        client.files.upload_from(
            remote_path,
            str(local_path),
            overwrite=False
        )
        logger.info("Uploaded: %s", remote_path)

    else:
        logger.info(
            "Existing remote file: %s",
            remote_path
        )

    local_hash = sha256_file(local_path)
    remote_hash = sha256_remote(client, remote_path)

    if local_hash != remote_hash:
        raise ValueError(
            f"File integrity mismatch: {remote_path}"
        )

    logger.info("SHA-256 verified: %s", local_path.name)

    return local_hash


def validate_local_batch(day):
    """Check local ingestion before publishing."""

    metadata_path = LOCAL_METADATA / f"{day}.json"

    if not metadata_path.is_file():
        raise FileNotFoundError(metadata_path)

    metadata = json.loads(
        metadata_path.read_text(encoding="utf-8")
    )

    if (
        metadata.get("source") != SOURCE
        or metadata.get("date") != day
        or metadata.get("status") != "SUCCESS"
    ):
        raise ValueError(
            f"{day}: Ingestion metadata is not successful"
        )

    day_directory = LOCAL_BRONZE / day

    if not day_directory.is_dir():
        raise FileNotFoundError(day_directory)

    pages = sorted(day_directory.glob("page_*.json"))

    expected_pages = metadata["pages_downloaded"]
    expected_rows = metadata["records_downloaded"]

    if len(pages) != expected_pages:
        raise ValueError(
            f"{day}: Page count mismatch"
        )

    total_rows = 0

    for page in pages:
        with page.open(encoding="utf-8") as file:
            rows = json.load(file)

        if not isinstance(rows, list):
            raise ValueError(
                f"Invalid JSON page: {page}"
            )

        total_rows += len(rows)

    if total_rows != expected_rows:
        raise ValueError(
            f"{day}: Record count mismatch "
            f"expected={expected_rows}, "
            f"actual={total_rows}"
        )

    return metadata_path, metadata, pages


def upload_day(client, day):
    """Upload and verify one completed historical day."""

    metadata_path, metadata, pages = (
        validate_local_batch(day)
    )

    remote_day = (
        f"{VOLUME_ROOT}/historical/{day}"
    )

    remote_metadata = (
        f"{VOLUME_ROOT}/metadata/historical/"
        f"{day}.json"
    )

    client.files.create_directory(remote_day)

    verified = []

    # Step 1: Transfer and verify all original pages.
    for page in pages:
        remote_path = f"{remote_day}/{page.name}"

        checksum = ensure_same_file(
            client,
            page,
            remote_path
        )

        verified.append({
            "file": page.name,
            "sha256": checksum,
            "bytes": page.stat().st_size
        })

    # Step 2: Publish metadata only after pages pass.
    ensure_same_file(
        client,
        metadata_path,
        remote_metadata
    )

    # Step 3: Record the successful local upload.
    manifest = {
        "source": SOURCE,
        "date": day,
        "status": "VERIFIED",
        "records": metadata["records_downloaded"],
        "pages": len(pages),
        "remote_directory": remote_day,
        "files": verified
    }

    manifest_path = (
        Path("data/metadata/nyc_311/uploads")
        / f"{day}.json"
    )

    manifest_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary = manifest_path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8"
    )
    temporary.replace(manifest_path)

    logger.info(
        "VERIFIED %s | %s records | %s pages",
        day,
        metadata["records_downloaded"],
        len(pages)
    )


def upload_refresh_snapshots(client):
    """Upload completed NYC 311 refresh snapshots."""

    metadata_dir = Path("data/metadata/nyc_311/refresh")
    bronze_dir = Path("data/bronze/nyc_311/refresh")

    for metadata_path in sorted(metadata_dir.glob("*.json")):
        metadata = json.loads(
            metadata_path.read_text(encoding="utf-8")
        )

        if metadata.get("status") != "SUCCESS":
            continue

        if metadata.get("source") != "nyc_311":
            raise ValueError("Unexpected refresh source")

        day = metadata["date"]

        if metadata_path.stem != day:
            raise ValueError("Refresh metadata date mismatch")

        local_dir = bronze_dir / day
        pages = sorted(local_dir.glob("page_*.json"))

        if len(pages) != metadata["pages_downloaded"]:
            raise ValueError(
                f"{day}: Refresh page count mismatch"
            )

        total = 0

        for page in pages:
            with page.open(encoding="utf-8") as file:
                records = json.load(file)

            if not isinstance(records, list):
                raise ValueError(f"Invalid page: {page}")

            total += len(records)

        if total != metadata["records_downloaded"]:
            raise ValueError(
                f"{day}: Refresh record count mismatch"
            )

        remote_dir = f"{VOLUME_ROOT}/refresh/{day}"

        remote_metadata = (
            f"{VOLUME_ROOT}/metadata/refresh/{day}.json"
        )

        # Verify each file after uploading it.
        for page in pages:
            ensure_same_file(
                client,
                page,
                f"{remote_dir}/{page.name}"
            )

        # Publish metadata only after all pages pass.
        ensure_same_file(
            client,
            metadata_path,
            remote_metadata
        )

        logger.info(
            "REFRESH VERIFIED %s | %s records | %s pages",
            day,
            total,
            len(pages)
        )


def main():
    parser = argparse.ArgumentParser(
        description="TrustFlow automated Bronze uploader"
    )

    parser.add_argument(
        "--date",
        help="Optional date; processes all completed dates if omitted"
    )

    parser.add_argument(
        "--profile",
        default="DEFAULT"
    )

    args = parser.parse_args()

    client = WorkspaceClient(profile=args.profile)

    # Automatically discover completed historical batches.
    if args.date:
        days = [args.date]
    else:
        days = sorted(
            path.stem
            for path in LOCAL_METADATA.glob("*.json")
        )

    if not days:
        raise RuntimeError(
            "No NYC 311 historical metadata files found"
        )

    for day in days:
        metadata_file = LOCAL_METADATA / f"{day}.json"

        metadata = json.loads(
            metadata_file.read_text(encoding="utf-8")
        )

        if metadata.get("status") != "SUCCESS":
            logger.warning(
                "Skipping incomplete batch: %s",
                day
            )
            continue

        # Call the NEW uploader function.
        upload_day(client, day)

    # After historical uploads, process refresh snapshots
    upload_refresh_snapshots(client)


if __name__ == "__main__":
    main()
