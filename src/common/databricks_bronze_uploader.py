
import argparse
import hashlib
import json
import logging
from pathlib import Path

from databricks.sdk import WorkspaceClient


VOLUME_ROOT = "/Volumes/workspace/default/trustflow_bronze"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def upload_batch(source, date, local_directory, profile="DEFAULT"):
    local_directory = Path(local_directory)

    metadata_path = (
        Path("data/metadata")
        / source
        / "historical"
        / f"{date}.json"
    )

    if not metadata_path.is_file():
        raise FileNotFoundError("Batch metadata not found")

    metadata = json.loads(metadata_path.read_text())

    if metadata.get("status") != "SUCCESS":
        raise ValueError("Source ingestion is not complete")

    files = sorted(local_directory.glob("page_*.json"))

    if len(files) != metadata["pages_downloaded"]:
        raise ValueError("Local page count mismatch")

    if not files:
        raise ValueError("No Bronze files to upload")

    client = WorkspaceClient(profile=profile)

    destination = (
        f"{VOLUME_ROOT}/{source}/historical/{date}"
    )

    client.files.create_directory(destination)

    uploaded = []

    for local_file in files:
        remote_path = f"{destination}/{local_file.name}"
        checksum = sha256_file(local_file)

        # Do not silently overwrite an existing Bronze file.
        existing_paths = {
            item.path
            for item in client.files.list_directory_contents(
                destination
            )
        }

        if remote_path in existing_paths:
            raise FileExistsError(
                f"Remote file already exists: {remote_path}"
            )

        client.files.upload_from(
            remote_path,
            str(local_file),
            overwrite=False,
        )

        # Verify the uploaded file's reported byte size.
        remote_info = client.files.get_metadata(remote_path)

        remote_size = getattr(
            remote_info, "content_length", None
        )

        if remote_size is not None:
            if int(remote_size) != local_file.stat().st_size:
                raise ValueError(
                    f"Uploaded file size mismatch: {remote_path}"
                )

        uploaded.append(
            {
                "file": local_file.name,
                "remote_path": remote_path,
                "local_sha256": checksum,
                "bytes": local_file.stat().st_size,
            }
        )

        logger.info("Uploaded: %s", local_file.name)

    manifest = {
        "source": source,
        "date": date,
        "destination": destination,
        "status": "UPLOADED",
        "files_uploaded": len(uploaded),
        "records_downloaded": metadata["records_downloaded"],
        "files": uploaded,
    }

    manifest_path = (
        Path("data/metadata")
        / source
        / "uploads"
        / f"{date}.json"
    )

    manifest_path.parent.mkdir(
        parents=True, exist_ok=True
    )

    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Upload completed: %s files",
        len(uploaded),
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--source", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--local-directory", required=True)
    parser.add_argument("--profile", default="DEFAULT")

    args = parser.parse_args()

    upload_batch(
        source=args.source,
        date=args.date,
        local_directory=args.local_directory,
        profile=args.profile,
    )


if __name__ == "__main__":
    main()
