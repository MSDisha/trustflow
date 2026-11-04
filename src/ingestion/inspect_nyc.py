
import json
from pathlib import Path

BRONZE_DIR = Path("data/bronze/nyc_311/historical")
METADATA_DIR = Path("data/metadata/nyc_311/historical")

# Find only completed day directories.
completed_days = []

for day_dir in BRONZE_DIR.iterdir():
    if not day_dir.is_dir():
        continue

    metadata_file = METADATA_DIR / f"{day_dir.name}.json"

    if not metadata_file.exists():
        continue

    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))

    if metadata.get("status") == "SUCCESS":
        completed_days.append(day_dir)

if not completed_days:
    raise FileNotFoundError("No completed NYC 311 Bronze batches found.")

# Choose the latest completed day.
latest_day = sorted(completed_days)[-1]
files = sorted(latest_day.glob("page_*.json"))

records = []

for file_path in files:
    with file_path.open("r", encoding="utf-8") as file:
        page_records = json.load(file)
        records.extend(page_records)

print("Completed day:", latest_day.name)
print("Page files:", len(files))
print("Total records:", len(records))

if records:
    ids = [row.get("unique_key") for row in records]

    print("Available columns:", list(records[0].keys()))
    print("Missing IDs:", sum(not key for key in ids))
    print("Duplicate IDs:", len(ids) - len(set(ids)))

    print("\nSample record:")
    print(json.dumps(records[0], indent=2))

    print("\nTimestamp inspection:")
    for record in records[:5]:
        print({
            "unique_key": record.get("unique_key"),
            "created_date": record.get("created_date"),
            "resolution_action_updated_date":
                record.get("resolution_action_updated_date"),
            "closed_date": record.get("closed_date"),
            "status": record.get("status")
        })
