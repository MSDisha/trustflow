
import json
from pathlib import Path

# Find downloaded NYC 311 JSON files
bronze_path = Path("data/bronze/nyc_311")
files = sorted(bronze_path.glob("*.json"))

if not files:
    raise FileNotFoundError("No NYC 311 JSON files found.")

# Select the latest file
latest_file = files[-1]

# Read JSON records
with open(latest_file, "r", encoding="utf-8") as file:
    records = json.load(file)

print("File:", latest_file.name)
print("Total records:", len(records))

if records:
    print("\nAvailable columns:")
    for column in records[0].keys():
        print("-", column)

    # Check duplicate request IDs in this batch
    ids = [row.get("unique_key") for row in records]
    non_null_ids = [id_value for id_value in ids if id_value is not None]

    print("\nMissing unique keys:", len(ids) - len(non_null_ids))
    print("Duplicate unique keys:", len(non_null_ids) - len(set(non_null_ids)))

    print("\nSample record:")
    print(json.dumps(records[0], indent=2))
    
    # Inspect timestamps for incremental ingestion
    print("\n--- Timestamp Inspection ---")

    for record in records[:5]:
        print({
            "unique_key": record.get("unique_key"),
            "created_date": record.get("created_date"),
            "resolution_action_updated_date": record.get(
                "resolution_action_updated_date"
            ),
            "closed_date": record.get("closed_date"),
            "status": record.get("status")
        })
