# Databricks notebook source


bronze_path = (
    "/Volumes/workspace/default/trustflow_bronze/"
    "nyc_311/historical/2026-10-26/page_*.json"
)

bronze_df = (
    spark.read
    .option("multiLine", "true")
    .json(bronze_path)
)

print("Bronze records:", bronze_df.count())
print("Total columns:", len(bronze_df.columns))

display(bronze_df.limit(5))

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Preserve all source columns
silver_clean_df = bronze_df

# Remove extra spaces from text columns
for field in silver_clean_df.schema.fields:
    if isinstance(field.dataType, StringType):
        silver_clean_df = silver_clean_df.withColumn(
            field.name,
            F.trim(F.col(field.name))
        )

# Convert date strings into timestamps
date_columns = [
    "created_date",
    "closed_date",
    "resolution_action_updated_date",
    "due_date"
]

for col_name in date_columns:
    if col_name in silver_clean_df.columns:
        silver_clean_df = silver_clean_df.withColumn(
            col_name,
            F.expr(f"try_cast(`{col_name}` AS TIMESTAMP)")
        )

print("Bronze records:", bronze_df.count())
print("Cleaned records:", silver_clean_df.count())
print("Total columns:", len(silver_clean_df.columns))


# COMMAND ----------


missing_ids = silver_clean_df.filter(
    F.col("unique_key").isNull() |
    (F.col("unique_key") == "")
).count()

duplicate_groups = (
    silver_clean_df
    .groupBy("unique_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_dates = silver_clean_df.filter(
    F.col("created_date").isNull()
).count()

invalid_date_order = silver_clean_df.filter(
    F.col("closed_date").isNotNull() &
    F.col("created_date").isNotNull() &
    (F.col("closed_date") < F.col("created_date"))
).count()

print("Total records:", silver_clean_df.count())
print("Missing request IDs:", missing_ids)
print("Duplicate ID groups:", duplicate_groups)
print("Missing creation dates:", missing_dates)
print("Closed before created:", invalid_date_order)


# COMMAND ----------

from pyspark.sql import functions as F

# Identify invalid records
invalid_condition = (
    F.col("unique_key").isNull()
    | (F.trim(F.col("unique_key")) == "")
    | F.col("created_date").isNull()
    | (
        F.col("closed_date").isNotNull()
        & (F.col("closed_date") < F.col("created_date"))
    )
)

# Separate valid records
valid_df = silver_clean_df.filter(~invalid_condition)

# Separate invalid records and explain the issue
quarantine_df = (
    silver_clean_df
    .filter(invalid_condition)
    .withColumn(
        "failure_reason",
        F.concat_ws(
            ", ",
            F.when(
                F.col("unique_key").isNull()
                | (F.trim(F.col("unique_key")) == ""),
                "MISSING_REQUEST_ID"
            ),
            F.when(
                F.col("created_date").isNull(),
                "MISSING_CREATION_DATE"
            ),
            F.when(
                F.col("closed_date").isNotNull()
                & (F.col("closed_date") < F.col("created_date")),
                "INVALID_DATE_ORDER"
            )
        )
    )
)

print("Bronze records:", silver_clean_df.count())
print("Valid records:", valid_df.count())
print("Quarantined records:", quarantine_df.count())

# COMMAND ----------

bronze_count = silver_clean_df.count()
valid_count = valid_df.count()
quarantine_count = quarantine_df.count()

assert bronze_count == valid_count + quarantine_count, (
    "Reconciliation failed: record counts do not match"
)

print("Reconciliation passed!")
print(
    f"{bronze_count} = {valid_count} valid "
    f"+ {quarantine_count} quarantined"
)

# COMMAND ----------


# Find why records were quarantined
display(
    quarantine_df
    .groupBy("failure_reason")
    .count()
    .orderBy(F.desc("count"))
)

# Check whether valid records contain duplicate request IDs
duplicate_ids_df = (
    valid_df
    .groupBy("unique_key")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate ID groups:", duplicate_ids_df.count())

# Compare incoming columns with our earlier test table
old_columns = set(
    spark.table(
        "workspace.trustflow_dev_silver.nyc_311_requests"
    ).columns
)

new_columns = set(valid_df.columns)

print("New columns:", sorted(new_columns - old_columns))
print("Missing columns:", sorted(old_columns - new_columns))


# COMMAND ----------

display(
    quarantine_df
    .groupBy("failure_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

silver_table = "workspace.trustflow_silver.nyc_311_requests"
quality_table = "workspace.trustflow_quality.nyc_311_quarantine"

print("Main Silver table exists:", spark.catalog.tableExists(silver_table))
print("Main Quarantine table exists:", spark.catalog.tableExists(quality_table))

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.trustflow_silver
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.trustflow_quality
""")

print("Main Silver and Quality schemas are ready!")

# COMMAND ----------


from pyspark.sql import functions as F

batch_id = "nyc_311_historical_2026-10-26"

quarantine_to_save = (
    quarantine_df
    .withColumn("batch_id", F.lit(batch_id))
    .withColumn("quarantined_at", F.current_timestamp())
)

quarantine_to_save.createOrReplaceTempView(
    "nyc_311_initial_quarantine"
)

spark.sql("""
CREATE TABLE IF NOT EXISTS
workspace.trustflow_quality.nyc_311_quarantine
USING DELTA
AS SELECT * FROM nyc_311_initial_quarantine
""")

print(
    "Saved quarantine records:",
    spark.table(
        "workspace.trustflow_quality.nyc_311_quarantine"
    ).count()
)


# COMMAND ----------


silver_to_save = (
    valid_df
    .withColumn("ingestion_batch_id", F.lit(batch_id))
    .withColumn("loaded_at", F.current_timestamp())
)

silver_to_save.createOrReplaceTempView(
    "nyc_311_initial_silver"
)

spark.sql("""
CREATE TABLE IF NOT EXISTS
workspace.trustflow_silver.nyc_311_requests
USING DELTA
AS SELECT * FROM nyc_311_initial_silver
""")

print(
    "Saved Silver records:",
    spark.table(
        "workspace.trustflow_silver.nyc_311_requests"
    ).count()
)


# COMMAND ----------


silver_count = spark.table(
    "workspace.trustflow_silver.nyc_311_requests"
).count()

quarantine_count = spark.table(
    "workspace.trustflow_quality.nyc_311_quarantine"
).count()

bronze_count = bronze_df.count()

print("Bronze:", bronze_count)
print("Silver:", silver_count)
print("Quarantine:", quarantine_count)

assert silver_count + quarantine_count == bronze_count

print("Initial load reconciliation PASSED!")
