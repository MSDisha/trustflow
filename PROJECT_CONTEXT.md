# TrustFlow — Project Context

## Project Identity

**Name:** TrustFlow — Unified Data Intelligence

**Owner:** Disha

**Goal:** Build a reliable, multi-source public-data intelligence platform using modern data engineering technologies.

## Architecture Decisions

The selected architecture is:

1. Python for API ingestion
2. JSON / Parquet for Bronze raw data
3. Databricks + PySpark + Delta Lake for Silver processing
4. dbt on Databricks for Gold analytical models
5. Airflow for orchestration
6. pytest and dbt tests for quality
7. Docker for local reproducibility
8. Streamlit for visualization and monitoring
9. GitHub Actions for CI/CD

**Important:** Databricks + PySpark + Delta Lake + dbt is the selected processing stack. Do not substitute another processing engine without documenting the reason and reviewing the decision.

**Budget constraint:** Zero mandatory paid services for the initial portfolio implementation.

## Required Data Sources

- NYC Open Data — 311 Service Requests
- USGS — Earthquakes
- FRED — Economic Indicators
- EIA — Electricity Data
- NWS — Weather Data

All five sources must eventually support ingestion, Bronze, Silver, Gold, quality checks, and Streamlit analytics.

## Completed and Verified

### Local Development

- Python virtual environment created on macOS
- Git repository initialized
- Initial project structure created
- `requests`, `pyarrow`, and `pytest` installed
- dbt Databricks adapter installed

### NYC 311 Bronze Prototype

- Connected to the NYC 311 API
- Downloaded and saved raw JSON data
- Implemented five-page offset pagination
- Successfully downloaded 500 records in one run
- Inspected source fields and timestamps
- Verified zero missing and duplicate unique keys within the inspected 500-record batch
- Implemented ingestion logging and run metadata
- Added automatic HTTP retry configuration

### Databricks Feasibility

- Successfully executed PySpark code
- Created and read a Delta table
- Successfully executed Delta MERGE
- Verified existing-record update and new-record insertion

### dbt Feasibility

- Installed dbt Core and Databricks adapter
- Created the `trustflow_gold` dbt project
- Configured Databricks connection
- Successfully completed `dbt debug`

## Not Yet Completed

- End-to-end NYC 311 Silver and Gold processing
- Real-data dbt Gold model execution
- Robust historical and incremental ingestion
- Persistent checkpoint management
- Automated failure-recovery tests
- All remaining source connectors
- Airflow orchestration
- Streamlit dashboards
- Docker integration
- GitHub Actions workflows

## Current Development Position

The NYC 311 Bronze prototype is working.

Databricks PySpark, Delta Lake MERGE, and dbt connectivity have passed initial feasibility checks.

Development is temporarily focused on creating the first GitHub commit before continuing with real analytical models.

## Next Steps

1. Complete README and project context documentation.
2. Review and push the initial code to GitHub.
3. Run the first dbt Gold feasibility model.
4. Make real NYC Bronze data accessible to Databricks.
5. Build the NYC Silver Delta table using PySpark.
6. Build Gold analytics using dbt.
7. Build the first Streamlit dashboard.
8. Improve historical loading, incremental ingestion, reconciliation, and recovery.
9. Extend the framework to the remaining four APIs.

## Engineering Principles

- Preserve original source data.
- Design for growth beyond the development sample.
- Avoid hardcoded record-count assumptions in core processing logic.
- Use idempotent processing where possible.
- Do not advance processing checkpoints before successful validation.
- Separate ingestion, cleaning, analytics, and presentation responsibilities.
- Track actual test results instead of assuming a feature is complete.
- Keep credentials and local raw data out of GitHub.
- Document important architectural changes.

## Longer-Term Vision

Evolve TrustFlow into a configurable, customer-facing data intelligence platform with reusable connectors, validation rules, monitoring, explainable failures, and human-approved remediation.