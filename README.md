# TrustFlow - Unified Data Intelligence

**From raw government data to reliable, decision-ready intelligence.**

## Project Overview

TrustFlow is an end-to-end data engineering portfolio project designed to ingest, process, validate, and analyze information from five public-sector data sources.

The platform follows a Bronze–Silver–Gold lakehouse architecture to produce reliable analytical datasets and display insights through Streamlit.

## Data Sources

1. **NYC Open Data:** 311 Service Requests
2. **USGS:** Earthquake Data
3. **FRED:** Economic Indicators
4. **EIA:** Electricity Data
5. **NWS:** Weather Data

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | API ingestion |
| JSON / Parquet | Bronze raw storage |
| Databricks + PySpark | Silver transformations |
| Delta Lake | Reliable tables and MERGE operations |
| dbt | Gold analytical models and data tests |
| Apache Airflow | Pipeline orchestration |
| pytest | Python testing |
| Docker | Local reproducibility |
| Streamlit | Analytics and monitoring |
| GitHub Actions | CI/CD |

## Architecture

Government APIs → Python Ingestion → Bronze (JSON / Parquet) → Silver (PySpark + Delta Lake) → Gold (dbt on Databricks) → Quality & Reconciliation → Streamlit

Airflow will orchestrate the pipeline, while automated testing, logging, and monitoring will support data reliability.

## Current Implementation Status

**Project status: In development**

### Implemented and verified

- NYC 311 API ingestion using Python
- Raw JSON storage with timestamped files
- Basic offset-based pagination
- Data inspection and duplicate-key checks
- Batch metadata and ingestion logging
- HTTP retry configuration
- Databricks Free Edition PySpark execution
- Delta table creation and MERGE feasibility test
- Successful dbt connection to Databricks

### Planned

- Safe incremental ingestion and historical backfills
- Automated retry and failure-recovery tests
- Real NYC 311 Silver and Gold transformations
- Complete pipelines for all five APIs
- Data reconciliation and freshness monitoring
- Airflow orchestration and Docker setup
- Streamlit analytics and monitoring dashboards
- GitHub Actions CI/CD

## Project Goals

- Build reliable ingestion pipelines for five public APIs.
- Handle record updates, duplicates, schema differences, and failed runs.
- Create useful analytical models and dashboards.
- Demonstrate real-world data engineering practices.
- Build an extensible foundation for a future configurable data intelligence product.

## Development Approach

TrustFlow is being developed incrementally. The initial milestone uses NYC 311 to validate an end-to-end workflow before extending the shared framework to the other four sources.

The initial implementation targets zero mandatory infrastructure spending.

## Project Documentation

See `PROJECT_CONTEXT.md` for architecture decisions, milestone progress, and next development tasks.