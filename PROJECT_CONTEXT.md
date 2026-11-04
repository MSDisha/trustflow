
# TrustFlow - Project Context and Engineering Handoff

**Project:** TrustFlow - Unified Data Intelligence  
**Tagline:** From raw government data to reliable, decision-ready intelligence.  
**Last Updated:** November 4, 2026  
**Current Phase:** NYC 311 Gold completed; Streamlit next  
**Development Status:** In progress

---

## 1. Project Overview

TrustFlow is a multi-source data engineering portfolio project designed to convert public-sector API data into reliable analytical datasets.

The platform combines:

- API ingestion.
- Raw data preservation.
- PySpark transformations.
- Delta Lake incremental processing.
- dbt analytical modeling.
- Data validation and reconciliation.
- Operational data-health monitoring.
- Interactive Streamlit dashboards.
- Future Airflow orchestration.

The platform follows a Bronze–Silver–Gold lakehouse architecture.

The objective is to demonstrate practical, end-to-end Data Engineering skills while building a reusable foundation for multiple independent data sources.

## 2. Final Technology Stack

| Component | Selected Technology |
|---|---|
| Ingestion | Python |
| Bronze storage | Raw JSON / Parquet |
| Processing platform | Databricks |
| Silver processing | PySpark |
| Silver storage | Delta Lake |
| Gold transformations | dbt Core on Databricks |
| Data validation | PySpark, dbt, pytest |
| Orchestration | Apache Airflow |
| Visualization | Streamlit |
| Local reproducibility | Docker |
| CI/CD | GitHub Actions |
| Version control | Git and GitHub |

**Architecture decision:** Databricks, PySpark, Delta Lake, and dbt are the chosen lakehouse components. 

The project targets zero mandatory infrastructure spending.

## 3. Data Sources

TrustFlow must eventually implement all five sources end to end.

| Source | Domain | Status |
|---|---|---|
| NYC Open Data | 311 Service Requests | Gold completed |
| USGS | Earthquakes | Not started |
| FRED | Economic Indicators | Not started |
| EIA | Electricity | Not started |
| NWS | Weather | Not started |

Each source will eventually include:

1. Ingestion.
2. Bronze storage.
3. Silver processing.
4. Gold analytical models.
5. Data-quality checks.
6. Health monitoring.
7. Streamlit outputs.
8. Automated orchestration.

The number of Gold models will depend on each source's actual analytical needs.

## 4. Architecture

```text
Public-Sector APIs
        |
        v
Python Ingestion
        |
        v
Bronze — Raw Data and Metadata
        |
        v
Databricks PySpark
        |
        +---------------------+
        |                     |
        v                     v
Silver Delta Tables      Audit / Quarantine
        |                     |
        v                     v
Analytical Gold          Data Health Gold
        |                     |
        +----------+----------+
                   |
                   v
             dbt Validation
                   |
                   v
           Streamlit (Planned)
```

Apache Airflow will eventually orchestrate the pipeline.

## 5. Completed NYC 311 Bronze Layer

### Main Scripts

```text
src/ingestion/nyc_311.py
src/ingestion/nyc_311_refresh.py
src/common/databricks_bronze_uploader.py
```

### Historical Ingestion

The historical ingestion pipeline retrieves NYC 311 records for defined creation-date ranges.

Implemented functionality includes:

- API requests.
- Pagination.
- Historical batch processing.
- Raw payload preservation.
- Batch metadata generation.
- Uploading source batches to Databricks.

### Incremental Refresh

The refresh ingestion script reobserves complaints created within a rolling 14-day window.

This supports detecting recent:

- New complaints.
- Changed complaint statuses.
- Changed record attributes.
- Existing unchanged complaints.

**Known limitation:** Complaints created more than 14 days earlier can still change, but the current creation-date refresh may not retrieve those changes.

This limitation must be addressed before claiming complete long-term change capture.

### Bronze Storage

```text
/Volumes/workspace/default/trustflow_bronze/nyc_311/
```

Raw source data and batch metadata are preserved in the Databricks Unity Catalog Volume.

The uploader verifies remote SHA256 hashes and writes metadata after the associated data assets.

## 6. Completed NYC 311 Silver Layer

### Databricks Notebook

```text
databricks/trustflow_nyc311_silver_v2.ipynb
```

### Main Silver Table

```text
workspace.trustflow_silver.nyc_311_requests_v2
```

### Quality Tables

```text
workspace.trustflow_quality.nyc_311_batch_audit_v2

workspace.trustflow_quality.nyc_311_refresh_audit_v2

workspace.trustflow_quality.nyc_311_quarantine_v2
```

### Silver Processing Responsibilities

The Silver pipeline:

1. Reads Bronze source batches.
2. Validates record structure and fields.
3. Identifies records requiring quarantine.
4. Deduplicates eligible records.
5. Maintains the latest observed record state.
6. Inserts new records.
7. Updates changed existing records.
8. Skips unchanged records.
9. Records processing audit information.
10. Performs reconciliation checks.

Delta Lake MERGE supports incremental current-state updates.

### Processing Safety

The Silver notebook contains preview/apply controls.

The apply mode requires the intended write-confirmation value.

Multiple concurrent Silver writers should be avoided.

Silver table, quarantine, and audit writes are separate commits, so partial-failure recovery still requires further testing.

## 7. NYC 311 Processing Snapshot

### November 3, 2026 Refresh

| Metric | Recorded Value |
|---|---:|
| Source records | 140,660 |
| Valid records | 140,528 |
| Quarantined records | 132 |
| Planned inserts | 55 |
| Planned updates | 3,015 |
| Planned skips | 137,458 |
| Audit status | SUCCESS |

Reconciliation:

```text
Valid + Quarantined = Source

140,528 + 132 = 140,660
```

**Interpretation:** All records in that observed refresh snapshot were accounted for by the validation totals.

The figures do not establish that every NYC 311 record available through the external API was retrieved.

## 8. Completed Analytical Gold Models

**Model count: 6**

**dbt location:**

```text
dbt/trustflow_gold/models/nyc_311/
```

**Target Databricks schema:**

```text
workspace.trustflow_gold
```

### Model Inventory

| Model | Grain | Purpose |
|---|---|---|
| `daily_complaints` | Creation date | Daily complaint volume |
| `borough_metrics` | Borough | Borough-level complaint distribution |
| `complaint_type_metrics` | Complaint type | Complaint category distribution |
| `agency_performance` | Agency | Agency workload and status counts |
| `resolution_time_metrics` | Complaint type | Closure-duration statistics |
| `borough_complaint_trends` | Creation date + borough | Daily borough trends |

### Important Metric Definitions

**Daily complaints**

Shows complaint counts grouped by creation date, using the latest recorded request statuses.

**Borough metrics**

Summarizes requests across normalized borough values.

**Complaint type metrics**

Summarizes complaint categories and current statuses.

**Agency performance**

Measures workload and request status distribution. It is not a complete agency-efficiency score.

**Resolution-time metrics**

Uses closed requests with valid creation and closure timestamps.

Provides:

- Average resolution hours.
- Approximate median resolution hours.
- Approximate p90 resolution hours.
- Measured closed requests.
- Excluded closed requests.

**Borough complaint trends**

Calculates daily volumes and changes by borough.

Percentage changes require valid consecutive dates and a nonzero previous-day count.

### Analytical Limitations

- Current status is not historical status as of the complaint creation day.
- Creation-date counts are not closure-event counts.
- Recent dates may be incomplete.
- Small resolution samples can produce unstable statistics.
- Complaint-type normalization can be improved where source values differ in capitalization.

## 9. Completed Data Health Gold Models

**Model count: 3**

**Location:**

```text
dbt/trustflow_gold/models/nyc_311/health/
```

### 9.1 `nyc_311_batch_health`

Combines historical and refresh audit records.

**Grain:** One row per recorded processing batch.

Fields include:

- `batch_id`
- `batch_type`
- `snapshot_date`
- `observed_at`
- `source_count`
- `valid_count`
- `quarantine_count`
- `planned_insert`
- `planned_update`
- `planned_skip`
- `status`
- `started_at`
- `completed_at`
- `duration_seconds`

**Dashboard purpose:** Display processing history, recorded statuses, source counts, refresh activity, and available run durations.

Historical processing duration remains NULL when no start time exists.

### 9.2 `nyc_311_quality_health`

Calculates data-quality metrics for each recorded batch.

**Grain:** One row per recorded batch.

Important fields:

- `source_count`
- `valid_count`
- `quarantine_count`
- `valid_pct`
- `quarantine_pct`
- `reconciliation_difference`
- `reconciliation_status`

Core calculations:

```text
Valid Percentage =
    Valid Count / Source Count * 100

Quarantine Percentage =
    Quarantine Count / Source Count * 100

Reconciliation Difference =
    Source Count - Valid Count - Quarantine Count
```

The reconciliation status distinguishes matched counts, mismatches, and invalid counts.

A matched status does not prove external source completeness.

### 9.3 `nyc_311_freshness_health`

Monitors the latest recorded refresh attempt and latest successful refresh.

**Grain:** One row representing a freshness evaluation.

Important fields:

- `evaluated_at`
- `latest_attempt_batch_id`
- `latest_attempt_status`
- `latest_attempt_completed_at`
- `latest_success_completed_at`
- `hours_since_success`
- `freshness_threshold_hours`
- `freshness_status`

Default freshness threshold:

```text
36 hours
```

The threshold can be configured with:

```text
nyc_311_refresh_freshness_hours
```

Possible freshness classifications:

- FRESH
- STALE
- NO_SUCCESSFUL_REFRESH
- CLOCK_ERROR

**Important:** Freshness is evaluated when dbt builds the table. Its stored status does not update continuously as time passes.

## 10. dbt Configuration

**Project directory:**

```text
dbt/trustflow_gold/
```

**dbt project name:**

```text
trustflow_gold
```

**Target catalog:**

```text
workspace
```

**Target schema:**

```text
trustflow_gold
```

**Connection:** Databricks SQL warehouse through the dbt Databricks adapter.

### Verified Tool Versions

| Tool | Version |
|---|---|
| dbt Core | 1.12.5 |
| dbt Databricks adapter | 1.12.8 |

### Registered dbt Sources

```text
workspace.trustflow_silver.nyc_311_requests_v2

workspace.trustflow_quality.nyc_311_batch_audit_v2

workspace.trustflow_quality.nyc_311_refresh_audit_v2
```

Authentication is configured locally. Never commit connection credentials or tokens.

## 11. Completed dbt Validation

**Total configured tests: 32**

### Analytical Tests

18 tests covering:

- Uniqueness.
- NOT NULL checks.
- Combined date-and-borough uniqueness.
- Status reconciliation.
- Silver-to-Gold reconciliation.
- Resolution reconciliation.
- Numeric validity.
- Daily borough reconciliation.

### Health Tests

14 tests covering:

- Batch identity uniqueness.
- Required health fields.
- Reconciliation.
- Numeric and percentage validity.
- Freshness consistency.
- Exactly one freshness summary row.

### Validation Evidence

The complete build containing 9 Gold models and 31 tests passed.

The additional 32nd freshness single-row test passed separately.

A complete `dbt build` containing all 32 tests is an optional final verification before release.

### Commands

```bash
cd dbt/trustflow_gold

dbt debug

dbt ls --resource-type model

dbt ls --resource-type source

dbt test

dbt build
```

For a specific model:

```bash
dbt build --select nyc_311_batch_health
```

For a specific test:

```bash
dbt test --select assert_freshness_single_row
```

## 12. Current Implementation Status

| Component | Status |
|---|---|
| NYC 311 historical ingestion | Implemented |
| NYC 311 refresh ingestion | Implemented |
| Databricks Bronze storage | Implemented |
| PySpark Silver processing | Implemented |
| Incremental Delta MERGE | Implemented |
| Audit and quarantine tables | Implemented |
| Six Analytical Gold models | Completed |
| Three Data Health Gold models | Completed |
| 32 dbt tests | Passed individually |
| Streamlit dashboard | Not started |
| Airflow orchestration | Not started |
| pytest suite | Planned |
| Docker setup | Planned |
| GitHub Actions CI/CD | Planned |
| Other four source pipelines | Planned |

## 13. Known Technical Risks

### Risk 1 — Missing Updates to Older Complaints

A rolling creation-date window does not capture every older record that changes.

**Next improvement:** Design and validate change-aware incremental ingestion or a broader reobservation strategy.

### Risk 2 — Ingestion Failure Visibility

Some failures may occur before a Silver audit record exists.

**Next improvement:** Add run-level ingestion logging and orchestration failure monitoring.

### Risk 3 — Partial Processing Failure

Silver data, audit, and quarantine tables are updated through separate writes.

**Next improvement:** Test retries, idempotency, and recovery after partial failures.

### Risk 4 — Freshness Evaluation

Materialized dbt freshness metrics reflect the latest model execution time.

**Next improvement:** Schedule regular evaluation or calculate display-time freshness.

### Risk 5 — External Source Completeness

Reconciliation proves consistency for observed records, not completeness of the entire external API.

**Next improvement:** Implement explicit source-level coverage checks where supported.

### Risk 6 — Free-Tier Execution

Databricks Free Edition and local environments may not support continuously available unattended execution.

**Next improvement:** Verify feasible orchestration and deployment within zero mandatory cost.

## 14. Next Development Phase — Streamlit

The next major milestone is to create a Streamlit application for NYC 311.

### Planned Analytics Page

- Daily complaint trends.
- Borough comparisons.
- Complaint-type distribution.
- Agency workload.
- Resolution-time statistics.
- Borough complaint trends.

### Planned Data Health Page

- Batch processing history.
- Latest recorded processing status.
- Valid and quarantined record metrics.
- Reconciliation status.
- Refresh activity.
- Processing duration.
- Latest successful refresh.
- Freshness indicators.

### Planned Data Connection

Use the Databricks SQL Connector for Python with secure credentials and query caching.

No credentials should be hardcoded or stored in Git.

## 15. Planned Automation

After the initial Streamlit implementation, add an Airflow workflow.

Target orchestration:

```text
Python Ingestion
       |
       v
Bronze Upload and Verification
       |
       v
Silver Processing and Audit
       |
       v
dbt Gold Models
       |
       v
dbt Validation Tests
       |
       v
Health Evaluation
       |
       v
Streamlit Data Availability
```

The workflow must support:

- Scheduled runs.
- Dependency ordering.
- Retries.
- Failure reporting.
- Run metadata.
- Safe incremental processing.
- Recovery from partial failures.

The scheduler must not claim success before downstream validation completes.

## 16. Remaining Source Pipelines

After the NYC 311 vertical slice is demonstrated, extend TrustFlow to:

### USGS Earthquakes

Earthquake ingestion, validated events, analytical metrics, and health monitoring.

### FRED Economic Indicators

Economic-series ingestion, observation-date modeling, analytical trends, and freshness monitoring.

### EIA Electricity

Energy-data ingestion, standardized measurements, analytical trends, and data quality.

### NWS Weather

Weather ingestion, observation/forecast distinctions, curated analytics, and health checks.

Each source must have a meaningful end-to-end implementation.

## 17. Engineering Standards

Follow these principles throughout development:

1. Prefer working, tested implementations over conceptual complexity.
2. Keep ingestion, Silver, Gold, and dashboard responsibilities separate.
3. Preserve source metadata and processing lineage.
4. Treat API observation dates and business-event dates differently.
5. Build repeatable, idempotent pipelines.
6. Validate data before exposing it to analytics.
7. Monitor both pipeline execution and data quality.
8. Make incomplete or stale data visible.
9. Never commit credentials or secret files.
10. Avoid unnecessary mandatory infrastructure costs.
11. Document every major architecture decision.
12. Do not claim production readiness before automated execution and recovery are demonstrated.

## 18. Instructions for Continuing Development

This file is the current technical handoff for TrustFlow.

When resuming development:

- Review this document and the actual repository.
- Continue from the latest completed implementation.
- Do not repeat completed beginner setup steps.
- Verify existing code before proposing modifications.
- Give clear explanations for new SQL and Python logic.
- Prefer permanent, maintainable solutions.
- Keep the five-source scope unchanged.
- Do not silently change the selected technology stack.
- Update this context file after major milestones.
- Clearly distinguish implemented functionality from planned functionality.

### Immediate Next Task

**Build the NYC 311 Streamlit dashboard using the nine completed Gold models.**

Begin with a secure Databricks connection, then create the Analytics and Data Health dashboard sections.

---

**TrustFlow — Unified Data Intelligence**

*Engineering reliable data from ingestion to insight.*
