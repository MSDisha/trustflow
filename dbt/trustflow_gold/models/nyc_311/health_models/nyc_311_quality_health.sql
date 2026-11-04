
{{ config(materialized='table') }}

-- TRUSTFLOW NYC 311 — DATA QUALITY HEALTH
-- One row per recorded pipeline batch.
-- Measures data quality and count reconciliation.

WITH batch_data AS (

    SELECT
        batch_id,
        batch_type,
        snapshot_date,
        status,
        completed_at,

        source_count,
        valid_count,
        quarantine_count

    FROM {{ ref('nyc_311_batch_health') }}

),

quality_metrics AS (

    SELECT
        *,

        -- Percentage of valid records
        CASE
            WHEN source_count > 0
            THEN ROUND(
                valid_count * 100.0 / source_count,
                2
            )
            ELSE NULL
        END AS valid_pct,

        -- Percentage of quarantined records
        CASE
            WHEN source_count > 0
            THEN ROUND(
                quarantine_count * 100.0 / source_count,
                2
            )
            ELSE NULL
        END AS quarantine_pct,

        -- Difference between input and classified records
        source_count
            - valid_count
            - quarantine_count AS reconciliation_difference

    FROM batch_data

)

SELECT
    batch_id,
    batch_type,
    snapshot_date,
    status,
    completed_at,

    source_count,
    valid_count,
    quarantine_count,

    valid_pct,
    quarantine_pct,
    reconciliation_difference,

    CASE
        WHEN source_count IS NULL
          OR valid_count IS NULL
          OR quarantine_count IS NULL
        THEN 'INVALID_COUNTS'

        WHEN source_count < 0
          OR valid_count < 0
          OR quarantine_count < 0
        THEN 'INVALID_COUNTS'

        WHEN reconciliation_difference != 0
        THEN 'MISMATCH'

        ELSE 'MATCHED'
    END AS reconciliation_status

FROM quality_metrics
