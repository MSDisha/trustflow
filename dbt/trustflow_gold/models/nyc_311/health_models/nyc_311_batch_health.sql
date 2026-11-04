
{{ config(materialized='table') }}

-- TRUSTFLOW NYC 311 — BATCH HEALTH
-- One row per recorded historical or refresh batch.
-- Unifies existing Silver audit data for pipeline monitoring.

WITH historical_batches AS (

    SELECT
        batch_id,
        'historical' AS batch_type,

        TO_DATE(process_date) AS snapshot_date,

        CAST(NULL AS STRING) AS observed_at,

        bronze_count AS source_count,
        valid_count,
        quarantine_count,

        CAST(NULL AS BIGINT) AS planned_insert,
        CAST(NULL AS BIGINT) AS planned_update,
        CAST(NULL AS BIGINT) AS planned_skip,

        status,

        CAST(NULL AS TIMESTAMP) AS started_at,
        processed_at AS completed_at

    FROM {{ source('nyc_311_quality', 'nyc_311_batch_audit_v2') }}

),

refresh_batches AS (

    SELECT
        batch_id,
        'refresh' AS batch_type,

        TO_DATE(snapshot_date) AS snapshot_date,

        observed_at,

        source_count,
        valid_count,
        quarantine_count,

        planned_insert,
        planned_update,
        planned_skip,

        status,

        started_at,
        completed_at

    FROM {{ source('nyc_311_quality', 'nyc_311_refresh_audit_v2') }}

),

all_batches AS (

    SELECT * FROM historical_batches

    UNION ALL

    SELECT * FROM refresh_batches

)

SELECT
    batch_id,
    batch_type,
    snapshot_date,
    observed_at,

    source_count,
    valid_count,
    quarantine_count,

    planned_insert,
    planned_update,
    planned_skip,

    status,
    started_at,
    completed_at,

    CASE
        WHEN started_at IS NOT NULL
         AND completed_at IS NOT NULL
         AND completed_at >= started_at
        THEN TIMESTAMPDIFF(
            SECOND,
            started_at,
            completed_at
        )
        ELSE NULL
    END AS duration_seconds

FROM all_batches
