
{{ config(materialized='table') }}

-- TRUSTFLOW NYC 311 — FRESHNESS HEALTH
-- Monitor the most recent recorded refresh attempt
-- and the most recent successful refresh.
--
-- Freshness threshold: 36 hours (configurable).
-- Calculated when dbt builds this table.

WITH refresh_batches AS (

    SELECT
        batch_id,
        status,
        started_at,
        completed_at
    FROM {{ ref('nyc_311_batch_health') }}
    WHERE batch_type = 'refresh'

),

latest_attempt AS (

    SELECT
        batch_id,
        status,
        started_at,
        completed_at,

        ROW_NUMBER() OVER (
            ORDER BY
                COALESCE(completed_at, started_at) DESC,
                batch_id DESC
        ) AS row_num

    FROM refresh_batches

),

successful_refresh AS (

    SELECT
        MAX(completed_at) AS latest_success_completed_at

    FROM refresh_batches

    WHERE UPPER(TRIM(status)) = 'SUCCESS'
      AND completed_at IS NOT NULL

),

freshness_metrics AS (

    SELECT
        CURRENT_TIMESTAMP() AS evaluated_at,

        a.batch_id AS latest_attempt_batch_id,
        a.status AS latest_attempt_status,
        a.completed_at AS latest_attempt_completed_at,

        s.latest_success_completed_at,

        CASE
            WHEN s.latest_success_completed_at IS NOT NULL
            THEN ROUND(
                TIMESTAMPDIFF(
                    SECOND,
                    s.latest_success_completed_at,
                    CURRENT_TIMESTAMP()
                ) / 3600.0,
                2
            )
            ELSE NULL
        END AS hours_since_success,

        CAST(
            {{ var('nyc_311_refresh_freshness_hours', 36) }}
            AS DOUBLE
        ) AS freshness_threshold_hours

    FROM successful_refresh s

    LEFT JOIN latest_attempt a
        ON a.row_num = 1

)

SELECT
    *,

    CASE
        WHEN latest_success_completed_at IS NULL
            THEN 'NO_SUCCESSFUL_REFRESH'

        WHEN hours_since_success < 0
            THEN 'CLOCK_ERROR'

        WHEN hours_since_success > freshness_threshold_hours
            THEN 'STALE'

        ELSE 'FRESH'
    END AS freshness_status

FROM freshness_metrics
