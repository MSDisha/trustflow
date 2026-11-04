
-- TRUSTFLOW GOLD — SILVER-TO-GOLD RECONCILIATION
-- Verify that all six Gold models account for Silver records.
-- Returns zero rows when every comparison passes.
-- No hardcoded dates or record counts.

WITH silver_counts AS (
    SELECT
        COUNT(*) AS all_requests,
        COUNT(created_date) AS dated_requests
    FROM {{ source('nyc_311_silver', 'nyc_311_requests_v2') }}
),

gold_counts AS (

    SELECT
        'borough_metrics' AS model_name,
        SUM(total_requests) AS actual_count
    FROM {{ ref('borough_metrics') }}

    UNION ALL

    SELECT
        'complaint_type_metrics',
        SUM(total_requests)
    FROM {{ ref('complaint_type_metrics') }}

    UNION ALL

    SELECT
        'agency_performance',
        SUM(total_requests)
    FROM {{ ref('agency_performance') }}

    UNION ALL

    SELECT
        'resolution_time_metrics',
        SUM(total_requests)
    FROM {{ ref('resolution_time_metrics') }}

    UNION ALL

    SELECT
        'daily_complaints',
        SUM(total_requests)
    FROM {{ ref('daily_complaints') }}

    UNION ALL

    SELECT
        'borough_complaint_trends',
        SUM(total_requests)
    FROM {{ ref('borough_complaint_trends') }}
),

reconciliation AS (
    SELECT
        g.model_name,

        CASE
            WHEN g.model_name IN (
                'daily_complaints',
                'borough_complaint_trends'
            )
            THEN s.dated_requests
            ELSE s.all_requests
        END AS expected_count,

        g.actual_count

    FROM gold_counts g
    CROSS JOIN silver_counts s
)

SELECT
    model_name,
    expected_count,
    actual_count,
    actual_count - expected_count AS difference

FROM reconciliation

WHERE actual_count IS NULL
   OR actual_count != expected_count
