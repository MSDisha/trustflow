
{{ config(materialized='table') }}

-- TRUSTFLOW GOLD — NYC 311 Resolution Time Metrics
-- Grain: One row per normalized complaint type.
-- Purpose: Measure closure time and identify missing/invalid dates.
-- Only currently CLOSED complaints with valid dates
-- contribute to resolution-time statistics.

WITH cleaned_requests AS (
    SELECT
        COALESCE(
            NULLIF(TRIM(complaint_type), ''),
            'UNKNOWN'
        ) AS complaint_type,

        LOWER(TRIM(status)) AS request_status,

        created_date,
        closed_date

    FROM {{ source('nyc_311_silver', 'nyc_311_requests_v2') }}
),

classified_requests AS (
    SELECT
        complaint_type,
        request_status,

        CASE
            WHEN request_status = 'closed'
             AND created_date IS NOT NULL
             AND closed_date IS NOT NULL
             AND closed_date >= created_date
            THEN timestampdiff(SECOND, created_date, closed_date) / 3600.0
            ELSE NULL
        END AS resolution_hours,

        CASE
            WHEN request_status = 'closed'
             AND (
                 created_date IS NULL
                 OR closed_date IS NULL
                 OR closed_date < created_date
             )
            THEN 1 ELSE 0
        END AS invalid_closed_date_flag

    FROM cleaned_requests
)

SELECT
    complaint_type,

    COUNT(*) AS total_requests,

    SUM(
        CASE WHEN request_status = 'closed'
        THEN 1 ELSE 0 END
    ) AS closed_requests,

    COUNT(resolution_hours) AS measured_closed_requests,

    SUM(invalid_closed_date_flag) AS excluded_closed_requests,

    ROUND(AVG(resolution_hours), 2)
        AS avg_resolution_hours,

    ROUND(
        percentile_approx(resolution_hours, 0.5),
        2
    ) AS median_resolution_hours,

    ROUND(
        percentile_approx(resolution_hours, 0.9),
        2
    ) AS p90_resolution_hours

FROM classified_requests

GROUP BY complaint_type
