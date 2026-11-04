
{{ config(materialized='table') }}

-- TRUSTFLOW GOLD — NYC 311 Borough Complaint Trends
-- Grain: One row per creation date and normalized borough.
-- Purpose: Track daily complaint volume and current status by borough.
-- Includes previous-day count and day-over-day changes.
-- Rebuilt after a successful Silver refresh.

WITH cleaned_requests AS (
    SELECT
        CAST(created_date AS DATE) AS complaint_date,

        COALESCE(
            NULLIF(UPPER(TRIM(borough)), ''),
            'UNKNOWN'
        ) AS borough,

        LOWER(TRIM(status)) AS request_status

    FROM {{ source('nyc_311_silver', 'nyc_311_requests_v2') }}
    WHERE created_date IS NOT NULL
),

daily_borough_counts AS (
    SELECT
        complaint_date,
        borough,

        COUNT(*) AS total_requests,

        SUM(
            CASE WHEN request_status = 'open'
            THEN 1 ELSE 0 END
        ) AS open_requests,

        SUM(
            CASE WHEN request_status = 'closed'
            THEN 1 ELSE 0 END
        ) AS closed_requests,

        SUM(
            CASE
                WHEN request_status IS NULL
                  OR request_status NOT IN ('open', 'closed')
                THEN 1 ELSE 0
            END
        ) AS other_status_requests

    FROM cleaned_requests
    GROUP BY complaint_date, borough
),

daily_trends AS (
    SELECT
        *,

        LAG(total_requests) OVER (
            PARTITION BY borough
            ORDER BY complaint_date
        ) AS previous_observed_day_requests,

        LAG(complaint_date) OVER (
            PARTITION BY borough
            ORDER BY complaint_date
        ) AS previous_observed_date

    FROM daily_borough_counts
)

SELECT
    complaint_date,
    borough,
    total_requests,
    open_requests,
    closed_requests,
    other_status_requests,

    previous_observed_day_requests,

    CASE
        WHEN datediff(
            complaint_date,
            previous_observed_date
        ) = 1
        THEN total_requests - previous_observed_day_requests
        ELSE NULL
    END AS daily_request_change,

    CASE
        WHEN datediff(
            complaint_date,
            previous_observed_date
        ) = 1
         AND previous_observed_day_requests > 0

        THEN ROUND(
            (
                total_requests - previous_observed_day_requests
            ) * 100.0 / previous_observed_day_requests,
            2
        )

        ELSE NULL
    END AS daily_change_pct

FROM daily_trends
