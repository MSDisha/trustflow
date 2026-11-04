
{{ config(materialized='table') }}

-- TRUSTFLOW GOLD — NYC 311 Daily Complaints
-- One row per complaint creation date.
-- Supports daily volume trends and status monitoring.

WITH daily_requests AS (
    SELECT
        CAST(created_date AS DATE) AS complaint_date,
        LOWER(TRIM(status)) AS request_status
    FROM {{ source('nyc_311_silver', 'nyc_311_requests_v2') }}
    WHERE created_date IS NOT NULL
)

SELECT
    complaint_date,
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

FROM daily_requests
GROUP BY complaint_date
