
{{ config(materialized='table') }}

-- TRUSTFLOW GOLD — NYC 311 Complaint Type Metrics
-- Grain: One row per normalized complaint type.
-- Purpose: Measure complaint volume and current status by type.
-- Rebuilt by dbt after a successful Silver refresh.

WITH cleaned_requests AS (
    SELECT
        COALESCE(
            NULLIF(TRIM(complaint_type), ''),
            'UNKNOWN'
        ) AS complaint_type,

        LOWER(TRIM(status)) AS request_status

    FROM {{ source('nyc_311_silver', 'nyc_311_requests_v2') }}
)

SELECT
    complaint_type,

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

GROUP BY complaint_type
