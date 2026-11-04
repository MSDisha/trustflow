
-- TRUSTFLOW GOLD — DAILY BOROUGH RECONCILIATION
-- Verify that citywide daily totals match borough daily totals.
-- Includes missing dates on either side.
-- PASS = zero rows returned.

WITH citywide AS (
    SELECT
        complaint_date,
        total_requests AS citywide_requests
    FROM {{ ref('daily_complaints') }}
),

borough_daily AS (
    SELECT
        complaint_date,
        SUM(total_requests) AS borough_requests
    FROM {{ ref('borough_complaint_trends') }}
    GROUP BY complaint_date
),

reconciliation AS (
    SELECT
        COALESCE(
            c.complaint_date,
            b.complaint_date
        ) AS complaint_date,

        c.citywide_requests,
        b.borough_requests

    FROM citywide c
    FULL OUTER JOIN borough_daily b
        ON c.complaint_date = b.complaint_date
)

SELECT
    complaint_date,
    citywide_requests,
    borough_requests,
    COALESCE(borough_requests, 0)
        - COALESCE(citywide_requests, 0) AS difference

FROM reconciliation

WHERE citywide_requests IS NULL
   OR borough_requests IS NULL
   OR citywide_requests != borough_requests
