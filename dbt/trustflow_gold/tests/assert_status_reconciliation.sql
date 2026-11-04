
-- TRUSTFLOW GOLD — STATUS RECONCILIATION
-- Check that Open + Closed + Other = Total.
-- A dbt test passes when this query returns zero rows.

WITH checks AS (

    SELECT
        'daily_complaints' AS model_name,
        COUNT(*) AS failed_rows
    FROM {{ ref('daily_complaints') }}
    WHERE total_requests IS NULL
       OR open_requests IS NULL
       OR closed_requests IS NULL
       OR other_status_requests IS NULL
       OR total_requests !=
          open_requests + closed_requests + other_status_requests

    UNION ALL

    SELECT
        'borough_metrics',
        COUNT(*)
    FROM {{ ref('borough_metrics') }}
    WHERE total_requests IS NULL
       OR open_requests IS NULL
       OR closed_requests IS NULL
       OR other_status_requests IS NULL
       OR total_requests !=
          open_requests + closed_requests + other_status_requests

    UNION ALL

    SELECT
        'complaint_type_metrics',
        COUNT(*)
    FROM {{ ref('complaint_type_metrics') }}
    WHERE total_requests IS NULL
       OR open_requests IS NULL
       OR closed_requests IS NULL
       OR other_status_requests IS NULL
       OR total_requests !=
          open_requests + closed_requests + other_status_requests

    UNION ALL

    SELECT
        'agency_performance',
        COUNT(*)
    FROM {{ ref('agency_performance') }}
    WHERE total_requests IS NULL
       OR open_requests IS NULL
       OR closed_requests IS NULL
       OR other_status_requests IS NULL
       OR total_requests !=
          open_requests + closed_requests + other_status_requests

    UNION ALL

    SELECT
        'borough_complaint_trends',
        COUNT(*)
    FROM {{ ref('borough_complaint_trends') }}
    WHERE total_requests IS NULL
       OR open_requests IS NULL
       OR closed_requests IS NULL
       OR other_status_requests IS NULL
       OR total_requests !=
          open_requests + closed_requests + other_status_requests
)

SELECT *
FROM checks
WHERE failed_rows > 0
