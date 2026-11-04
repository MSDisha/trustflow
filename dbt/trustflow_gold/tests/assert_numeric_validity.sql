
-- TRUSTFLOW GOLD — NUMERIC VALIDITY
-- Verify that complaint counts are present and nonnegative.
-- PASS = zero rows returned.

WITH invalid_rows AS (

    SELECT 'daily_complaints' AS model_name
    FROM {{ ref('daily_complaints') }}
    WHERE total_requests IS NULL OR total_requests < 0
       OR open_requests IS NULL OR open_requests < 0
       OR closed_requests IS NULL OR closed_requests < 0
       OR other_status_requests IS NULL OR other_status_requests < 0

    UNION ALL

    SELECT 'borough_metrics'
    FROM {{ ref('borough_metrics') }}
    WHERE total_requests IS NULL OR total_requests < 0
       OR open_requests IS NULL OR open_requests < 0
       OR closed_requests IS NULL OR closed_requests < 0
       OR other_status_requests IS NULL OR other_status_requests < 0

    UNION ALL

    SELECT 'complaint_type_metrics'
    FROM {{ ref('complaint_type_metrics') }}
    WHERE total_requests IS NULL OR total_requests < 0
       OR open_requests IS NULL OR open_requests < 0
       OR closed_requests IS NULL OR closed_requests < 0
       OR other_status_requests IS NULL OR other_status_requests < 0

    UNION ALL

    SELECT 'agency_performance'
    FROM {{ ref('agency_performance') }}
    WHERE total_requests IS NULL OR total_requests < 0
       OR open_requests IS NULL OR open_requests < 0
       OR closed_requests IS NULL OR closed_requests < 0
       OR other_status_requests IS NULL OR other_status_requests < 0

    UNION ALL

    SELECT 'borough_complaint_trends'
    FROM {{ ref('borough_complaint_trends') }}
    WHERE total_requests IS NULL OR total_requests < 0
       OR open_requests IS NULL OR open_requests < 0
       OR closed_requests IS NULL OR closed_requests < 0
       OR other_status_requests IS NULL OR other_status_requests < 0
)

SELECT
    model_name,
    COUNT(*) AS invalid_count
FROM invalid_rows
GROUP BY model_name
