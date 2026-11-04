
-- TRUSTFLOW GOLD — RESOLUTION RECONCILIATION
-- Verify closed complaints are fully accounted for.
-- Reject negative or inconsistent resolution statistics.
-- PASS = query returns zero rows.

SELECT
    complaint_type,
    total_requests,
    closed_requests,
    measured_closed_requests,
    excluded_closed_requests,
    avg_resolution_hours,
    median_resolution_hours,
    p90_resolution_hours

FROM {{ ref('resolution_time_metrics') }}

WHERE
    -- Every closed request must be accounted for.
    closed_requests !=
        measured_closed_requests + excluded_closed_requests

    -- Counts must be valid and nonnegative.
    OR total_requests IS NULL
    OR closed_requests IS NULL
    OR measured_closed_requests IS NULL
    OR excluded_closed_requests IS NULL
    OR total_requests < 0
    OR closed_requests < 0
    OR measured_closed_requests < 0
    OR excluded_closed_requests < 0
    OR closed_requests > total_requests

    -- Measured closures must have valid statistics.
    OR (
        measured_closed_requests > 0
        AND (
            avg_resolution_hours IS NULL
            OR median_resolution_hours IS NULL
            OR p90_resolution_hours IS NULL
            OR avg_resolution_hours < 0
            OR median_resolution_hours < 0
            OR p90_resolution_hours < 0
        )
    )

    -- No measured closures means no duration statistics.
    OR (
        measured_closed_requests = 0
        AND (
            avg_resolution_hours IS NOT NULL
            OR median_resolution_hours IS NOT NULL
            OR p90_resolution_hours IS NOT NULL
        )
    )
