
-- TRUSTFLOW — FRESHNESS SINGLE ROW
-- Exactly one freshness summary row must exist.
-- PASS = zero rows returned.

SELECT
    COUNT(*) AS actual_rows
FROM {{ ref('nyc_311_freshness_health') }}
HAVING COUNT(*) != 1
