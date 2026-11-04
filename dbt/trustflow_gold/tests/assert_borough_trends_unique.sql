
-- TRUSTFLOW GOLD — BOROUGH TREND UNIQUENESS
-- Each combination of complaint_date and borough
-- must appear only once.
-- dbt tests fail when this query returns any rows.

SELECT
    complaint_date,
    borough,
    COUNT(*) AS duplicate_count

FROM {{ ref('borough_complaint_trends') }}

GROUP BY
    complaint_date,
    borough

HAVING COUNT(*) > 1
