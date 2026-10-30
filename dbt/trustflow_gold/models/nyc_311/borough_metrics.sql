
{{ config(materialized='table') }}

SELECT
    borough,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN LOWER(status) = 'open' THEN 1 ELSE 0 END) AS open_requests,
    SUM(CASE WHEN LOWER(status) = 'closed' THEN 1 ELSE 0 END) AS closed_requests,
    SUM(
        CASE
            WHEN status IS NULL
              OR LOWER(status) NOT IN ('open', 'closed')
            THEN 1 ELSE 0
        END
    ) AS other_status_requests
FROM {{ source('nyc_311_silver', 'nyc_311_requests') }}
GROUP BY borough
