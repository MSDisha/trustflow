
{{ config(materialized='table') }}

SELECT
    CAST(created_date AS DATE) AS complaint_date,
    COUNT(*) AS total_requests,
    COUNT(DISTINCT unique_key) AS unique_requests
FROM {{ source('nyc_311_silver', 'nyc_311_requests') }}
WHERE created_date IS NOT NULL
GROUP BY CAST(created_date AS DATE)