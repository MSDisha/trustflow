
-- TRUSTFLOW — HEALTH RECONCILIATION
-- Verify batch counts and quality metrics agree.
-- PASS = zero rows returned.

WITH reconciliation AS (

    SELECT
        b.batch_id,

        b.source_count AS batch_source_count,
        b.valid_count AS batch_valid_count,
        b.quarantine_count AS batch_quarantine_count,

        q.source_count AS quality_source_count,
        q.valid_count AS quality_valid_count,
        q.quarantine_count AS quality_quarantine_count,

        q.reconciliation_difference,
        q.reconciliation_status

    FROM {{ ref('nyc_311_batch_health') }} b

    FULL OUTER JOIN {{ ref('nyc_311_quality_health') }} q
        ON b.batch_id = q.batch_id

)

SELECT *
FROM reconciliation

WHERE batch_source_count IS NULL
   OR quality_source_count IS NULL

   OR batch_valid_count IS NULL
   OR quality_valid_count IS NULL

   OR batch_quarantine_count IS NULL
   OR quality_quarantine_count IS NULL

   OR batch_source_count != quality_source_count
   OR batch_valid_count != quality_valid_count
   OR batch_quarantine_count != quality_quarantine_count

   OR batch_source_count !=
       batch_valid_count + batch_quarantine_count

   OR reconciliation_difference IS NULL
   OR reconciliation_difference != 0
   OR reconciliation_status != 'MATCHED'
   OR reconciliation_status IS NULL
