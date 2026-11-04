
-- TRUSTFLOW — HEALTH NUMERIC VALIDITY
-- Validate counts and percentages for every audited batch.
-- PASS = zero rows returned.

SELECT
    batch_id,
    batch_type,
    source_count,
    valid_count,
    quarantine_count,
    valid_pct,
    quarantine_pct

FROM {{ ref('nyc_311_quality_health') }}

WHERE
    -- Counts must be present and nonnegative.
    source_count IS NULL
    OR valid_count IS NULL
    OR quarantine_count IS NULL

    OR source_count < 0
    OR valid_count < 0
    OR quarantine_count < 0

    -- Neither classified count can exceed its input.
    OR valid_count > source_count
    OR quarantine_count > source_count

    -- Percentages must be between 0 and 100.
    OR valid_pct < 0
    OR valid_pct > 100
    OR quarantine_pct < 0
    OR quarantine_pct > 100

    -- Positive source counts require percentages.
    OR (
        source_count > 0
        AND (
            valid_pct IS NULL
            OR quarantine_pct IS NULL
            OR ABS(valid_pct + quarantine_pct - 100) > 0.02
        )
    )

    -- Empty batches should have zero valid/quarantine records
    -- and NULL percentages under our model definition.
    OR (
        source_count = 0
        AND (
            valid_count != 0
            OR quarantine_count != 0
            OR valid_pct IS NOT NULL
            OR quarantine_pct IS NOT NULL
        )
    )
