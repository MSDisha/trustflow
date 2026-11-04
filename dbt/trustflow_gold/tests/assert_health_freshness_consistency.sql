

-- TRUSTFLOW — FRESHNESS CONSISTENCY
-- Independently verify the freshness model against batch history.
-- PASS = zero rows returned.

WITH refresh_batches AS (

    SELECT
        batch_id,
        status,
        started_at,
        completed_at
    FROM {{ ref('nyc_311_batch_health') }}
    WHERE batch_type = 'refresh'

),

ranked_attempts AS (

    SELECT
        batch_id,
        status,
        completed_at,
        ROW_NUMBER() OVER (
            ORDER BY
                COALESCE(completed_at, started_at) DESC,
                batch_id DESC
        ) AS rn
    FROM refresh_batches

),

latest_attempt AS (

    SELECT
        batch_id,
        status,
        completed_at
    FROM ranked_attempts
    WHERE rn = 1

),

latest_success AS (

    SELECT
        MAX(completed_at) AS success_at
    FROM refresh_batches
    WHERE UPPER(TRIM(status)) = 'SUCCESS'
      AND completed_at IS NOT NULL

),

expected AS (

    SELECT
        a.batch_id,
        a.status,
        a.completed_at,
        s.success_at
    FROM latest_success s
    LEFT JOIN latest_attempt a ON TRUE

),

actual AS (

    SELECT *
    FROM {{ ref('nyc_311_freshness_health') }}

),

comparison AS (

    SELECT
        a.*,
        e.batch_id AS expected_batch_id,
        e.status AS expected_status,
        e.completed_at AS expected_attempt_completed_at,
        e.success_at AS expected_success_at

    FROM actual a
    FULL OUTER JOIN expected e ON TRUE

)

SELECT *
FROM comparison

WHERE
    -- Exactly one freshness evaluation is expected.
    evaluated_at IS NULL

    -- Latest attempt must match the audit.
    OR NOT (
        latest_attempt_batch_id <=> expected_batch_id
    )
    OR NOT (
        latest_attempt_status <=> expected_status
    )
    OR NOT (
        latest_attempt_completed_at
        <=> expected_attempt_completed_at
    )

    -- Latest successful completion must match the audit.
    OR NOT (
        latest_success_completed_at <=> expected_success_at
    )

    -- Threshold must be positive.
    OR freshness_threshold_hours IS NULL
    OR freshness_threshold_hours <= 0

    -- Verify hours since success using the evaluation time.
    OR (
        expected_success_at IS NULL
        AND hours_since_success IS NOT NULL
    )

    OR (
        expected_success_at IS NOT NULL
        AND (
            hours_since_success IS NULL
            OR ABS(
                hours_since_success -
                ROUND(
                    TIMESTAMPDIFF(
                        SECOND,
                        expected_success_at,
                        evaluated_at
                    ) / 3600.0,
                    2
                )
            ) > 0.02
        )
    )

    -- Verify classification.
    OR NOT (
        freshness_status <=>
        CASE
            WHEN expected_success_at IS NULL
                THEN 'NO_SUCCESSFUL_REFRESH'

            WHEN hours_since_success < 0
                THEN 'CLOCK_ERROR'

            WHEN hours_since_success > freshness_threshold_hours
                THEN 'STALE'

            ELSE 'FRESH'
        END
    )
