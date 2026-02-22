-- ============================================================
-- Silver Layer: stg_service_jobs
-- ============================================================

WITH source AS (

    SELECT * FROM {{ source('bronze', 'service_jobs') }}

),

cleaned AS (

    SELECT
        job_id,
        vehicle_id,
        technician_id,
        location_id,
        LOWER(job_type) AS job_type,
        ROUND(estimated_hours, 2) AS estimated_hours,
        ROUND(actual_hours, 2) AS actual_hours,
        ROUND(labor_revenue, 2) AS labor_revenue,
        CAST(job_date AS TIMESTAMP) AS job_date,
        CAST(job_date AS DATE) AS job_date_only,
        DATE_TRUNC('month', CAST(job_date AS TIMESTAMP)) AS job_month,

        -- Flag jobs that took significantly longer than estimated
        -- This is a real operational metric service managers track
        CASE
            WHEN actual_hours > estimated_hours * 1.25 THEN TRUE
            ELSE FALSE
        END AS is_overrun,

        -- Efficiency ratio: how close to estimate did they finish
        ROUND(actual_hours / NULLIF(estimated_hours, 0), 2) AS efficiency_ratio,

        _ingested_at,
        _source_file

    FROM source

    WHERE job_id IS NOT NULL
    AND actual_hours > 0
    AND labor_revenue > 0

)

SELECT * FROM cleaned