-- ============================================================
-- Silver Layer: stg_locations
-- ============================================================

WITH source AS (

    SELECT * FROM {{ source('bronze', 'locations') }}

),

cleaned AS (

    SELECT
        location_id,
       CONCAT(UPPER(SUBSTR(city, 1, 1)), LOWER(SUBSTR(city, 2))) AS city,
        UPPER(state) AS state,
        manager_name,
        CAST(monthly_target AS DECIMAL(12,2)) AS monthly_target,
        CAST(opened_date AS DATE) AS opened_date,
        _ingested_at,
        _source_file

    FROM source

)

SELECT * FROM cleaned