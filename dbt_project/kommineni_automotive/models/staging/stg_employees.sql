-- ============================================================
-- Silver Layer: stg_employees
-- Cleans and standardizes the raw employees table
-- ============================================================

WITH source AS (

    -- Pull raw data from bronze layer
    -- source() tells dbt where to find the bronze table
    SELECT * FROM {{ source('bronze', 'employees') }}

),

cleaned AS (

    SELECT
        employee_id,

        -- Clean employee names
        -- Some names from Faker have titles like "PhD" or "MD"
        -- We strip those out here using REGEXP_REPLACE
        -- This is a real data quality issue you would see in CRM data
        REGEXP_REPLACE(
            full_name,
            ' (PhD|MD|DDS|DVM|Jr\.|Sr\.|II|III)$',
            ''
        ) AS full_name,

        -- Standardize role values to lowercase
        LOWER(role) AS role,

        location_id,

        -- Cast hire_date to proper date type
        CAST(hire_date AS DATE) AS hire_date,

        -- Round commission rate to 4 decimal places
        ROUND(commission_rate, 4) AS commission_rate,

        -- Add a flag for salespeople vs technicians
        -- This makes filtering easier in gold layer
        CASE 
            WHEN LOWER(role) = 'salesperson' THEN TRUE
            ELSE FALSE
        END AS is_salesperson,

        -- Keep metadata columns from bronze
        _ingested_at,
        _source_file

    FROM source

),

-- Final quality filter
-- Remove any rows where employee_id or location_id is null
-- These would break joins downstream
final AS (

    SELECT * FROM cleaned
    WHERE employee_id IS NOT NULL
    AND location_id IS NOT NULL

)

SELECT * FROM final