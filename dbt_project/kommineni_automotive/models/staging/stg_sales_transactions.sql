-- ============================================================
-- Silver Layer: stg_sales_transactions
-- Cleans and standardizes the raw sales transactions table
-- ============================================================

WITH source AS (

    SELECT * FROM {{ source('bronze', 'sales_transactions') }}

),

cleaned AS (

    SELECT
        transaction_id,
        vehicle_id,
        employee_id,
        location_id,

        -- Round sale price to 2 decimal places
        ROUND(sale_price, 2) AS sale_price,

        -- Cast to proper timestamp type
        CAST(sale_date AS TIMESTAMP) AS sale_date,

        -- Extract useful date parts for easier querying later
        CAST(sale_date AS DATE) AS sale_date_only,
        DATE_TRUNC('month', CAST(sale_date AS TIMESTAMP)) AS sale_month,
        EXTRACT(DOW FROM CAST(sale_date AS TIMESTAMP)) AS day_of_week,

        -- Standardize boolean
        CAST(financing_approved AS BOOLEAN) AS financing_approved,

        -- Flag high value sales (above 50k)
        CASE
            WHEN sale_price > 50000 THEN TRUE
            ELSE FALSE
        END AS is_high_value_sale,

        _ingested_at,
        _source_file

    FROM source

),

final AS (

    SELECT * FROM cleaned

    -- Remove invalid rows
    -- Sale price must be positive
    -- All ID fields must exist
    WHERE sale_price > 0
    AND transaction_id IS NOT NULL
    AND employee_id IS NOT NULL
    AND location_id IS NOT NULL

)

SELECT * FROM final