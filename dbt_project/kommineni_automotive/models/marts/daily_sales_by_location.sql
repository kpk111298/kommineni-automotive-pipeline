-- ============================================================
-- Gold Layer: daily_sales_by_location
-- Answers: How many cars sold and revenue per location per day?
-- This is what the CEO looks at every morning
-- ============================================================

WITH sales AS (

    SELECT * FROM {{ ref('stg_sales_transactions') }}

),

locations AS (

    SELECT * FROM {{ ref('stg_locations') }}

),

daily_summary AS (

    SELECT
        s.sale_date_only AS sale_date,
        s.location_id,
        l.city,
        l.state,
        l.monthly_target,
        l.manager_name,

        -- How many cars sold this day at this location
        COUNT(s.transaction_id) AS units_sold,

        -- Total revenue this day at this location
        ROUND(SUM(s.sale_price), 2) AS daily_revenue,

        -- Average sale price this day
        ROUND(AVG(s.sale_price), 2) AS avg_sale_price,

        -- How many were financed
        SUM(CASE WHEN s.financing_approved THEN 1 ELSE 0 END) AS financed_deals,

        -- High value sales count
        SUM(CASE WHEN s.is_high_value_sale THEN 1 ELSE 0 END) AS high_value_sales

    FROM sales s
    LEFT JOIN locations l ON s.location_id = l.location_id

    GROUP BY
        s.sale_date_only,
        s.location_id,
        l.city,
        l.state,
        l.monthly_target,
        l.manager_name

)

SELECT * FROM daily_summary
ORDER BY sale_date DESC, daily_revenue DESC