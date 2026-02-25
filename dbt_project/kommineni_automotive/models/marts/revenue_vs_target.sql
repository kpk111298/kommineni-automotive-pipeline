-- ============================================================
-- Gold Layer: revenue_vs_target
-- Answers: How close is each location to hitting monthly target?
-- ============================================================

WITH sales AS (

    SELECT * FROM {{ ref('stg_sales_transactions') }}

),

locations AS (

    SELECT * FROM {{ ref('stg_locations') }}

),

-- Get only this month's sales
monthly_sales AS (

    SELECT * FROM sales
    WHERE sale_month = DATE_TRUNC('month', CURRENT_DATE)

),

location_revenue AS (

    SELECT
        l.location_id,
        l.city,
        l.state,
        l.manager_name,
        l.monthly_target,

        -- Total revenue so far this month
        ROUND(COALESCE(SUM(s.sale_price), 0), 2) AS revenue_to_date,

        -- How many days into the month are we
        EXTRACT(DAY FROM CURRENT_DATE) AS days_elapsed,

        -- How many total days in this month
        EXTRACT(DAY FROM 
            (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')
        ) AS days_in_month

    FROM locations l
    LEFT JOIN monthly_sales s ON l.location_id = s.location_id

    GROUP BY
        l.location_id,
        l.city,
        l.state,
        l.manager_name,
        l.monthly_target

),

final AS (

    SELECT
        *,

        -- How much revenue still needed to hit target
        ROUND(monthly_target - revenue_to_date, 2) AS revenue_remaining,

        -- Percentage of target achieved so far
        ROUND((revenue_to_date / monthly_target) * 100, 2) AS pct_of_target,

        -- Daily run rate needed to hit target
        ROUND(
            (monthly_target - revenue_to_date) / 
            NULLIF(days_in_month - days_elapsed, 0),
        2) AS daily_rate_needed,

        -- Are they on track?
        CASE
            WHEN (revenue_to_date / monthly_target) * 100 >= 
                 (days_elapsed::FLOAT / days_in_month * 100)
            THEN 'On Track'
            ELSE 'Behind'
        END AS status

    FROM location_revenue

)

SELECT * FROM final
ORDER BY pct_of_target DESC