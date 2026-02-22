-- ============================================================
-- Gold Layer: salesperson_leaderboard
-- Answers: Who are the top performers this week?
-- ============================================================

WITH sales AS (

    SELECT * FROM {{ ref('stg_sales_transactions') }}

),

employees AS (

    SELECT * FROM {{ ref('stg_employees') }}

),

locations AS (

    SELECT * FROM {{ ref('stg_locations') }}

),

-- Get sales from the last 7 days only
recent_sales AS (

    SELECT * FROM sales
    WHERE sale_date_only >= CURRENT_DATE - INTERVAL '7 days'

),

performance AS (

    SELECT
        e.employee_id,
        e.full_name,
        e.location_id,
        l.city,
        e.commission_rate,

        -- Deals closed this week
        COUNT(s.transaction_id) AS deals_closed,

        -- Total revenue generated this week
        ROUND(SUM(s.sale_price), 2) AS revenue_generated,

        -- Commission earned this week
        ROUND(SUM(s.sale_price) * e.commission_rate, 2) AS commission_earned,

        -- Average deal size
        ROUND(AVG(s.sale_price), 2) AS avg_deal_size,

        -- Best single sale
        ROUND(MAX(s.sale_price), 2) AS best_single_sale

    FROM employees e
    LEFT JOIN recent_sales s ON e.employee_id = s.employee_id
    LEFT JOIN locations l ON e.location_id = l.location_id

    WHERE e.is_salesperson = TRUE

    GROUP BY
        e.employee_id,
        e.full_name,
        e.location_id,
        l.city,
        e.commission_rate

),

ranked AS (

    SELECT
        *,

        -- Rank within their own location
        RANK() OVER (
            PARTITION BY location_id
            ORDER BY deals_closed DESC, revenue_generated DESC
        ) AS location_rank,

        -- Rank across the entire company
        RANK() OVER (
            ORDER BY deals_closed DESC, revenue_generated DESC
        ) AS company_rank

    FROM performance

)

SELECT * FROM ranked
ORDER BY company_rank