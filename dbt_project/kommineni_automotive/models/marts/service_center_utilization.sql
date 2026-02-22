-- ============================================================
-- Gold Layer: service_center_utilization
-- Answers: How efficient is each service center this week?
-- ============================================================

WITH jobs AS (

    SELECT * FROM {{ ref('stg_service_jobs') }}

),

employees AS (

    SELECT * FROM {{ ref('stg_employees') }}

),

locations AS (

    SELECT * FROM {{ ref('stg_locations') }}

),

recent_jobs AS (

    SELECT * FROM jobs
    WHERE job_date_only >= CURRENT_DATE - INTERVAL '7 days'

),

technician_performance AS (

    SELECT
        e.employee_id AS technician_id,
        e.full_name AS technician_name,
        e.location_id,
        l.city,

        COUNT(j.job_id) AS jobs_completed,
        ROUND(SUM(j.labor_revenue), 2) AS total_revenue,
        ROUND(AVG(j.efficiency_ratio), 2) AS avg_efficiency,
        SUM(CASE WHEN j.is_overrun THEN 1 ELSE 0 END) AS overrun_jobs,
        ROUND(AVG(j.actual_hours), 2) AS avg_job_hours

    FROM employees e
    LEFT JOIN recent_jobs j ON e.employee_id = j.technician_id
    LEFT JOIN locations l ON e.location_id = l.location_id

    WHERE e.is_salesperson = FALSE

    GROUP BY
        e.employee_id,
        e.full_name,
        e.location_id,
        l.city

)

SELECT * FROM technician_performance
ORDER BY total_revenue DESC