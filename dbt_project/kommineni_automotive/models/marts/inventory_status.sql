-- ============================================================
-- Gold Layer: inventory_status
-- Answers: What vehicles do we have available across locations?
-- ============================================================

WITH vehicles AS (

    SELECT * FROM {{ ref('stg_vehicles') }}

),

locations AS (

    SELECT * FROM {{ ref('stg_locations') }}

),

summary AS (

    SELECT
        v.location_id,
        l.city,
        l.state,
        v.make,
        v.status,

        COUNT(v.vehicle_id) AS vehicle_count,
        ROUND(AVG(v.list_price), 2) AS avg_list_price,
        ROUND(MIN(v.list_price), 2) AS min_price,
        ROUND(MAX(v.list_price), 2) AS max_price,

        SUM(CASE WHEN v.is_premium THEN 1 ELSE 0 END) AS premium_count

    FROM vehicles v
    LEFT JOIN locations l ON v.location_id = l.location_id

    GROUP BY
        v.location_id,
        l.city,
        l.state,
        v.make,
        v.status

)

SELECT * FROM summary
ORDER BY location_id, make, status