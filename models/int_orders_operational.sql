WITH sales AS (
SELECT * 
FROM {{ ref('int_sales_margin') }}
 ), 
ship AS (
SELECT *
FROM {{ ref('stg_gz_raw_data__raw_gz_ship') }}
),
combined AS (
SELECT sales.orders_id,
sales.quantity,
sales.revenue,
sales.quantity * CAST(sales.margin AS FLOAT64) + CAST(ship.shipping_fee AS FLOAT64) - CAST(ship.logcost AS FLOAT64) - CAST(ship.ship_cost AS FLOAT64)AS Operational_margin
FROM sales
INNER JOIN ship
ON sales.orders_id = ship.orders_id
)
SELECT *
FROM combined