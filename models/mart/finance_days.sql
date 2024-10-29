WITH sales AS (
SELECT *
FROM {{ ref('stg_gz_raw_data__raw_gz_sales') }}
),
products AS (
SELECT *
FROM {{ ref('stg_gz_raw_data__raw_gz_product') }}
),
shipping AS (
SELECT *
FROM {{ ref('stg_gz_raw_data__raw_gz_ship') }}
),
combined AS ( SELECT
sales.date_date,
COUNT(DISTINCT sales.orders_id) AS total_transactions,
SUM(sales.revenue) AS total_revenue,
AVG(sales.revenue) AS average_basket,
SUM(sales.revenue - (sales.quantity * CAST(products.purchse_price AS FLOAT64)) - shipping.shipping_fee - shipping.logcost) AS operational_margin,
SUM(sales.quantity * CAST(products.purchse_price AS FLOAT64)) AS total_purchase_cost,
SUM(shipping.shipping_fee) AS total_shipping_fees,
SUM(shipping.logcost) AS total_log_costs,
SUM(sales.quantity) AS total_quantity_sold
FROM sales
INNER JOIN products ON sales.products_id = products.products_id
INNER JOIN shipping ON sales.orders_id = shipping.orders_id
GROUP BY sales.date_date
)
SELECT *
FROM combined
