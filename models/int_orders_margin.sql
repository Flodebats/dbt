WITH sales AS (
SELECT * 
FROM {{ ref('stg_gz_raw_data__raw_gz_sales') }}
 ), 
products AS (
SELECT *
FROM {{ ref('stg_gz_raw_data__raw_gz_product') }}
),
combined AS (
SELECT sales.orders_id,
date_date,
sales.quantity,
sales.revenue,
sales.quantity * CAST(products.purchse_price AS FLOAT64) AS purchase_cost,  
CAST(sales.revenue AS FLOAT64) - (sales.quantity * CAST(products.purchse_price AS FLOAT64)) AS margin  
FROM sales
INNER JOIN products
ON sales.products_id = products.products_id
 )
SELECT *
FROM combined