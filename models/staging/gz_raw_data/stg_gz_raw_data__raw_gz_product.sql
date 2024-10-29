WITH 
    source AS (
        SELECT * 
        FROM {{ source("gz_raw_data", "raw_gz_product") }}
    ), 
    renamed AS (
        SELECT 
            products_id, 
            CAST(purchse_price AS FLOAT64) AS purchase_price
        FROM source
    )
SELECT *
FROM renamed






