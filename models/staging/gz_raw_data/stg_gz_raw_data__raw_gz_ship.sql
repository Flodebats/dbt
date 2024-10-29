WITH 
    source AS (
        SELECT * 
        FROM {{ source("gz_raw_data", "raw_gz_ship") }}
    ), 
    renamed AS (
        SELECT 
            orders_id, 
            shipping_fee, 
            logcost,
            CAST(ship_cost AS FLOAT64) AS ship_cost
        FROM source
    )
SELECT *
FROM renamed
