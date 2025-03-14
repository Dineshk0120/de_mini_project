WITH cross_sell AS (
    SELECT
        customer_id,
        COUNT(DISTINCT product_id)  AS product_count,
        SUM(revenue)  AS customer_spent
    FROM
        {{ref('stg_transactions')}}
    WHERE 
        revenue_type = 1
    GROUP BY
        customer_id
),
max_cross_sell AS (
    SELECT
        customer_id,
        product_count,
        customer_spent,
        RANK() OVER (ORDER BY product_count DESC, customer_spent DESC) AS ranK
    FROM cross_sell
    wHERE product_count > 1
)
SELECT
    *
FROM
    max_cross_sell
LIMIT 1
 
 