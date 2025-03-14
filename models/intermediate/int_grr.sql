WITH customer_transactions AS (
    SELECT
        customer_id,
        payment_month,
        SUM(revenue) AS curr_revenue
    FROM {{ ref('stg_transactions') }}
    WHERE payment_month <= DATEADD(MONTH, -12, (SELECT MAX(payment_month) FROM {{ ref('stg_transactions') }}))
    GROUP BY  customer_id,payment_month
),
 
previous_revenue AS (
    SELECT
        customer_id,
        curr_revenue,
        payment_month,
        LAG(curr_revenue) OVER (PARTITION BY customer_id ORDER BY payment_month) AS prev_revenue
    FROM customer_transactions
),
 
expansion_contraction AS (
    SELECT
        customer_id,
        payment_month,
        prev_revenue,
        curr_revenue,
        CASE
            WHEN curr_revenue > prev_revenue THEN curr_revenue - prev_revenue
            ELSE 0
        END AS expansion_revenue,
        CASE
            WHEN curr_revenue < prev_revenue THEN prev_revenue - curr_revenue
            ELSE 0
        END AS contraction_revenue
    FROM previous_revenue
)
 
SELECT
    customer_id,
    payment_month,
    curr_revenue,
    prev_revenue ,
    expansion_revenue,
    contraction_revenue,
    CASE
        WHEN prev_revenue = 0 THEN NULL  
        ELSE (curr_revenue - contraction_revenue) / prev_revenue
    END AS grr
FROM expansion_contraction