WITH customer_revenue AS (
    SELECT
        customer_id,
        SUM(revenue) AS total_revenue
    FROM {{ ref('stg_transactions') }} 
    GROUP BY customer_id
)
 
SELECT
    cr.customer_id,
    c.customer_name,
    cr.total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM customer_revenue AS cr inner join {{ ref('stg_customers') }} AS c on cr.customer_id =c.CUSTOMER_ID

