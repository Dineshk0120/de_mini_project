SELECT
    t.product_id,
    p.product_family,
    SUM(revenue) AS total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM {{ ref('stg_transactions') }} t inner join {{ ref('stg_products') }} p on t.product_id = p.product_id
group by t.product_id, p.product_family
ORDER BY total_revenue DESC