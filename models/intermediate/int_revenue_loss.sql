WITH monthly_revenue AS (
    SELECT
        product_id,
        YEAR(payment_month) AS year,
        MONTH(payment_month) AS month,
        ROUND(SUM(revenue), 2) AS current_revenue
    FROM
        {{ ref('stg_transactions') }}
    GROUP BY
        product_id, year, month
    ORDER BY
        product_id, year, month
),

revenue_comparison AS (
    SELECT
        *,
        LAG(current_revenue, 1) OVER (PARTITION BY product_id ORDER BY year, month) AS previous_revenue
    FROM
        monthly_revenue
)
SELECT
    *,
    CASE
        WHEN previous_revenue > current_revenue THEN 'contraction'
        ELSE 'expansion'
    END AS revenue_trend,
    CASE
        WHEN previous_revenue > current_revenue THEN 'LOSS'
        ELSE 'PROFIT'
    END AS profit_or_loss,
    CASE
        WHEN previous_revenue > current_revenue THEN previous_revenue - current_revenue
        ELSE current_revenue - previous_revenue
    END AS amount
FROM
    revenue_comparison