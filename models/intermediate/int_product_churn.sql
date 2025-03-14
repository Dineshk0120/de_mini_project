
WITH product_churn AS (
    SELECT
        customer_id,
        payment_month,
        product_id,
        MAX(payment_month) OVER(PARTITION BY customer_id) AS latest_month
    FROM
        {{ ref('stg_transactions') }}
    WHERE
        payment_month IS NOT NULL
    GROUP BY
        customer_id, payment_month, product_id
),
is_churned AS (
    SELECT
        customer_id,
        payment_month,
        product_id,
        (CASE
            WHEN
                payment_month IS NOT NULL
                AND
                DATEDIFF(MONTH, MAX(payment_month),latest_month) > 12
            THEN 1
            ELSE 0
        END) AS churned_status
    FROM
        product_churn
    GROUP BY
        customer_id,
        payment_month,
        product_id,
        latest_month
),
most_churned AS (
    SELECT
        product_id,
        COUNT(churned_status) AS churned_product_count
    FROM
        is_churned
    WHERE
        churned_status = 1
    GROUP BY
        PRODUCT_ID
)
 
SELECT
    *
FROM
    most_churned
ORDER BY
    churned_product_count DESC
 