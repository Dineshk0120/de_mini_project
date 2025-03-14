WITH individual_month AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', PAYMENT_MONTH) AS transaction_month
    FROM {{ ref('stg_transactions') }}
),
 
max_month AS (
    SELECT MAX(transaction_month) AS latest_month FROM individual_month
),
 
customer_churn_status AS (
    SELECT
        i.customer_id,
        MAX(i.transaction_month) AS max_month,
        CASE
            WHEN MAX(i.transaction_month) <= DATEADD('month', -3, m.latest_month) THEN 'Churned'
            WHEN MIN(i.transaction_month) >= DATEADD('month', -3, m.latest_month) THEN 'New'
            ELSE 'Active'
        END AS churn_status
    FROM individual_month i
    CROSS JOIN max_month m
    GROUP BY i.customer_id, m.latest_month
)
 
SELECT 
    churn_status,
    COUNT(*) AS customer_count
FROM customer_churn_status
GROUP BY churn_status