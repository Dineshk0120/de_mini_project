with raw_transactions as (
    select 
        CAST(TRIM(customer_id) AS INT) AS customer_id,
        TRIM(product_id) AS PRODUCT_ID,
        CAST(TRIM(payment_month) AS DATE) AS payment_month,
        CAST(TRIM(revenue_type) AS INT) AS revenue_type,
        CAST(case when revenue < 0 then 0 else revenue end AS DECIMAL) AS revenue,
        CAST(quantity AS DECIMAL) AS quantity
    from {{ source('hotel_booking', 'transactions') }}
    where customer_id is not null
)
select * from raw_transactions