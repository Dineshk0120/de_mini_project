with raw_products as (
    select 
       TRIM(PRODUCT_ID) as PRODUCT_ID,
       TRIM(PRODUCT_FAMILY) as PRODUCT_FAMILY,
       TRIM(PRODUCT_SUB_FAMILY) as PRODUCT_SUB_FAMILY
    from {{ source('hotel_booking', 'products') }}
    where "PRODUCT_ID" is not null
)
select * from raw_products