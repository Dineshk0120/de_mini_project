with raw_country_region as (
    select 
       TRIM(CUSTOMER_ID) as CUSTOMER_ID,
       TRIM(COUNTRY) as COUNTRY,
       TRIM(REGION)  as REGION
    from {{ source('hotel_booking', 'country_region') }}
    where "CUSTOMER_ID" is not null
)

select * from raw_country_region