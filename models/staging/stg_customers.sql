{{ config(
    materialized='table'
) }}
with cte1 as(
    select * from
    {{source('hotel_booking','customers')}}
     where COMPANY is not null and CUSTOMER_ID is not null
  )
  ,cte2 as(
        select * ,
        ROW_NUMBER() OVER (partition by CUSTOMER_ID order by CUSTOMER_ID )as rn
       from cte1
  )
select  CUSTOMER_ID,TRIM(CUSTOMER_NAME) as CUSTOMER_NAME ,TRIM(COMPANY) as COMPANY
from cte2 where rn=1