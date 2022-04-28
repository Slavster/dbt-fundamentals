with base_customers as (

    select *
    from {{ source('jaffle_shop', 'customers') }}

),

base_orders as (

    select * 
    from {{ source('jaffle_shop', 'orders') }}
)

select 
    C.ID as customer_id,
    C.FIRST_NAME as customer_first_name,
    C.LAST_NAME as customer_last_name,
    min(ORDER_DATE) as first_order_date,
    max(ORDER_DATE) as most_recent_order_date,
    count(ORDERS.ID) AS number_of_orders
from base_customers C 
left join base_orders as Orders
on orders.USER_ID = C.ID 
group by 1,2,3
