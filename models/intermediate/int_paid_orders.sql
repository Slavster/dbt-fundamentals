with base_orders as (

    select * 
    from {{ source('jaffle_shop', 'orders') }}
),

base_payments as (

    select *
    from {{ source('stripe', 'payment') }}

),


payments as (

    select 
        ORDERID as order_id,
        max(CREATED) as payment_finalized_date,
        sum(AMOUNT) / 100.0 as total_amount_paid
    from base_payments
    where STATUS <> 'fail'
    group by 1

),

paid_orders as (
    
    select 
        Orders.ID as order_id,
        Orders.USER_ID	as customer_id,
        Orders.ORDER_DATE AS order_placed_at,
        Orders.STATUS AS order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,
    FROM base_orders as Orders
    left join payments ON orders.ID = payments.order_id 

),

order_clv_add as (

    select
        * ,
        sum(total_amount_paid) over(
            partition by customer_id
            order by order_placed_at
        ) as customer_lifetime_value
    from paid_orders as p

)

select
    *
from order_clv_add