with

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select * from {{ ref('int_orders') }}

),

distinct_orders as (

    select
        distinct(order_id) as order_id,
        customer_id as customer_id
    from orders
    group by 1,2

),

orders_per_customer as (
    select
        customer_id,
        array_agg(distinct_orders.order_id) over(
            partition by distinct_orders.customer_id) as order_ids
from distinct_orders

),

final as (

    select

        orders.*,

        customers.surname,
        customers.givenname,

        -- Customer-level aggregations
        min(orders.order_date) over(
            partition by orders.customer_id
        ) as first_order_date,

        min(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as first_non_returned_order_date,

        max(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as most_recent_non_returned_order_date,

        count(*) over(
            partition by orders.customer_id
        ) as order_count,

        sum(if(orders.valid_order_date is not null, 1, 0)) over(
            partition by orders.customer_id
        ) as non_returned_order_count,

        orders_per_customer.order_ids as order_ids,

        sum(if(orders.valid_order_date is not null, orders.order_value_dollars, 0)) over(
            partition by orders.customer_id
        ) as total_lifetime_value

    from orders
    inner join customers
        on orders.customer_id = customers.customer_id
    inner join orders_per_customer
        on orders_per_customer.customer_id = customers.customer_id
    

)

select * from final