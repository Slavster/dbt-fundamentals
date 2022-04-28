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

customer_orders as (

    select

        orders.*,
        customers.full_name,
        customers.surname,
        customers.givenname,

        -- Customer-level aggregations
        min(orders.order_date) over(
            partition by orders.customer_id
        ) as customer_first_order_date,

        min(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as customer_first_non_returned_order_date,

        max(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as customer_most_recent_non_returned_order_date,

        count(*) over(
            partition by orders.customer_id
        ) as customer_order_count,

        sum(if(orders.valid_order_date is not null, 1, 0)) over(
            partition by orders.customer_id
        ) as customer_non_returned_order_count,

        sum(if(orders.valid_order_date is not null, orders.order_value_dollars, 0)) over(
            partition by orders.customer_id
        ) as customer_total_lifetime_value,

        orders_per_customer.order_ids as customer_order_ids

    from orders
    inner join customers
        on orders.customer_id = customers.customer_id
    inner join orders_per_customer
        on orders_per_customer.customer_id = customers.customer_id
    
),

add_avg_order_values as (

  select

    *,

    customer_total_lifetime_value / customer_non_returned_order_count 
    as customer_avg_non_returned_order_value

  from customer_orders

),

final as (

  select 

    order_id,
    customer_id,
    surname,
    givenname,
    customer_first_order_date as first_order_date,
    customer_order_count as order_count,
    customer_total_lifetime_value as total_lifetime_value,
    order_value_dollars,
    order_status,
    payment_status

  from add_avg_order_values

)

select * from final