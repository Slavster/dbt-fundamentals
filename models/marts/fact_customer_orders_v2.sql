with paid_orders as (

    select *
    from {{ ref('int_paid_orders') }}

),

customer_orders as (

    select *
    from {{ ref('int_customer_orders') }}

)

select
    p.*,
    co.customer_first_name,
    co.customer_last_name,
    -- indicated paid_orders
    ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
    CASE WHEN ( rank() over (
        partition by customer_id
        order by order_placed_at, order_id ) = 1
    ) THEN 'new' ELSE 'return' END as nvsr,
    first_value(order_placed_at) over (
        partition by customer_id
        order by order_placed_at
    ) as fdos
FROM paid_orders p
left join customer_orders as co USING (customer_id)
ORDER BY order_id