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
    CASE WHEN co.first_order_date = p.order_placed_at
        THEN 'new' ELSE 'return' END as nvsr,
    --order_clv as customer_lifetime_value,
    co.first_order_date as fdos
FROM paid_orders p
left join customer_orders as co USING (customer_id)
ORDER BY order_id