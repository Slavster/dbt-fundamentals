select
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    amount,
    created as created_at
from dbt-tutorial.stripe.payment