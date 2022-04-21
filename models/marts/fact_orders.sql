select
    o.order_id,
    o.customer_id,
    p.amount
from {{ ref('stg_orders') }} as o
left join {{ ref('stg_payments') }} as p
    on o.order_id = p.order_id
    and p.status = 'success'