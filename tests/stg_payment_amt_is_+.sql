select 
    order_id,
    sum(amount) as order_total
from {{ ref('stg_payments') }}
group by 1
having order_total < 0