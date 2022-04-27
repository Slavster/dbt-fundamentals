select sum(amount) as total_revenue
from {{ ref('stg_payments') }}
where status = 'success'