with daily as (

    select
        order_date,
        count (*) as order_num,
        {% 
            
            for order_status in 
            ['returned','completed','return_pending', 'shipped', 'placed']
        
        %}

            sum(case when status = '{{ order_status }}' 
                then 1 else 0 end) 
            as {{ order_status }}_total {{ ',' if not loop.last }} 
        
        {% endfor %}
    from {{ ref('stg_orders') }}
    group by 1
    
),

compared as (

    select 
        *,
        lag(order_num) over (order by order_date) as prev_day_orders
    from daily

)

select *
from compared
order by 1 desc