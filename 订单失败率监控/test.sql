select
    order_num,
    count(order_num) as order_count
from all_order_log
where  order_date:: date = '2025-03-22'
group by order_num