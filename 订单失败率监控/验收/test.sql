----------------------------------------------------
-- 测试每一天订单的重复数量
----------------------------------------------------
select
    order_num,
    count(order_num) as order_count
from all_order_log
where  order_date:: date = '2025-03-22'
group by order_num;

----------------------------------------------------
-- 查看发起请求数
----------------------------------------------------
select
    sum(total_order_request_num),
    sum(total_failed_order_num),
    sum(total_order_num)
from dw_order_status_statistics
where order_date = '2025-04-08';
----------------------------------------------------
-- 查看成功充值订单数
----------------------------------------------------
select
    count(distinct case when all_order_log.status = 1 then order_num else null end) as order_count
from all_order_log
where  created_date::text:: date = '2025-04-12';

select
    count(distinct case when status = 1 and rn = 1 then order_num else null end) as order_count
from (
    select
        created_date,
        order_num,
        status,
        rn
    from (select
        created_date::text::date as created_date,
        case
            when os = 'ios' then 'IOS'
            when os = 'android' then '安卓'
            else '未知'
        end as os,
        uid,
        order_num,
        status,
        row_number() over (partition by order_num,created_date order by created_at desc ,case when status = 1 then 3
            when status = 3 then 4 else status end desc ) as rn
    from all_order_log
    where environment = 1 and created_date::text:: date >= '2025-03-22' ) t
    where rn = 1
) t
where  created_date::text:: date = '2025-04-12'

--------------------------------------------------------------------
-- 找到
--------------------------------------------------------------------
select
    *
from (
    select
    case when status = 1 and rn != 1 then order_num else null end as order_num,
    status,
    rn
from (
    select
        created_date,
        order_num,
        status,
        rn
    from (select
        created_date::text::date as created_date,
        case
            when os = 'ios' then 'IOS'
            when os = 'android' then '安卓'
            else '未知'
        end as os,
        uid,
        order_num,
        status,
        row_number() over (partition by order_num,created_date order by created_at desc  ,case when status = 1 then 3
            when status = 3 then 4 when status = 4 then 2 when status = 2 then 1 else status end desc ) as rn
    from all_order_log
    where environment = 1 and created_date::text:: date >= '2025-03-22' ) t
) t
where  created_date = '2025-04-11'
    ) t1
where t1.order_num is not null


-- 查看该订单状态
select status,created_at,updated_at
from all_order_log
where order_num = 'SH169338915775229952' and created_date::text::date = '2025-04-11'
order by created_at desc

