------------------------------------------
-- file: 临时取数5-13.sql
-- author: xiaoj
-- time: 2025/5/13 17:16
-- description:
------------------------------------------
set timezone ='UTC-0';
with user_pay_info as (
    -- 用于判断是否是付费用户
    SELECT
        uid
    FROM all_order_log
    WHERE to_timestamp(created_at)::date >= current_date - 14
      AND environment = 1
      AND status = 1
    group by uid
),
tmp_user_active as (
    select
        d_date,
        uid
    from dwd_user_active
    where d_date >= current_date - 7
    group by d_date, uid
)
select
    t1.d_date as "日期",
    count(distinct t1.uid) as "活跃用户数量"
from tmp_user_active t1 left join user_pay_info t2 on t1.uid = t2.uid
where t2.uid is null
group by t1.d_date
