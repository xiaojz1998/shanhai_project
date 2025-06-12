---------------------------------------------
-- File: 验收问题.sql
-- Time: 2025/6/11 18:31
-- User: xiaoj
-- Description:  
---------------------------------------------
-- 测试dw_retention_detail_tmp01 和 all_order_log
select
    count(distinct uid)
from tmp.dw_retention_detail_tmp01
where TO_CHAR(active_date::timestamp, 'YYYY-MM') = '2025-04' and pay_amt > 0

with tmp_pay as (
    -- 维度：日期 uid
	-- 度量字段：每日uid 付款总金额
    select
        to_timestamp(created_at):: date as d_date   -- 交易日期
        , o.uid                                     -- 交易uid
        , sum(o.money) as  pay_amt                  -- 每个交易日期的付款金额
    from public.all_order_log o
    where 1=1
        and o.environment = 1
        and o.os in('android','ios')
        and o.status = 1
        and o.created_date>=20240701
    group by to_timestamp(created_at):: date , o.uid
    -- exclude refund?
)
select
    count(distinct uid)
from tmp_pay
where TO_CHAR(d_date::timestamp, 'YYYY-MM') = '2025-04'