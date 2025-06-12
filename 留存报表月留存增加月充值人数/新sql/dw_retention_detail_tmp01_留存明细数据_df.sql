---------------------------------------------
-- File: dw_retention_detail_tmp01_留存明细数据_df.sql
-- Time: 2025/6/9 15:09
-- User: xiaoj
-- Description:  
---------------------------------------------
SET timezone ='UTC';
---------------------------------------------
-- 更新
---------------------------------------------
drop table if exists tmp.dw_retention_detail_tmp01;
CREATE TABLE if not exists tmp.dw_retention_detail_tmp01 (
    active_date date,
    uid bigint,
    register_date date,
    country_code text,
    country_name text,
    area text,
    is_campaign bigint,
    os text,
    lang_name text,
    register_week text,
    active_week text,
    week_start date,
    week_end date,
    week_day text,
    register_month text,
    active_month text,
    is_paid integer,
    user_source text,
    pay_amt bigint
);
---------------------------------------------
-- 更新
---------------------------------------------
-- 留存明细数据
    -- 日周月都依赖明细数据
TRUNCATE TABLE tmp.dw_retention_detail_tmp01;
INSERT INTO tmp.dw_retention_detail_tmp01
--
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
, tmp_user_paid as(
	select
        uid
        ,sum(pay_amt) as pay_amt        -- 付款总金额
        ,min(d_date) as first_pay_date  -- 第一次付款日期
	from tmp_pay
	group by uid
	having sum(pay_amt)>0               -- 付款金额大于0
)
select
    ta.d_date as active_date
    , ta.uid
    , ta.reg_date as register_date
    , ta.reg_country as country_code
    , ta.reg_country_name as country_name
    , ta.reg_area as area
    , ta.is_campaign
    , ta.reg_os as os
    , ta.lang_name

    , TO_CHAR(ta.reg_date::timestamp, 'IYYY"-"IW') AS register_week
    , TO_CHAR(ta.d_date::timestamp, 'IYYY"-"IW') AS active_week
    , (date_trunc('week', ta.d_date::timestamp))::date week_start
    , (date_trunc('week', ta.d_date::timestamp) + interval '6 days')::date week_end
    , concat((date_trunc('week', ta.d_date::timestamp))::date, ' ~ ', (date_trunc('week', ta.d_date::timestamp) + interval '6 days')::date) week_day
    , TO_CHAR(ta.reg_date::timestamp, 'YYYY-MM') AS register_month
    , TO_CHAR(ta.d_date::timestamp, 'YYYY-MM') AS active_month
    , if(pd.uid is null ,0,1) as is_paid                                         -- 判断是否是付费用户
    , u.user_source
    , tp.pay_amt                                                                -- 当日付款金额
from public.dwd_user_active ta
left join tmp_user_paid pd on ta.uid=pd.uid and ta.d_date>=pd.first_pay_date    -- 在第一次付款以后就变成付费用户
LEFT JOIN public.dwd_user_info u on ta.uid=u.uid::BIGINT                        -- 补充user_source
left join tmp_pay tp on ta.uid=tp.uid and ta.d_date=tp.d_date                   -- 判断活跃当天是否付款
;

