---------------------------------------------
-- File: dw_retention_detail_tmp01_留存明细数据_df.sql
-- Time: 2025/6/9 14:48
-- User: xiaoj
-- Description:  
---------------------------------------------
SET timezone ='UTC';

---------------------------------------------
-- 更新
---------------------------------------------
-- 留存明细数据
    -- 日周月都依赖明细数据
TRUNCATE TABLE tmp.dw_retention_detail_tmp01;
INSERT INTO tmp.dw_retention_detail_tmp01
--
with tmp_user_paid as(
	select
	o.uid
	,sum(pay_amt) as pay_amt        -- 付款总金额
	,min(o.created_date) as first_paydate   -- 第一次付款日期
	,to_date(min(o.created_date)::text,'yyyymmdd') as first_pay_date
	from(
	-- 维度：日期 uid
	-- 度量字段：每日uid 付款总金额
	select
	o.created_date,o.uid
	,sum(o.money) as  pay_amt
	from public.all_order_log o
	where 1=1 and o.environment = 1 and o.os in('android','ios') and o.status = 1
	and o.created_date>=20240701
	group by
	o.created_date,o.uid
    -- exclude refund?
	)o
	group by
	o.uid
	having sum(pay_amt)>0
)
select
ta.d_date as active_date
,ta.uid
,ta.reg_date as register_date
,ta.reg_country as country_code
,ta.reg_country_name as country_name
,ta.reg_area as area
,ta.is_campaign
,ta.reg_os as os
,ta.lang_name
    ,TO_CHAR(ta.reg_date::timestamp, 'IYYY"-"IW') AS register_week
    ,TO_CHAR(ta.d_date::timestamp, 'IYYY"-"IW') AS active_week
    ,(date_trunc('week', ta.d_date::timestamp))::date week_start
    ,(date_trunc('week', ta.d_date::timestamp) + interval '6 days')::date week_end
    ,concat((date_trunc('week', ta.d_date::timestamp))::date, ' ~ ', (date_trunc('week', ta.d_date::timestamp) + interval '6 days')::date) week_day
    ,TO_CHAR(ta.reg_date::timestamp, 'YYYY-MM') AS register_month
    ,TO_CHAR(ta.d_date::timestamp, 'YYYY-MM') AS active_month
,if(pd.uid is null ,0,1) as is_paid                         -- 判断是否是付费用户
,u.user_source
from public.dwd_user_active ta
left join tmp_user_paid pd on ta.uid=pd.uid and ta.d_date>=pd.first_pay_date
LEFT JOIN public.dwd_user_info u on ta.uid=u.uid::BIGINT            -- 补充user_source
;
