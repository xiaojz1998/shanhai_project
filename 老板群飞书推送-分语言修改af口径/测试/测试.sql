---------------------------------------------
-- File: 测试.sql
-- Time: 2025/6/4 17:53
-- User: xiaoj
-- Description:  
---------------------------------------------
-- 老板群飞书推送-整体
-- 修改前
with tmp_af_purchase as(
	select a.uid,
	    event_revenue_usd * 0.01 as event_revenue_usd,   -- 变现收益 美分转美元
	    created_date::date as p_date,                    -- 交易日期
	    install_time::date as d_date,                    -- 安装日期
	    conversion_type                                    -- 用于判断af视图类型
	from public.dwd_app_purchase_event_log a
	where event_name in('af_purchase','Purchase')
),
tmp_af_daily as (
    select
        t1.p_date,
        round(sum(t1.event_revenue_usd),2) as total_pay_amt,     -- 当日总收入
        round(sum( case when t1.p_date=t1.d_date and conversion_type in ('install','unknown') then t1.event_revenue_usd else 0 end),2) as af_d0  -- 当日新用户收入
    from tmp_af_purchase t1
    group by
        t1.p_date
) select * from tmp_af_daily


-- 修改后
with tmp_af_daily as (
    select
        d_date,
        round(sum(pay_amt),2) as total_pay_amt,
        round(sum(case when conversion_type in ('install','unknown') and campaign_type in ('ua','organic','unknown')  then af_d0 else 0 end),2) as af_d0
    from public.ads_operate_roi_af_rs
    group by d_date
)
select
    *
from tmp_af_daily

-- 老板群飞书推送-分语言

select distinct lang_name from public.ads_operate_roi_af_rs

with tmp_af_daily as (
    select
        d_date,
        case
           when lang_name in ('日语','韩语') then '日韩'
           when lang_name in ('法语','德语') then '法德'
           when lang_name in ('西班牙语','葡萄牙语') then '西葡'
           when lang_name in ('泰语','印尼语','中文简体','繁体中文') then '泰印中繁'
           else lang_name
         end as lang_name,
        round(sum(pay_amt),2) as total_pay_amt,
        round(sum(case when conversion_type in ('install','unknown') and campaign_type in ('ua','organic','unknown')  then af_d0 else 0 end),2) as af_d0
    from public.ads_operate_roi_af_rs
    group by d_date,
        case
           when lang_name in ('日语','韩语') then '日韩'
           when lang_name in ('法语','德语') then '法德'
           when lang_name in ('西班牙语','葡萄牙语') then '西葡'
           when lang_name in ('泰语','印尼语','中文简体','繁体中文') then '泰印中繁'
           else lang_name
         end
)
