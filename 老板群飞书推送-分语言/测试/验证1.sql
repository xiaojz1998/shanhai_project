------------------------------------------
-- file: 验证1.sql
-- author: xiaoj
-- time: 2025/5/9 18:20
-- description:
------------------------------------------
with tmp_af_purchase as(
	select a.uid,
	    case
           when lang_name in ('日语','韩语') then '日韩'
           when lang_name in ('法语','德语') then '法德'
           when lang_name in ('西班牙语','葡萄牙语') then '西葡'
           when lang_name in ('泰语','印度尼西亚语','简体中文','繁体中文') then '泰印中繁'
           else lang_name
         end as lang_name,                               -- 合并语言
	    event_revenue_usd * 0.01 as event_revenue_usd,   -- 变现收益 美分转美元
	    created_date::date as p_date,                    -- 交易日期
	    install_time::date as d_date,                     -- 安装日期
	    conversion_type                                   -- 用于判断af视图类型
	from public.dwd_app_purchase_event_log a
	left join dwd_user_info b on a.uid::text = b.uid        -- 关联用户表取得语言
	where event_name in('af_purchase','Purchase')
),
tmp_af_daily as (
    -- 取得每个交易日期 每个合并语言 总收入和新用户收入
    select
        t1.p_date,
        t1.lang_name,
        round(sum(t1.event_revenue_usd),2) as total_pay_amt,     -- 当日总收入
        round(sum( case when t1.p_date=t1.d_date and conversion_type in ('install','unknown')then t1.event_revenue_usd else 0 end),2) as af_d0  -- 当日新用户收入
    from tmp_af_purchase t1
    group by
        t1.p_date,
        t1.lang_name
) select * from tmp_af_daily where p_date='2025-05-08';