------------------------------------------
-- file: 分语言提取新字段.sql
-- author: xiaoj
-- time: 2025/5/8 16:13
-- description:
------------------------------------------

---------------------------------------------------
--  查看  数据结构
---------------------------------------------------
-- public.dwd_app_purchase_event_log
select install_time from public.dwd_app_purchase_event_log where install_time is not null limit 100;

-- dwd_user_info
select distinct lang_name from dwd_user_info limit 100;

-- uid 唯一
select count(*) from dwd_user_info;
select count(distinct uid) from dwd_user_info;

select *
from public.dwd_app_purchase_event_log
where created_date::date < install_time::date

-- 测试runner 的sql
-- SELECT split_part(campaign_name ,'_',5) ,sum(cost_amount) * 0.0001
-- FROM ad_cost_data_log a
--     left join public."oversea-api_osd_videos" v1
--         on split_part(campaign_name ,'_',5)::text=v1.id::text
-- where v1.id is not null and TO_TIMESTAMP(a.created_at)='20250507' and cost_amount>0
-- group by split_part(campaign_name ,'_',5)

---------------------------------------------------
-- 分语言
-- 昨日新用户roi（af口径）
-- 昨日整体roi（af口径）
-- 当月整体roi（af口径）
---------------------------------------------------

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
	       install_time::date as d_date                     -- 安装日期
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
           round(sum( case when t1.p_date=t1.d_date then t1.event_revenue_usd else 0 end),2) as af_d0  -- 当日新用户收入
       from tmp_af_purchase t1
       group by
           t1.p_date,
           t1.lang_name
    ),
    tmp_af_daily_with_month_accum_payamt as (
        select
            p_date,
            lang_name,
            total_pay_amt,
            af_d0,
            sum(total_pay_amt) over (partition by lang_name,to_char(p_date,'YYYY-MM') order by p_date) as month_accum_payamt -- 当月累计支付金额
        from tmp_af_daily
    )
select * from tmp_af_daily_with_month_accum_payamt
where p_date = '2025-05-08'





