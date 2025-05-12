------------------------------------------
-- file: 老板飞书群推送-整体.sql
-- author: xiaoj
-- time: 2025/5/9 01:25
-- description:
------------------------------------------
with t as (
    -- 粒度：日期和合并的语言
      select
        d_date :: date as 日期,
        sum(pay_amt) as pay_amt,
        sum(new_pay_amt) as new_pay_amt,
        sum(old_pay_amt) as old_pay_amt,
        sum(ad_cost) as ad_cost,
        sum(pay_refund_amt) as pay_refund_amt,
        sum(dau) as dau
      from
        public.dw_operate_view a
      where 1 = 1
      group by
        d_date :: date
),
t0 as (
    select
        created_date :: date 日期,
        round(sum(adin_amt),2) 广告收入
    from dwd_adin_media_revenue
    group by created_date :: date
),
t1 as (
    -- 粒度： 日期和语言
    -- 功能： 根据 t 再计算部分字段
    select
      t.日期,
      pay_amt+COALESCE(广告收入,0) 总收入,
      pay_amt 总充值金额,
      COALESCE(广告收入,0) 广告收入,
      sum(pay_amt+COALESCE(广告收入,0)) over(partition by to_char(t.日期,'YYYY-MM') order by t.日期) 当月累计收入,
      new_pay_amt 新用户充值金额,
      old_pay_amt 老用户充值金额,
      ad_cost 消耗,
      dau,
      sum(ad_cost) over(partition by to_char(t.日期,'YYYY-MM')order by t.日期) 当月累计消耗,
      sum(pay_refund_amt) over(partition by to_char(t.日期,'YYYY-MM')order by t.日期) 当月累计退款,
      case when ad_cost = 0 then 0 else round(new_pay_amt * 1.0 / ad_cost, 2) end 新用户ROI,
      case when ad_cost = 0 then 0 else round((pay_amt+COALESCE(广告收入,0)-pay_refund_amt) * 1.0 / ad_cost, 2) end 整体ROI
    from t left join t0 on t.日期 = t0.日期
),
t2 as (
    -- 求昨日留存率
    select
        d_date :: date + 1 日期,
        case when sum(dau) = 0 then 0 else round(sum(dau_2login) * 100.0 / sum(dau), 2) end as 昨天留存率
      from
        public.dw_operate_view a
      where 1 = 1
        and d_date :: date = current_date - 2
      group by
        d_date :: date + 1
),
tmp_af_purchase as(
	select a.uid,
	    event_revenue_usd * 0.01 as event_revenue_usd,   -- 变现收益 美分转美元
	    created_date::date as p_date,                    -- 交易日期
	    install_time::date as d_date,                    -- 安装日期
	    conversion_type                                    -- 用于判断af视图类型
	from public.dwd_app_purchase_event_log a
	where event_name in('af_purchase','Purchase')
),
tmp_af_daily as (
    -- 取得每个交易日期 每个合并语言 总收入和新用户收入
    select
        t1.p_date,
        round(sum(t1.event_revenue_usd),2) as total_pay_amt,     -- 当日总收入
        round(sum( case when t1.p_date=t1.d_date and conversion_type in ('install','unknown') then t1.event_revenue_usd else 0 end),2) as af_d0  -- 当日新用户收入
    from tmp_af_purchase t1
    group by
        t1.p_date
),
tmp_af_daily_with_month_accum_payamt as (
    select
        p_date,
        total_pay_amt,
        af_d0,
        sum(total_pay_amt) over (partition by to_char(p_date,'YYYY-MM') order by p_date) as month_accum_payamt -- 当月累计支付金额
    from tmp_af_daily
)
select
  t2.日期,
  总收入 as "昨天总收入",
  总充值金额 as "昨天总充值金额",
  广告收入 as "昨天广告收入",
  新用户充值金额 as "昨天新用户充值金额",
  老用户充值金额 as "昨天老用户充值金额",
  消耗 as "昨天消耗",
  新用户ROI as 昨天新用户ROI,
  整体ROI as 昨天整体ROI,
  case when 消耗 = 0 then 0 else round(1.0* af_d0 / 消耗,2) end "昨日新用户ROI(af口径)",
  case when 消耗 = 0 then 0 else round(1.0* total_pay_amt / 消耗,2) end "昨日整体ROI(af口径)",
  dau as "昨天DAU",
  concat(cast(昨天留存率 as varchar),'%') 昨天留存率,
  当月累计收入 as "当月累计收入",
  当月累计消耗 as "当月累计消耗",
  case when 当月累计消耗 = 0 then 0 else round((当月累计收入 - 当月累计退款) * 1.0 / 当月累计消耗,2) end 当月整体ROI,
  case when 当月累计消耗 = 0 then 0 else round(month_accum_payamt * 1.0/当月累计消耗,2) end "当月整体ROI(af口径)"
from t2
    inner join t1 on t1.日期 = t2.日期     --只求昨天的指标
    inner join tmp_af_daily_with_month_accum_payamt t3 on t2.日期 = t3.p_date