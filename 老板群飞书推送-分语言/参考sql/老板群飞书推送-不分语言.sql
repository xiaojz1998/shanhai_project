------------------------------------------
-- file: 老板群飞书推送-不分语言.sql
-- author: xiaoj
-- time: 2025/5/9 10:07
-- description:
------------------------------------------
-- 老板群飞书推送
with t1 as
  (select
a.日期
, pay_amt+COALESCE(广告收入,0) 总收入
, pay_amt 总充值金额
, COALESCE(广告收入,0) 广告收入
, sum(pay_amt+COALESCE(广告收入,0)) over(partition by to_char(a.日期,'YYYY-MM') order by a.日期) 当月累计收入
, new_pay_amt 新用户充值金额
, old_pay_amt 老用户充值金额
, ad_cost 消耗
, dau
, sum(ad_cost) over(partition by to_char(a.日期,'YYYY-MM') order by a.日期) 当月累计消耗
, sum(pay_refund_amt) over(partition by to_char(a.日期,'YYYY-MM') order by a.日期) 当月累计退款
, case when ad_cost =0 then 0 else round(new_pay_amt*1.0/ad_cost,2) end 新用户ROI
, case when ad_cost =0 then 0 else round((pay_amt+COALESCE(广告收入,0)-pay_refund_amt)*1.0/ad_cost,2) end 整体ROI
from (select
	d_date::date as 日期,
	sum(pay_amt)  as pay_amt,
	sum(new_pay_amt) as new_pay_amt,
	sum(old_pay_amt) as old_pay_amt,
	sum(ad_cost)  as ad_cost,
    sum(pay_refund_amt) as pay_refund_amt,
	sum(dau) as dau
	from public.dw_operate_view a
	where 1=1
	and d_date::date = current_date - 1
    group by d_date::date) a left join
(select TO_TIMESTAMP(created_at) :: date 日期
, round(sum(micros)*1.0/1000000,2) 广告收入
from ad_mob_total_log
where TO_TIMESTAMP(created_at) :: date = current_date - 1
group by TO_TIMESTAMP(created_at) :: date) b on a.日期 = b.日期)

, t2 as (select
d_date::date +1 日期,
case when sum(dau) = 0 then 0 else round(sum(dau_2login)*100.0/sum(dau),2) end as 昨天留存率
from public.dw_operate_view a
where 1=1
and d_date::date = current_date - 2
group by d_date::date +1)


select
t1.日期
, 总收入 as "昨天总收入($)"
, 总充值金额 as "昨天总充值金额($)"
, 广告收入 as "昨天广告收入($)"
, 新用户充值金额 as "昨天新用户充值金额($)"
, 老用户充值金额 as "昨天老用户充值金额($)"
, 消耗 as "昨天消耗($)"
, 新用户ROI as 昨天新用户ROI
, 整体ROI as 昨天整体ROI
, dau as "昨天DAU"
, concat(cast(昨天留存率 as varchar),'%') 昨天留存率
, 当月累计收入 as "当月累计收入($)"
, 当月累计消耗 as "当月累计消耗($)"
, case when 当月累计消耗 = 0 then 0 else round((当月累计收入-当月累计退款)*1.0/当月累计消耗,2) end 当月整体ROI
from t1 inner join t2 on t1.日期 = t2.日期