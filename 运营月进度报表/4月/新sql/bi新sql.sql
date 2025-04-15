with mt as (select
substr(d_date,1,7) 月份
, d_date 日期
, '总体' as 语言
, sum(pay_amt+ad_income_amt) 收入
, sum(month_income) as 累计收入
, sum(ad_cost) 消耗
, sum(month_ad_cost) 累计消耗
, case when sum(ad_cost) = 0 then 0
else round(sum(pay_amt+ad_income_amt-pay_refund_amt)*1.0/sum(ad_cost),2) end as "总ROI"
, case when sum(ad_cost) = 0 then 0
else round(sum(case when current_date - d_date::date >6 then pay_7 else 0 end)*1.0/sum(ad_cost),2) end as "新用户ROI7"
, sum(dau)  DAU
-- , case when sum(new_dau) = 0 then 0
-- else round(sum(case when current_date - d_date::date >1 then new_dau_2login else 0 end)*1.0/sum(new_dau),4) end 新用户次留
, case when sum(dau) = 0 then 0
else round(sum(case when current_date - d_date::date >1 then dau_2login else 0 end)*1.0/sum(dau),4) end 总次留
, case when sum(dau) = 0 then 0
else round(sum(case when current_date - d_date::date >2 then dau_3login else 0 end)*1.0/sum(dau),4) end 总3留
, case when sum(dau) = 0 then 0
else round(sum(case when current_date - d_date::date >6 then dau_7login else 0 end)*1.0/sum(dau),4) end 总7留
, case when sum(dau) = 0 then 0
else round(sum(pay_user)*1.0/sum(dau),4) end 总付费率
, case when sum(pay_user) = 0 then 0
else round(sum(pay_amt)*1.0/sum(pay_user),2) end 总ARPPU
, case when sum(dau) = 0 then 0
else round(sum(pay_amt)*1.0/sum(dau),2) end 总ARPU
, case when sum(pay_user) = 0 then 0
else round(sum(pay_order)*1.0/sum(pay_user),2) end 总人均付费次数
, case when sum(due_user) = 0 then 0
else round(sum(repay_user)*1.0/sum(due_user),4) end 订阅会员续订率
, case when sum(old_pay_user) = 0 then 0
else round(sum(old_pay_order)*1.0/sum(old_pay_user),2) end 老用户人均付费次数
, case when sum(total_push_unt) = 0 then 0
else round(sum(total_click_unt)*1.0/sum(total_push_unt),6) end "PUSH点击率"
, case when sum(watch_user) = 0 then 0
else round(sum(watch_duration)*1.0/sum(watch_user),2) end "人均播放时长(分钟)"
, case when sum(watch_user) = 0 then 0
else round(sum(eid_watch_cnt)*1.0/sum(watch_user),1) end 人均每日观看集数
, case when sum(watch_user) = 0 then 0
else round(sum(eidpay_watch_user)*1.0/sum(watch_user),4) end 付费集播放率
  , case when sum(eidfree_watch_user) = 0 then 0
else round(sum(eidfree_watch_cnt)*1.0/sum(eidfree_watch_user),1) end 人均剧均免费集观看数
-- , case when sum(watch_user) = 0 then 0
-- else round(sum(eidfree_watch_user)*1.0/sum(watch_user),4) end 免费集播放率
from dw_operate_schedule
where 1 = 1
and d_date::date between '2025-01-01' and current_date - 1
group by substr(d_date,1,7)
, d_date

union

select
substr(d_date,1,7) 月份
,d_date 日期
, case when lang_name = '葡萄牙语' then '葡语'
  when lang_name = '印度尼西亚语' then '印尼语'
  when lang_name in ('简体中文','繁体中文') then '中文'
  else lang_name end as 语言
--   when lang_name = 'UNKNOWN' then '英语'
, sum(pay_amt+ad_income_amt) 收入
, sum(month_income) as 累计收入
, sum(ad_cost) 消耗
, sum(month_ad_cost) 累计消耗
, case when sum(ad_cost) = 0 then 0
else round(sum(pay_amt+ad_income_amt-pay_refund_amt)*1.0/sum(ad_cost),4) end as "总ROI"
, case when sum(ad_cost) = 0 then 0
else round(sum(case when current_date - d_date::date >6 then pay_7 else 0 end)*1.0/sum(ad_cost),4) end as "新用户ROI7"
, sum(dau)  DAU
-- , case when sum(new_dau) = 0 then 0
-- else round(sum(case when current_date - d_date::date >1 then new_dau_2login else 0 end)*1.0/sum(new_dau),4) end 新用户次留
, case when sum(dau) = 0 then 0
else round(sum(case when current_date - d_date::date >1 then dau_2login else 0 end)*1.0/sum(dau),4) end 总次留
, case when sum(dau) = 0 then 0
else round(sum(case when current_date - d_date::date >2 then dau_3login else 0 end)*1.0/sum(dau),4) end 总3留
, case when sum(dau) = 0 then 0
else round(sum(case when current_date - d_date::date >6 then dau_7login else 0 end)*1.0/sum(dau),4) end 总7留
, case when sum(dau) = 0 then 0
else round(sum(pay_user)*1.0/sum(dau),4) end 总付费率
, case when sum(pay_user) = 0 then 0
else round(sum(pay_amt)*1.0/sum(pay_user),2) end 总ARPPU
, case when sum(dau) = 0 then 0
else round(sum(pay_amt)*1.0/sum(dau),2) end 总ARPU
, case when sum(pay_user) = 0 then 0
else round(sum(pay_order)*1.0/sum(pay_user),2) end 总人均付费次数
, case when sum(due_user) = 0 then 0
else round(sum(repay_user)*1.0/sum(due_user),4) end 订阅会员续订率
, case when sum(old_pay_user) = 0 then 0
else round(sum(old_pay_order)*1.0/sum(old_pay_user),2) end 老用户人均付费次数
, case when sum(total_push_unt) = 0 then 0
else round(sum(total_click_unt)*1.0/sum(total_push_unt),6) end "PUSH点击率"
, case when sum(watch_user) = 0 then 0
else round(sum(watch_duration)*1.0/sum(watch_user),2) end "人均播放时长(分钟)"
, case when sum(watch_user) = 0 then 0
else round(sum(eid_watch_cnt)*1.0/sum(watch_user),1) end 人均每日观看集数
, case when sum(watch_user) = 0 then 0
else round(sum(eidpay_watch_user)*1.0/sum(watch_user),4) end 付费集播放率
  , case when sum(eidfree_watch_user) = 0 then 0
else round(sum(eidfree_watch_cnt)*1.0/sum(eidfree_watch_user),1) end 人均剧均免费集观看数
-- , case when sum(watch_user) = 0 then 0
-- else round(sum(eidfree_watch_user)*1.0/sum(watch_user),4) end 免费集播放率
from dw_operate_schedule
where 1 = 1
and d_date::date between '2025-01-01' and current_date - 1
group by substr(d_date,1,7)
, d_date
, case when lang_name = '葡萄牙语' then '葡语'
  when lang_name = '印度尼西亚语' then '印尼语'
  when lang_name in ('简体中文','繁体中文') then '中文'
--   when lang_name = 'UNKNOWN' then '英语'
  else lang_name end
)

, dt as (select mt.语言
, 月份
, 日期
,  收入
, 累计收入
, case when 目标收入 = 0 then 0
else round(累计收入*1.0/目标收入,4) end as 收入累计完成率
, "总ROI"
, "新用户ROI7"
, 消耗
, 累计消耗
, case when 目标消耗 = 0 then 0
else round(累计消耗*1.0/目标消耗,4) end 消耗累计完成率
, DAU
, lag(DAU) over(partition by mt.语言 order by 日期 ) DAU2
-- , 新用户次留
-- , lag(新用户次留) over(partition by a.语言 order by 日期 ) 新用户次留2
, 总次留
, lag(总次留) over(partition by mt.语言 order by 日期 ) 总次留2
, 总3留
, lag(总3留) over(partition by mt.语言 order by 日期 ) 总3留2
, 总7留
, lag(总7留) over(partition by mt.语言 order by 日期 ) 总7留2
, 总付费率
, lag(总付费率) over(partition by mt.语言 order by 日期 ) 总付费率2
, 总ARPPU
, lag(总ARPPU) over(partition by mt.语言 order by 日期 ) 总ARPPU2
, 总ARPU
, lag(总ARPU) over(partition by mt.语言 order by 日期 ) 总ARPU2
, 总人均付费次数
, lag(总人均付费次数) over(partition by mt.语言 order by 日期 ) 总人均付费次数2
, 订阅会员续订率
, lag(订阅会员续订率) over(partition by mt.语言 order by 日期 ) 订阅会员续订率2
, 老用户人均付费次数
, lag(老用户人均付费次数) over(partition by mt.语言 order by 日期 ) 老用户人均付费次数2
, "PUSH点击率"
, lag("PUSH点击率") over(partition by mt.语言 order by 日期 ) "PUSH点击率2"
, "人均播放时长(分钟)"
, lag("人均播放时长(分钟)") over(partition by mt.语言 order by 日期 ) "人均播放时长(分钟)2"
, 人均每日观看集数
, lag(人均每日观看集数) over(partition by mt.语言 order by 日期 ) 人均每日观看集数2
, 付费集播放率
, lag(付费集播放率) over(partition by mt.语言 order by 日期 ) 付费集播放率2
, 人均剧均免费集观看数
, lag(人均剧均免费集观看数) over(partition by mt.语言 order by 日期 ) 人均剧均免费集观看数2
from (SELECT d_month
  , bud_area as 语言
  , bud_payamt as 目标收入
  , cost_target as 目标消耗
  FROM "dw_area_month_budget" where area_tag = '语言' ) a right join mt on a.语言 = mt.语言 and a.d_month=mt.月份)

, total_all as (
  select
  语言, 日期
  , 收入
  , 累计收入
  , 收入累计完成率
  , "总ROI"
  , coalesce("新用户ROI7",0) as "新用户ROI7"
  , DAU
  , case when DAU2 =0 or DAU2 is null then 0 else round(DAU*1.0/DAU2-1,4) end DAU日环比
--   , 新用户次留
--   , case when 新用户次留2 =0 or 新用户次留2 is null or 新用户次留=0 then 0 else round(新用户次留*1.0/新用户次留2-1,4) end 新用户次留日环比
   , 总次留
  , case when 总次留2 =0 or 总次留2 is null then 0 else round(总次留*1.0/总次留2-1,4) end 总次留日环比
  , 总3留
    , case when 总3留2 =0 or 总3留2 is null then 0 else round(总3留*1.0/总3留2-1,4) end 总3留日环比
   , 总7留
  , case when 总7留2 =0 or 总7留2 is null then 0 else round(总7留*1.0/总7留2-1,4) end 总7留日环比
  , 总付费率
  , case when 总付费率2 =0 or 总付费率2 is null then 0 else round(总付费率*1.0/总付费率2-1,4) end 总付费率日环比
  , 总ARPPU
  , case when 总ARPPU2 =0 or 总ARPPU2 is null then 0 else round(总ARPPU*1.0/总ARPPU2-1,4) end 总ARPPU日环比
  , 总ARPU
  , case when 总ARPU2 =0 or 总ARPU2 is null then 0 else round(总ARPU*1.0/总ARPU2-1,4) end 总ARPU日环比
   , 订阅会员续订率
  , case when 订阅会员续订率2 =0 or 订阅会员续订率2 is null then 0 else round(订阅会员续订率*1.0/订阅会员续订率2-1,4) end 订阅会员续订率日环比
  , "PUSH点击率"
    ,  case when "PUSH点击率2" =0 or "PUSH点击率2" is null then 0 else round("PUSH点击率"*1.0/"PUSH点击率2"-1,4) end "PUSH点击率日环比"
    , "人均播放时长(分钟)"
  , case when "人均播放时长(分钟)2" =0 or "人均播放时长(分钟)2" is null then 0 else round("人均播放时长(分钟)"*1.0/"人均播放时长(分钟)2"-1,4) end "人均播放时长(分钟)日环比"
    , 人均每日观看集数
  , case when 人均每日观看集数2 =0 or 人均每日观看集数2 is null then 0 else round(人均每日观看集数*1.0/人均每日观看集数2-1,4) end 人均每日观看集数日环比
    , 付费集播放率
  , case when 付费集播放率2 =0 or 付费集播放率2 is null then 0 else round(付费集播放率*1.0/付费集播放率2-1,4) end 付费集播放率日环比
  from dt
  where 日期::date < current_date  and 月份 = TO_CHAR(current_date-1, 'YYYY-MM')

  union

select bud_area as 语言
  , '目标' as 日期
  , null as 收入
  , bud_payamt as 累计收入
  , null as 收入累计完成率
  , bud_roi as "总ROI"
  , bud_roi7 as "新用户ROI7"
  , bud_dau as DAU
  , null as DAU日环比
  , bud_dau2login_rate as 总次留
  , null as 总次留日环比
  , bud_dau3login_rate as 总3留
  , null as 总3留日环比
  , bud_dau7login_rate as 总7留
  , null as 总7留日环比
  , bud_payuser_rate 总付费率
  , null as 总付费率日环比
  , bud_arppu as 总ARPPU
  , null as 总ARPPU日环比
  , bud_arpu as 总ARPU
  , null as 总ARPU日环比
  , bud_subsrepay_rate as 订阅会员续订率
  , null as 订阅会员续订率日环比
  , bud_pushclick_rate as "Push点击率"
  , null as "Push点击率日环比"
  , bud_duration_avguser as "人均播放时长(分钟)"
  , null as "人均播放时长(分钟)日环比"
  , bud_eid_avguser as 人均每日观看集数
  , null 人均每日观看集数日环比
  , bud_eidpay_rate as 付费集播放率
  , null as  付费集播放率日环比
  FROM dw_area_month_budget
   where area_tag = '语言' and d_month = TO_CHAR(current_date-1, 'YYYY-MM'))
---------------------------------------------------------------
-- 上面是with中间语句，下面是各个语言的查询语句
---------------------------------------------------------------



--葡萄牙语
select
  *
from
  total_all
where
  语言 = '葡语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc

--英语
select
  *
from
  total_all
where
  语言 = '英语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc

--日语
select
  *
from
  total_all
where
  语言 = '日语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc


--韩语
select
  *
from
  total_all
where
  语言 = '韩语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc


--泰语
select
  *
from
  total_all
where
  语言 = '泰语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc


--印尼语
select
  *
from
  total_all
where
  语言 = '印尼语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc


--法语
select
  *
from
  total_all
where
  语言 = '法语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc

--德语
select
  *
from
  total_all
where
  语言 = '德语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc


--西班牙语
select
  *
from
  total_all
where
  语言 = '西班牙语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc





--中文
select
  *
from
  total_all
where
  语言 = '中文'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc


--阿拉伯语
select
  *
from
  total_all
where
  语言 = '阿拉伯语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end DESC


--越南语
select
  *
from
  total_all
where
  语言 = '越南语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end DESC


--土耳其语
select
  *
from
  total_all
where
  语言 = '土耳其语'
order by
  case
    when 日期 = '目标' then '2099-01-01'
    else 日期
  end desc