---------------------------------------------------------------
-- 
---------------------------------------------------------------
with mt as (
  select
    substr(d_date, 1, 7) 月份,
    d_date 日期,
    case
      when lang_name = '印度尼西亚语' then '印尼语'
      when lang_name in (
        '简体中文',
        '繁体中文'
      ) then '中文'
      else lang_name
    end as 语言,
    sum(pay_amt + ad_income_amt) 收入,
    sum(month_income) as 累计收入,
    sum(dau) DAU -- , case when sum(new_dau) = 0 then 0
    -- else round(sum(case when current_date - d_date::date >1 then new_dau_2login else 0 end)*100.0/sum(new_dau),2) end 新用户次留
,
    case
      when sum(dau) = 0 then 0
      else round(
        sum(
          case
            when current_date - d_date :: date > 1 then dau_2login
            else 0
          end
        ) * 100.0 / sum(dau),
        2
      )
    end 总次留,
    case
      when sum(dau) = 0 then 0
      else round(sum(pay_user) * 100.0 / sum(dau), 2)
    end 总付费率,
    case
      when sum(pay_user) = 0 then 0
      else round(sum(pay_amt) * 1.0 / sum(pay_user), 2)
    end 总ARPPU,
    case
      when sum(old_pay_user) = 0 then 0
      else round(sum(old_pay_order) * 1.0 / sum(old_pay_user), 2)
    end 老用户人均付费次数,
    case
      when sum(watch_user) = 0 then 0
      else round(sum(watch_duration) * 1.0 / sum(watch_user), 2)
    end "人均播放时长(分钟)",
    case
      when sum(watch_user) = 0 then 0
      else round(sum(eid_watch_cnt) * 1.0 / sum(watch_user), 1)
    end 人均每日观看集数,
    case
      when sum(watch_user) = 0 then 0
      else round(sum(eidpay_watch_user) * 100.0 / sum(watch_user), 2)
    end 付费集播放率 -- , case when sum(watch_user) = 0 then 0
    -- else round(sum(eidfree_watch_user)*1.0/sum(watch_user),4) end 免费集播放率
  from
    dw_operate_schedule
  where
    1 = 1
    and d_date :: date between current_date - 40
    and current_date - 1
  group by
    substr(d_date, 1, 7),
    d_date,
    case
      when lang_name = '印度尼西亚语' then '印尼语'
      when lang_name in (
        '简体中文',
        '繁体中文'
      ) then '中文'
      else lang_name
    end
),
dt as (
  select
    mt.语言,
    月份,
    日期,
    收入,
    累计收入,
    case
      when 目标收入 = 0 then 0
      else round(
        累计收入 * 100.0 / 目标收入,
        2
      )
    end 收入累计完成率,
    DAU,
    lag(DAU) over(
      partition by a.语言
      order by
        日期
    ) DAU2 -- , 新用户次留
    -- , lag(新用户次留) over(partition by a.语言 order by 日期 ) 新用户次留2
,
    总次留,
    lag(
      总次留
    ) over(
      partition by a.语言
      order by
        日期
    ) 总次留2,
    总付费率,
    lag(
      总付费率
    ) over(
      partition by a.语言
      order by
        日期
    ) 总付费率2,
    总ARPPU,
    lag(总ARPPU) over(
      partition by a.语言
      order by
        日期
    ) 总ARPPU2,
    老用户人均付费次数,
    lag(
      老用户人均付费次数
    ) over(
      partition by a.语言
      order by
        日期
    ) 老用户人均付费次数2,
    "人均播放时长(分钟)",
    lag(
      "人均播放时长(分钟)"
    ) over(
      partition by a.语言
      order by
        日期
    ) "人均播放时长(分钟)2",
    人均每日观看集数,
    lag(
      人均每日观看集数
    ) over(
      partition by a.语言
      order by
        日期
    ) 人均每日观看集数2,
    付费集播放率,
    lag(
      付费集播放率
    ) over(
      partition by a.语言
      order by
        日期
    ) 付费集播放率2 -- , 免费集播放率
    -- , lag(免费集播放率) over(partition by a.语言 order by 日期 ) 免费集播放率2
  from
    (
      SELECT
        d_month,
        if(
          bud_area = '葡语',
          '葡萄牙语',
          bud_area
        ) as 语言,
        bud_payamt as 目标收入
      FROM
        "dw_area_month_budget"
      where
        area_tag = '语言'
    ) a
    right join mt on a.语言 = mt.语言
    and a.d_month = mt.月份
),
total_all as (
  select
    语言,
    日期,
    收入 "收入($)",
    累计收入 "累计收入($)",
    concat(
      cast(
        收入累计完成率 as varchar
      ),
      '%'
    ) 收入累计完成率,
    DAU,
    concat(
      cast(
        DAU日环比 as varchar
      ),
      '%'
    ) DAU日环比 -- , concat(cast(新用户次留 as varchar),'%') 新用户次留
    -- , concat(cast(新用户次留日环比 as varchar),'%') 新用户次留日环比
,
    concat(
      cast(
        总次留 as varchar
      ),
      '%'
    ) 总次留,
    concat(
      cast(
        总次留日环比 as varchar
      ),
      '%'
    ) 总次留日环比,
    concat(
      cast(
        总付费率 as varchar
      ),
      '%'
    ) 总付费率,
    concat(
      cast(
        总付费率日环比 as varchar
      ),
      '%'
    ) 总付费率日环比,
    总ARPPU,
    concat(
      cast(
        总ARPPU日环比 as varchar
      ),
      '%'
    ) 总ARPPU日环比,
    老用户人均付费次数,
    concat(
      cast(
        老用户人均付费次数日环比 as varchar
      ),
      '%'
    ) 老用户人均付费次数日环比,
    "人均播放时长(分钟)",
    concat(
      cast(
        "人均播放时长(分钟)日环比" as varchar
      ),
      '%'
    ) "人均播放时长(分钟)日环比",
    人均每日观看集数,
    concat(
      cast(
        人均每日观看集数日环比 as varchar
      ),
      '%'
    ) 人均每日观看集数日环比,
    concat(
      cast(
        付费集播放率 as varchar
      ),
      '%'
    ) 付费集播放率,
    concat(
      cast(
        付费集播放率日环比 as varchar
      ),
      '%'
    ) 付费集播放率日环比
  from
    (
      select
        语言,
        日期 -- , 月份
,
        收入,
        累计收入,
        收入累计完成率,
        DAU,
        case
          when DAU2 = 0
          or DAU2 is null then 0
          else round(DAU * 100.0 / DAU2-100, 2)
        end DAU日环比 -- , 新用户次留
        -- , case when 新用户次留2 =0 or 新用户次留2 is null or 新用户次留=0 then 0 else round(新用户次留*100.0/新用户次留2-100,2) end 新用户次留日环比
,
        总次留,
        case
          when 总次留2 = 0
          or 总次留2 is null then 0
          else round(
            总次留 * 100.0 / 总次留2-100,
            2
          )
        end 总次留日环比,
        总付费率,
        case
          when 总付费率2 = 0
          or 总付费率2 is null then 0
          else round(
            总付费率 * 100.0 / 总付费率2-100,
            2
          )
        end 总付费率日环比,
        总ARPPU,
        case
          when 总ARPPU2 = 0
          or 总ARPPU2 is null then 0
          else round(
            总ARPPU * 100.0 / 总ARPPU2-100,
            2
          )
        end 总ARPPU日环比,
        老用户人均付费次数,
        case
          when 老用户人均付费次数2 = 0
          or 老用户人均付费次数2 is null then 0
          else round(
            老用户人均付费次数 * 100.0 / 老用户人均付费次数2-100,
            2
          )
        end 老用户人均付费次数日环比,
        "人均播放时长(分钟)",
        case
          when "人均播放时长(分钟)2" = 0
          or "人均播放时长(分钟)2" is null then 0
          else round(
            "人均播放时长(分钟)" * 100.0 / "人均播放时长(分钟)2" -100,
            2
          )
        end "人均播放时长(分钟)日环比",
        人均每日观看集数,
        case
          when 人均每日观看集数2 = 0
          or 人均每日观看集数2 is null then 0
          else round(
            人均每日观看集数 * 100.0 / 人均每日观看集数2-100,
            2
          )
        end 人均每日观看集数日环比,
        付费集播放率,
        case
          when 付费集播放率2 = 0
          or 付费集播放率2 is null then 0
          else round(
            付费集播放率 * 100.0 / 付费集播放率2-100,
            2
          )
        end 付费集播放率日环比 --       , 免费集播放率
        --   , case when 免费集播放率2 =0 or 免费集播放率2 is null then 0 else round(免费集播放率*1.0/免费集播放率2-1,4) end 免费集播放率日环比
      from
        dt
      where
        1 = 1
    ) a
  where
    substr(日期, 1, 7) = TO_CHAR(current_date-1, 'YYYY-MM')
  union
  select
    if(
      bud_area = '葡语',
      '葡萄牙语',
      bud_area
    ) as 语言,
    '目标' as 日期,
    null as 收入,
    bud_payamt as 累计收入,
    null as 收入累计完成率,
    bud_dau as DAU,
    null as DAU日环比 -- , concat(cast(bud_newdau2login_rate*100 as varchar),'%') as 新用户次留
    -- , null as 新用户次留日环比
,
    concat(cast(bud_dau2login_rate * 100 as varchar), '%') as 总次留,
    null as 总次留日环比,
    concat(cast(bud_payuser_rate * 100 as varchar), '%') as 总付费率,
    null as 总付费率日环比,
    bud_arppu as 总ARPPU,
    null as 总ARPPU日环比,
    bud_oldpayuser_avgorder as 老用户人均付费次数,
    null as 老用户人均付费次数日环比,
    bud_duration_avguser as "人均播放时长(分钟)",
    null as "人均播放时长(分钟)日环比",
    round(bud_eid_avguser, 1) as 人均每日观看集数,
    null 人均每日观看集数日环比,
    concat(cast(bud_eidpay_rate * 100 as varchar), '%') as 付费集播放率,
    null as 付费集播放率日环比
  FROM
    dw_area_month_budget
  where
    area_tag = '语言'
    and d_month = TO_CHAR(current_date-1, 'YYYY-MM')
)

---------------------------------------------------------------
-- 推送sql
---------------------------------------------------------------

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


--葡萄牙语
select
  *
from
  total_all
where
  语言 = '葡萄牙语'
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