---------------------------------------------
-- File: 分语言投放消耗.sql
-- Time: 2025/6/26 18:23
-- User: xiaoj
-- Description:  
---------------------------------------------
----------新增用户
with newuser_tb as (
    select
        uid::bigint
        , campaign_id
        , d_date::date as p_date
    from public.dwd_user_info
    where d_date::date < current_date
        and d_date::date > current_date-40
),
---------付费数据
pay_tb as (
    select
        p_date,
        uid,
        round(sum(money), 2) as money
    from
        (
            -- 支付数据
            select
              to_timestamp(created_at) :: date as p_date,
              uid,
              1.0 * sum("money") / 100 as "money"
            from
              all_order_log
            where
              status = 1
              and environment = 1 -- and to_char(to_timestamp(created_at), 'YYYY-MM') = TO_CHAR(current_date, 'YYYY-MM')
              and to_timestamp(created_at) :: date < current_date
              and to_timestamp(created_at) :: date > current_date -40
            group by
              uid,
              to_timestamp(created_at) :: date
            union all
            -- 退款数据
            select
              to_timestamp(created_at) :: date as p_date,
              uid,
              -1.0 * sum("money") / 100 as "money"
            from
              all_refund_order_log
            where
              status = 1
              and environment = 1 -- and to_char(to_timestamp(created_at), 'YYYY-MM') = TO_CHAR(current_date, 'YYYY-MM')
              and to_timestamp(created_at) :: date < current_date
              and to_timestamp(created_at) :: date > current_date -40
            group by
              uid,
              to_timestamp(created_at) :: date
        ) tmp
    group by
        p_date,
        uid
),
---新增、新增付费、累计收入数据
roi_tb as (
    select
        newuser_tb.p_date,
        newuser_tb.campaign_id,
        count(distinct newuser_tb.uid) as newuser_uv,
        count(distinct if(newuser_tb.uid = pay_tb.uid and newuser_tb.p_date = pay_tb.p_date,newuser_tb.uid,null)) as new_pay_uv,    -- 新增用户当天付费人数
        sum(if(newuser_tb.uid = pay_tb.uid and pay_tb.p_date = newuser_tb.p_date, pay_tb.money,0.0)) as money_0        -- 新增用户当天付费金额
    from newuser_tb
    left join pay_tb on cast(newuser_tb.uid as varchar) = cast(pay_tb.uid as varchar)
    group by
        newuser_tb.p_date,
        newuser_tb.campaign_id
),
----消耗数据
cost_tb as (
    select
        to_timestamp(created_at) :: date as p_date,
        a.campaign_id,
        b.campaign_name,
        sum(impression) as impression,
        sum(click) as click,
        round(sum(cost_amount) / 10000.0, 2) as cost_amount
    from ad_cost_data_log a
    left join v_dim_ad_campaign_info b on a.campaign_id = b.campaign_id
    where
        1 = 1 -- and to_char(to_timestamp(created_at), 'YYYY-MM') = TO_CHAR(current_date, 'YYYY-MM')
        and to_timestamp(created_at) :: date < current_date
        and to_timestamp(created_at) :: date > current_date -40
    group by
        to_timestamp(created_at) :: date,
        a.campaign_id,
        b.campaign_name
),
-- 总消耗
total_cost as (
    select
        p_date,
        语言,
        当月累计消耗,
        case when cost_target is null or cost_target = 0 then 0
            else round(当月累计消耗 * 100.0 /(cost_target),2)
            end 当月累计消耗完成率,
        cost_target - 当月累计消耗 as 剩余消耗
    from(
        select
            *
        from(
            select
                p_date,
                语言,
                sum(cost_amount) over(partition by 语言,to_char(p_date, 'YYYY-MM') order by p_date) 当月累计消耗
            from(
                select
                    p_date,
                    if(language_tb.name in ('中文简体','繁体中文'),'中文',language_tb.name) 语言,
                    sum(cost_amount) cost_amount
                from(
                    select
                        to_timestamp(created_at) :: date as p_date,
                        a.campaign_id,
                        b.campaign_name,
                        round(sum(cost_amount) / 10000.0, 2) as cost_amount
                    from ad_cost_data_log a
                    left join v_dim_ad_campaign_info b on a.campaign_id = b.campaign_id
                    where   1 = 1
                        and "cost_amount" > 0
                        and to_timestamp(created_at) :: date < current_date
                        and to_timestamp(created_at) :: date > current_date -40
                    group by
                        to_timestamp(created_at) :: date,
                        a.campaign_id,
                        b.campaign_name
                  ) t1
                left join "oversea-api_osd_videos" vid_tb on cast(split_part(t1.campaign_name, '_', 5) as varchar) = cast(vid_tb.id as varchar)
                left join "oversea-api_osd_categories" language_tb on cast(vid_tb.category_id as varchar) = cast(language_tb.id as varchar)
                where   language_tb.name is not null
                group by p_date,
                    if(language_tb.name in ('中文简体','繁体中文'),'中文',language_tb.name)
            ) a
        ) a
    ) a
    left join(
        select
            d_month,
            case when bud_area='葡语' then '葡萄牙语'
                else bud_area end   as bud_area,
            cost_target
        from dw_area_month_budget
    ) b on to_char(a.p_date, 'YYYY-MM') = b.d_month
        and a.语言 = b.bud_area
),
total_recharge as (
    select
      d_date :: date 日期,
      语言,
      总充值,
      退款,
      case when lag(总充值) over(partition by 语言 order by d_date) = 0 then '0%'
          else concat(cast(round(总充值 * 100.0 / lag(总充值) over(partition by 语言 order by d_date) -100,2) as varchar),'%')
        end 总充值环比
    from(
        select
            d_date,
            case when lang_name = '印度尼西亚语' then '印尼语'
                when lang_name in ( '简体中文', '繁体中文') then '中文'
              else lang_name end as 语言,
            sum(pay_amt) 总充值,
            sum(pay_refund_amt) 退款
        from dw_operate_view
        where 1 = 1
            and d_date :: date < current_date
            and d_date :: date > current_date -40
        group by d_date,
            case when lang_name = '印度尼西亚语' then '印尼语'
              when lang_name in ('简体中文','繁体中文') then '中文'
              else lang_name end
    ) a
),
total_all as (
    select
        *
    from(
        select
            a.日期,
            a.语言,
            消耗 "消耗($)",
            -- lag(消耗) over(partition by a.语言 order by a.日期) "昨日消耗($)",
            case when lag(消耗) over( partition by a.语言 order by a.日期 ) is null or lag(消耗) over( partition by a.语言 order by a.日期 ) = 0 then '0%'
                else concat(cast(round(消耗 * 100.0 / lag(消耗) over(partition by a.语言 order by a.日期) -100, 2) as varchar),'%')
                end 消耗环比,
            新增当日付费总额 "新用户充值($)",
            新用户首日ROI,
            总充值 "总充值($)",
            总充值环比,
            case when 消耗=0 then 0 else round((总充值 - 退款) * 100.0 / 消耗,2) end as 总ROI,
            当月累计消耗 "当月累计消耗($)",
            concat(cast(当月累计消耗完成率 as varchar),'%') 当月累计消耗完成率,
            COALESCE(剩余消耗,0.00) "剩余消耗($)",
            -- remaining_days,
            COALESCE(case when remaining_days = 0.0 then 剩余消耗 else round(剩余消耗 / remaining_days,2)END,0.00) "剩余每日需消耗($)",
            展示量,
            "CPM(千次展现成本:$)",
            点击数,
            concat(cast(点击率 as varchar),'%') 点击率,
            拉新量,
            "CPA(激活成本:$)",
            新增当日付费用户数,
            concat(cast(付费率 as varchar),'%') 付费率,
            付费成本 "付费成本($)"
        from(
            select
              t1.p_date 日期,
              if( language_tb.name in ('中文简体','繁体中文'),'中文',language_tb.name) 语言,
              sum(cost_amount) 消耗,
              sum(money_0) 新增当日付费总额,
              case
                when sum(cost_amount) = 0 then 0
                else round(sum(money_0) * 100.0 / sum(cost_amount), 2)
              end as 新用户首日ROI,
              sum(impression) 展示量,
              case
                when sum(impression) = 0 then 0
                else round(sum(cost_amount) / sum(impression) * 1000.0, 2)
              end as "CPM(千次展现成本:$)",
              sum(click) 点击数,
              case
                when sum(impression) = 0 then 0
                else round(sum(click) * 100.0 / sum(impression), 2)
              end as 点击率,
              sum(newuser_uv) as 拉新量,
              case
                when sum(newuser_uv) = 0 then 0
                else round(sum(cost_amount) * 1.0 / sum(newuser_uv), 2)
              end as "CPA(激活成本:$)",
              sum(new_pay_uv) 新增当日付费用户数,
              case
                when sum(new_pay_uv) = 0 then 0
                else round(sum(new_pay_uv) * 100.0 / sum(newuser_uv), 2)
              end as 付费率,
              case
                when sum(new_pay_uv) = 0 then 0
                else round(sum(cost_amount) * 1.0 / sum(new_pay_uv), 2)
              end as 付费成本,
              EXTRACT( DAY FROM ( DATE_TRUNC('month', t1.p_date) + INTERVAL '1 month' - INTERVAL '1 day' ) - t1.p_date ) :: numeric AS remaining_days
            from(
                select
                  cost_tb.p_date,
                  cost_tb.campaign_id,
                  cost_tb.campaign_name,
                  impression,
                  click,
                  newuser_uv,
                  new_pay_uv,
                  cost_amount,
                  money_0
                from cost_tb
                left join roi_tb on cost_tb.p_date = roi_tb.p_date and cost_tb.campaign_id = roi_tb.campaign_id
            ) t1
            left join "oversea-api_osd_videos" vid_tb on cast(split_part(t1.campaign_name, '_', 5) as varchar) = cast(vid_tb.id as varchar)
            left join "oversea-api_osd_categories" language_tb on cast(vid_tb.category_id as varchar) = cast(language_tb.id as varchar)
            where language_tb.name is not null
            group by t1.p_date,
                if(language_tb.name in ('中文简体','繁体中文'),'中文',language_tb.name)
        ) a
        left join total_cost b on a.日期 = b.p_date and a.语言 = b.语言
        left join total_recharge c on a.日期 = c.日期 and a.语言 = c.语言
    ) a
    where to_char(日期,'YYYY-MM') = TO_CHAR(current_date-1, 'YYYY-MM')
)
-- 俄语
select
    *
from total_all
where 语言 = '俄语'
order by 日期 desc
--英语
select
    *
from total_all
where 语言 = '英语'
order by 日期 desc

--日语
select
  *
from
  total_all
where
  语言 = '日语'
order by
  日期 desc


--韩语
select
  *
from
  total_all
where
  语言 = '韩语'
order by
  日期 desc


--泰语
select
  *
from
  total_all
where
  语言 = '泰语'
order by
  日期 desc


--印尼语
select
  *
from
  total_all
where
  语言 = '印尼语'
order by
  日期 desc


--法语
select
  *
from
  total_all
where
  语言 = '法语'
order by
  日期 desc


--德语
select
  *
from
  total_all
where
  语言 = '德语'
order by
  日期 desc


--西班牙语
select
  *
from
  total_all
where
  语言 = '西班牙语'
order by
  日期 desc


--葡萄牙语
select
  *
from
  total_all
where
  语言 = '葡萄牙语'
order by
  日期 desc

--中文
select
  *
from
  total_all
where
  语言 = '中文'
order by
  日期 desc

--阿拉伯语
select
  *
from
  total_all
where
  语言 = '阿拉伯语'
order by
  日期 DESC

--越南语
select
  *
from
  total_all
where
  语言 = '越南语'
order by
  日期 DESC

--土耳其语
select
  *
from
  total_all
where
  语言 = '土耳其语'
order by
  日期 desc