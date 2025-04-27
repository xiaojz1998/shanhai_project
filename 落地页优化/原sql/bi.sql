----------推广流新增用户明细
----------取出每日的新增用户
with camp_newuser_tb as (
  select
    d_date :: date as p_date,
    country_code,
    upper(os) as os,
    campaign_id,
    uid :: int8 as uid
  from
    public.dwd_user_info
  where
    is_campaign = 1
),
----------推广流新增用户
camp_newuser_info as (
  select
    p_date,
    campaign_id,
    country_code,
    os,
    count(distinct uid) as register_users
  from
    camp_newuser_tb             -- 取出每日的新增用户
  group by
    p_date,
    campaign_id,
    country_code,
    os
),
---------国家数据
----------用于补充信息
country_info as (
  SELECT
    area,
    upper(country_code) as country_code,
    country_name
  FROM
    "v_dim_country_area"
),
---------付费数据
---------维度 日期 ，用户  计算字段：money包括付费和退款

pay_tb as (
  select
    p_date,
    uid,
    sum(money) as money
  from
    (
      select
        to_timestamp(created_at) :: date as p_date,
        uid,
        1.0 * sum("money") / 100 as "money"
      from
        all_order_log
      where
        status = 1
        and environment = 1
      group by
        uid,
        to_timestamp(created_at) :: date
      union all
      select
        to_timestamp(created_at) :: date as p_date,
        uid,
        -1.0 * sum("money") / 100 as "money"
      from
        all_refund_order_log        -- 退款
      where
        status = 1
        and environment = 1
      group by
        uid,
        to_timestamp(created_at) :: date
    ) tmp
  group by
    p_date,
    uid
),
---落地页数据
landing_tb as(
  SELECT
    to_timestamp(created_at) :: date AS p_date,
    campaign_id,
    upper(country_code) as country_code,
    upper(platform) as os,
    count(
      distinct case
        when event = 114 then id
        else null
      end
    ) as landing_view_pv,
    count(
      distinct case
        when event = 114 then ip
        else null
      end
    ) as landing_view_uv,
    count(
      distinct case
        when event = 115 then id
        else null
      end
    ) as landing_click_pv,
    count(
      distinct case
        when event = 115 then ip
        else null
      end
    ) as landing_click_uv,
    sum(
      case
        when event = 133 then enter_time
        else null
      end
    ) as landing_data_time,
    count(
      distinct case
        when event = 133 then id
        else null
      end
    ) as landing_data_time_pv,
    sum(
      case
        when event = 133
        and enter_time < 30000 then enter_time
        else null
      end
    ) as landing_data_time_30s,
    count(
      distinct case
        when event = 133
        and enter_time < 30000 then id
        else null
      end
    ) as landing_data_time_30s_pv,
    sum(
      case
        when event = 127 then enter_time
        else null
      end
    ) as landing_stand_time,
    count(
      distinct case
        when event = 127 then id
        else null
      end
    ) as landing_stand_time_pv,
    sum(
      case
        when event = 127
        and enter_time < 30000 then enter_time
        else null
      end
    ) as landing_stand_time_30s,
    count(
      distinct case
        when event = 127
        and enter_time < 30000 then id
        else null
      end
    ) as landing_stand_time_30s_pv
  from
    (
      select
        created_at,
        campaign_id,
        country_code,
        platform,
        event,
        enter_time,
        id,
        ip,
        case
          when campaign_id = '0'
          or campaign_id = '' then '自然'
          when ad_channel = 'tt' then 'tt'
          when ad_channel in ('fb', 'metaweb') then 'fb'
          when ad_channel = 'apple' then 'apple'
          when ad_channel = 'moloco_int' then 'moloco_int'
          when substr(ad_channel, 1, 12) = 'social_media' then '社媒'
          else '未知'
        end as ad_channel
      FROM
        "web_user_track_log"
      WHERE
        event in (114, 115, 127, 133)
        and to_timestamp(created_at) :: date between '2024-12-21'
        and CURRENT_DATE
    ) as landing_info
  group by
    to_timestamp(created_at) :: date,
    campaign_id,
    upper(country_code),
    upper(platform)
),
----安装新用户数据
install_tb as (
  select
    install_time :: date as p_date,
    campaign_id,
    upper(country_code) as country_code,
    upper(platform) as os,
    count(distinct af_id) as install_cnt
  from
    app_install_event_log
  where
    install_time :: date between '2025-02-22'
    and CURRENT_DATE
  group by
    install_time :: date,
    campaign_id,
    upper(country_code),
    upper(platform)
),
----安装老用户数据
reinstall_tb as (
  select
    install_time :: date as p_date,
    campaign_id,
    upper(country_code) as country_code,
    upper(platform) as os,
    count(distinct af_id) as reinstall_cnt
  from
    user_reattribution_event_log
  where
    event_name = 'reinstall'
    and install_time :: date between '2025-02-22'
    and CURRENT_DATE
  group by
    install_time :: date,
    campaign_id,
    upper(country_code),
    upper(platform)
),
---用户付费数据
camp_pay_info as (
  select
    pay_tb.p_date,
    camp_newuser_tb.campaign_id,
    camp_newuser_tb.country_code,
    camp_newuser_tb.os,
    sum(
      if(
        camp_newuser_tb.uid = pay_tb.uid
        and camp_newuser_tb.p_date = pay_tb.p_date,
        pay_tb.money,
        null
      )
    ) as new_pay_money,
    sum(
      if(
        camp_newuser_tb.uid = pay_tb.uid
        and camp_newuser_tb.p_date < pay_tb.p_date,
        pay_tb.money,
        null
      )
    ) as old_pay_money
  from
    pay_tb
    left join camp_newuser_tb on camp_newuser_tb.uid = pay_tb.uid
  group by
    pay_tb.p_date,
    camp_newuser_tb.campaign_id,
    camp_newuser_tb.country_code,
    camp_newuser_tb.os
),
----消耗数据
cost_tb as (
  select
    to_timestamp(created_at) :: date as p_date,
    campaign_id,
    max(campaign_name) as campaign_name
  from
    ad_cost_data_log
  group by
    to_timestamp(created_at) :: date,
    campaign_id
),
dim_tb as (
  select
    p_date,
    campaign_id,
    country_code,
    os
  from
    landing_tb
  group by
    p_date,
    campaign_id,
    country_code,
    os
)
select
  dim_tb.p_date as "日期",
  dim_tb.os as "平台",
  country_info.country_name as "国家",
  split_part(cost_tb.campaign_name, '_', 5) as "剧ID",
  vid_tb.name as "剧名",
  language_tb.name as "语种",
  case
    when upper(split_part(campaign_name, '_', 8)) = 'G1' then '广州一组'
    when upper(split_part(campaign_name, '_', 8)) = 'G2' then '广州二组'
    when upper(split_part(campaign_name, '_', 8)) = 'G3' then '广州三组'
    when upper(split_part(campaign_name, '_', 8)) = 'G4' then '广州四组'
    when upper(split_part(campaign_name, '_', 8)) = 'H1' then '杭州一组'
    when upper(split_part(campaign_name, '_', 8)) = 'Z1' then '深圳一组'
    when upper(split_part(campaign_name, '_', 8)) = 'Z2' then '深圳二组'
    else '其他'
  end as "投放组",
  split_part(campaign_name, '_', 9) as "投放人员",
  cost_tb.campaign_id as "广告系列ID",
  cost_tb.campaign_name as "广告系列名称",
  landing_view_pv as "落地页展示次数",
  landing_view_uv as "落地页展示人数",
  landing_data_time as "落地页加载时长",
  landing_data_time_pv as "落地页加载次数",
  landing_data_time_30s as "落地页加载时长_删除大于30s及以上异常值",
  landing_data_time_30s_pv as "落地页加载次数_删除大于30s及以上异常值",
  landing_stand_time as "落地页停留时长",
  landing_stand_time_pv as "落地页停留次数",
  landing_stand_time_30s as "落地页停留时长_删除大于30s及以上异常值",
  landing_stand_time_30s_pv as "落地页停留次数_删除大于30s及以上异常值",
  landing_click_pv as "落地页点击次数",
  landing_click_uv as "落地页点击人数",
  (
    COALESCE(install_cnt, 0) + COALESCE(reinstall_cnt, 0)
  ) as "安装设备数",
  install_cnt as "安装的新设备数",
  reinstall_cnt as "安装的老设备数",
  register_users as "激活用户数",
  new_pay_money as "新用户充值金额：减退款",
  old_pay_money as "老用户充值金额：减退款"
from
  dim_tb
  left join landing_tb on dim_tb.p_date = landing_tb.p_date
  and dim_tb.campaign_id = landing_tb.campaign_id
  and dim_tb.country_code = landing_tb.country_code
  and dim_tb.os = landing_tb.os
  left join cost_tb on dim_tb.p_date = cost_tb.p_date
  and dim_tb.campaign_id = cost_tb.campaign_id
  left join camp_newuser_info on dim_tb.p_date = camp_newuser_info.p_date
  and dim_tb.campaign_id = camp_newuser_info.campaign_id
  and dim_tb.country_code = camp_newuser_info.country_code
  and dim_tb.os = camp_newuser_info.os
  left join camp_pay_info on dim_tb.p_date = camp_pay_info.p_date
  and dim_tb.campaign_id = camp_pay_info.campaign_id
  and dim_tb.country_code = camp_pay_info.country_code
  and dim_tb.os = camp_pay_info.os
  left join install_tb on dim_tb.p_date = install_tb.p_date
  and dim_tb.campaign_id = install_tb.campaign_id
  and dim_tb.country_code = install_tb.country_code
  and dim_tb.os = install_tb.os
  left join reinstall_tb on dim_tb.p_date = reinstall_tb.p_date
  and dim_tb.campaign_id = reinstall_tb.campaign_id
  and dim_tb.country_code = reinstall_tb.country_code
  and dim_tb.os = reinstall_tb.os
  left join "oversea-api_osd_videos" vid_tb on cast(
    split_part(cost_tb.campaign_name, '_', 5) as varchar
  ) = cast(vid_tb.id as varchar)
  left join "oversea-api_osd_categories" language_tb on cast(vid_tb.category_id as varchar) = cast(language_tb.id as varchar)
  left join country_info on dim_tb.country_code = country_info.country_code