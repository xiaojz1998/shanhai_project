------------------------------------------
-- file: 投放落地页转化漏斗报表.sql
-- author: xiaoj
-- time: 2025/5/16 16:23
-- description:
------------------------------------------

set timezone ='UTC-0';
-- 建表
-- drop table if exists public.ads_adinput_landing_page_conversion_funnel;
CREATE TABLE if not exists public.ads_adinput_landing_page_Conversion_funnel(
    -- 维度
    "日期" date,
    "平台" text,
    "国家" text,
    "剧ID" text,
    "剧名" text,
    "语种" text,
    "投放组" text,
    "投放人员" text,
    "广告系列ID" text,
    "广告系列名称" text,
    -- 计算字段
    "自动落地页展示次数" bigint ,    -- 拆分自动手动
    "自动落地页展示人数" bigint ,    -- 拆分自动手动
    "手动落地页展示次数" bigint,     -- 拆分自动手动
    "手动落地页展示人数" bigint,     -- 拆分自动手动
    "落地页加载时长" bigint,
    "落地页加载次数" bigint,
    "落地页加载时长_删除大于30s及以上异常值" bigint,
    "落地页加载次数_删除大于30s及以上异常值" bigint,
    "落地页停留时长" bigint,
    "落地页停留次数" bigint,
    "落地页停留时长_删除大于30s及以上异常值" bigint,
    "落地页停留次数_删除大于30s及以上异常值" bigint,
    "自动落地页点击次数" bigint,
    "自动落地页点击人数" bigint,
    "手动落地页点击次数" bigint,
    "手动落地页点击人数" bigint,
    "安装设备数" bigint,
    "安装的新设备数" bigint,
    "安装的老设备数" bigint,
    "激活用户数" bigint,
    "新用户充值金额：减退款" numeric(22,4),
    "老用户充值金额：减退款" numeric(22,4)
);
-- 更新
drop table if exists public.tmp_ads_adinput_landing_page_conversion_funnel;
create table public.tmp_ads_adinput_landing_page_conversion_funnel as
-- 更新
----------推广流新增用户明细
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
    camp_newuser_tb
  group by
    p_date,
    campaign_id,
    country_code,
    os
),
---------国家数据
country_info as (
  SELECT
    area,
    upper(country_code) as country_code,
    country_name
  FROM
    "v_dim_country_area"
),
---------付费数据
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
        all_refund_order_log
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
---增加自动、手动展示和点击指标
landing_tb as(
  SELECT
    to_timestamp(created_at) :: date AS p_date,
    campaign_id,
    upper(country_code) as country_code,
    upper(platform) as os,
--      count(
--       distinct case
--         when event = 114 then id
--         else null
--       end
--     ) as landing_view_pv,
--     count(
--       distinct case
--         when event = 114 then ip
--         else null
--       end
--     ) as landing_view_uv,
    count(
      distinct case
        when event = 114 and get_json_object(ext_body, '$.reason')='0' and get_json_object(ext_body, '$.is_optimize') = '1'then id
        else null
      end
    ) as auto_landing_view_pv,
    count(
      distinct case
        when event = 114 and get_json_object(ext_body, '$.reason')='0' and get_json_object(ext_body, '$.is_optimize') = '1'then ip
        else null
      end
    ) as auto_landing_view_uv,
    count(
      distinct case
        when event = 114 and get_json_object(ext_body, '$.reason')='1' and get_json_object(ext_body, '$.is_optimize') = '1'then id
        else null
      end
    ) as manual_landing_view_pv,
    count(
      distinct case
        when event = 114 and get_json_object(ext_body, '$.reason')='1' and get_json_object(ext_body, '$.is_optimize') = '1'then ip
        else null
      end
    ) as manual_landing_view_uv,
--     count(
--       distinct case
--         when event = 115 then id
--         else null
--       end
--     ) as landing_click_pv,
--     count(
--       distinct case
--         when event = 115 then ip
--         else null
--       end
--     ) as landing_click_uv,
    count(
      distinct case
        when event = 127 and get_json_object(ext_body, '$.reason')='0' and get_json_object(ext_body, '$.is_optimize') = '1'
            and get_json_object(ext_body, '$.go_page_url') not like 'https://san-api.stardust-tv.com%' then id
        else null
      end
    ) as auto_landing_click_pv,
    count(
      distinct case
        when event = 127 and get_json_object(ext_body, '$.reason')='0' and get_json_object(ext_body, '$.is_optimize') = '1'
            and get_json_object(ext_body, '$.go_page_url') not like 'https://san-api.stardust-tv.com%' then ip
        else null
      end
    ) as auto_landing_click_uv,
    count(
      distinct case
        when event = 127 and get_json_object(ext_body, '$.reason')='1' and get_json_object(ext_body, '$.is_optimize') = '1'
            and get_json_object(ext_body, '$.go_page_url') not like 'https://san-api.stardust-tv.com%' then id
        else null
      end
    ) as manual_landing_click_pv,
    count(
      distinct case
        when event = 127 and get_json_object(ext_body, '$.reason')='1' and get_json_object(ext_body, '$.is_optimize') = '1'
            and get_json_object(ext_body, '$.go_page_url') not like 'https://san-api.stardust-tv.com%' then ip
        else null
      end
    ) as manual_landing_click_uv,
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
        ext_body,
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
        -- and to_timestamp(created_at) :: date between '2025-04-15'
        -- and CURRENT_DATE
        and to_timestamp(created_at) :: date BETWEEN  (current_date+interval '-2 day') and current_date

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
    dwd_app_install_event_log
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
     dwd_user_reattribution_event_log
  where
    (event_name = 'reinstall' or event_name = 're-attribution')
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
      case when camp_newuser_tb.uid = pay_tb.uid and camp_newuser_tb.p_date = pay_tb.p_date then pay_tb.money else null end
    ) as new_pay_money,
    sum(
      case when camp_newuser_tb.uid = pay_tb.uid and camp_newuser_tb.p_date < pay_tb.p_date then pay_tb.money else null end
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
  -- 维度
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
  -- 计算字段
--   landing_view_pv as "落地页展示次数",
--   landing_view_uv as "落地页展示人数",
  auto_landing_view_pv as "自动落地页展示次数",
  auto_landing_view_uv as "自动落地页展示人数",
  manual_landing_view_pv as "手动落地页展示次数",
  manual_landing_view_uv as "手动落地页展示人数",
  landing_data_time as "落地页加载时长",
  landing_data_time_pv as "落地页加载次数",
  landing_data_time_30s as "落地页加载时长_删除大于30s及以上异常值",
  landing_data_time_30s_pv as "落地页加载次数_删除大于30s及以上异常值",
  landing_stand_time as "落地页停留时长",
  landing_stand_time_pv as "落地页停留次数",
  landing_stand_time_30s as "落地页停留时长_删除大于30s及以上异常值",
  landing_stand_time_30s_pv as "落地页停留次数_删除大于30s及以上异常值",
--   landing_click_pv as "落地页点击次数",
--   landing_click_uv as "落地页点击人数",
  auto_landing_click_pv as "自动落地页点击次数",
  auto_landing_click_uv as "自动落地页点击人数",
  manual_landing_click_pv as "手动落地页点击次数",
  manual_landing_click_uv as "手动落地页点击人数",
  (
    COALESCE(install_cnt, 0) + COALESCE(reinstall_cnt, 0)
  ) as "安装设备数",
  install_cnt as "安装的新设备数",
  reinstall_cnt as "安装的老设备数",
  register_users as "激活用户数",
  cast(new_pay_money as numeric(22,4)) as "新用户充值金额：减退款",
  cast(old_pay_money as numeric(22,4)) as "老用户充值金额：减退款"
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
  left join country_info on dim_tb.country_code = country_info.country_code;

delete from public.ads_adinput_landing_page_conversion_funnel where 日期 BETWEEN  (current_date+interval '-2 day') and CURRENT_DATE ;
insert into public.ads_adinput_landing_page_conversion_funnel select * from tmp_ads_adinput_landing_page_conversion_funnel;
