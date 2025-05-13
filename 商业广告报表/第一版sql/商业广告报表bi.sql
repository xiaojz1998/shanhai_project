-----------------------------------------------------------------
-- 分广告场景数据
-----------------------------------------------------------------
-- 建表
set timezone ='UTC-0';
drop table if exists public.ads_traffic_ad_type_statistics;
create table if not exists public.ads_traffic_ad_type_statistics
(
    d_date date,
    area text,
    country_name text,
    lang_name text,
    os text,
    ad_type text,
    total_ad_click_uv bigint,
    total_ad_click_pv bigint,
    total_ad_click_new_uv bigint,
    total_ad_click_new_pv bigint,
    total_ad_click_old_uv bigint,
    total_ad_click_old_pv bigint,
    total_ad_click_pay_uv bigint,
    total_ad_click_pay_pv bigint,
    total_ad_click_no_pay_uv bigint,
    total_ad_click_no_pay_pv bigint,
    total_ads_load_succeed_uv bigint,
    total_ads_load_succeed_pv bigint,
    total_ads_load_fail_uv bigint,
    total_ads_load_fail_pv bigint,
    ads_load_fail_rate numeric(20,2),
    total_ads_show_succeed_uv bigint,
    total_ads_show_succeed_pv bigint,
    total_ads_show_fail_uv bigint,
    total_ads_show_fail_pv bigint,
    ads_show_fail_rate numeric(20,2)
);
-- 注入
truncate table public.ads_traffic_ad_type_statistics;
insert into public.ads_traffic_ad_type_statistics
select
    d_date,
    coalesce(area, 'UNKNOWN'),
    coalesce(country_name, 'UNKNOWN'),
    coalesce(lang_name, 'UNKNOWN'),
    coalesce(os, 'UNKNOWN'),
    ad_type,
    sum(case when event not in(6,7,8,9) then 1 else 0 end ) as total_ad_click_uv,
    sum(case when event not in(6,7,8,9) then pv else 0 end ) as total_ad_click_pv,
    sum(case when event not in(6,7,8,9) and is_new_user = 1 then 1 else 0 end ) as total_ad_click_new_uv,
    sum(case when event not in(6,7,8,9) and is_new_user = 1 then pv else 0 end ) as total_ad_click_new_pv,
    sum(case when event not in(6,7,8,9) and is_new_user = 0 then 1 else 0 end ) as total_ad_click_old_uv,
    sum(case when event not in(6,7,8,9) and is_new_user = 0 then pv else 0 end ) as total_ad_click_old_pv,
    sum(case when event not in(6,7,8,9) and is_pay_user = 1 then 1 else 0 end ) as total_ad_click_pay_uv,
    sum(case when event not in(6,7,8,9) and is_pay_user = 1 then pv else 0 end ) as total_ad_click_pay_pv,
    sum(case when event not in(6,7,8,9) and is_pay_user = 0 then 1 else 0 end ) as total_ad_click_no_pay_uv,
    sum(case when event not in(6,7,8,9) and is_pay_user = 0 then pv else 0 end ) as total_ad_click_no_pay_pv,
    sum(case when event = 6 then 1 else 0 end) as total_ads_load_succeed_uv,
    sum(case when event = 6 then pv else 0 end ) as total_ads_load_succeed_pv,
    sum(case when event = 7 then 1 else 0 end ) as total_ads_load_fail_uv,
    sum(case when event = 7 then pv else 0 end ) as total_ads_load_fail_pv,
    case when sum(case when event = 7 then pv else 0 end )+ sum(case when event = 6 then pv else 0 end ) != 0
        then 1.0*sum(case when event = 7 then pv else 0 end )/ (sum(case when event = 7 then pv else 0 end )+ sum(case when event = 6 then pv else 0 end ))
        else null end as ads_load_fail_rate,
    sum(case when event = 8 then 1 else 0 end ) as total_ads_show_succeed_uv,
    sum(case when event = 8 then pv else 0 end ) as total_ads_show_succeed_pv,
    sum(case when event = 9 then 1 else 0 end ) as total_ads_show_fail_uv,
    sum(case when event = 9 then pv else 0 end ) as total_ads_show_fail_pv,
    case when sum(case when event = 9 then pv else 0 end )+ sum(case when event = 8 then pv else 0 end ) != 0
        then 1.0*sum(case when event = 9 then pv else 0 end )/ (sum(case when event = 9 then pv else 0 end )+ sum(case when event = 8 then pv else 0 end ))
        else null end as ads_show_fail_rate
from dws_traffic_user_ad_log_1d
where d_date >= '2024-09-01' and d_date <= current_date-1            -- 测试日期
group by d_date,
         coalesce(area, 'UNKNOWN'),
         coalesce(country_name, 'UNKNOWN'),
         coalesce(lang_name, 'UNKNOWN'),
         coalesce(os, 'UNKNOWN'),
         ad_type;


-----------------------------------------------------------------
-- 广告汇总数据
-----------------------------------------------------------------
set timezone ='UTC-0';
drop table if exists public.ads_traffic_ad_statistics;
create table if not exists public.ads_traffic_ad_statistics
(
    d_date date,
    area text,
    country_name text,
    lang_name text,
    os text,
    total_ad_click_uv bigint,
    total_ad_click_pv bigint,
    total_checkin_ad_click_uv bigint,
    total_checkin_ad_click_pv bigint,
    total_ladder_ad_click_uv bigint,
    total_ladder_ad_click_pv bigint,
    total_unlock_ad_click_uv bigint,
    total_unlock_ad_click_pv bigint,
    total_ad_watch_finish_uv bigint,
    total_ad_watch_finish_pv bigint,
    total_firefly_ad_click_uv bigint,
    total_firefly_ad_click_pv bigint,
    total_adcloud_ad_click_uv bigint,
    total_adcloud_ad_click_pv bigint,
    total_ads_load_succeed_uv bigint,
    total_ads_load_succeed_pv bigint,
    total_ads_load_fail_uv bigint,
    total_ads_load_fail_pv bigint,
    ads_load_fail_rate numeric(20,2),
    total_ads_show_succeed_uv bigint,
    total_ads_show_succeed_pv bigint,
    total_ads_show_fail_uv bigint,
    total_ads_show_fail_pv bigint,
    ads_show_fail_rate numeric(20,2)
);
truncate table public.ads_traffic_ad_statistics;
insert into public.ads_traffic_ad_statistics
select
    d_date,
    coalesce(area, 'UNKNOWN'),
    coalesce(country_name, 'UNKNOWN'),
    coalesce(lang_name, 'UNKNOWN'),
    coalesce(os, 'UNKNOWN'),
    sum(case when event in (262,266,269,39,36,5,41) then 1 else 0 end ) as total_ad_click_uv,
    sum(case when event in (262,266,269,39,36,5,41) then pv else 0 end ) as total_ad_click_pv,
    sum(case when event = 39 or event = 36 then 1 else 0 end ) as total_checkin_ad_click_uv,
    sum(case when event = 39 or event = 36 then pv else 0 end ) as total_checkin_ad_click_pv,
    sum(case when event = 41 then 1 else 0 end ) as total_ladder_ad_click_uv,
    sum(case when event = 41 then pv else 0 end ) as total_ladder_ad_click_pv,
    sum(case when event = 5 then 1 else 0 end) as total_unlock_ad_click_uv,
    sum(case when event = 5 then pv else 0 end) as total_unlock_ad_click_pv,
    sum(case when event = 262 then 1 else 0 end ) as total_ad_watch_finish_uv,
    sum(case when event = 262 then pv else 0 end ) as total_ad_watch_finish_pv,
    sum(case when event = 266 then 1 else 0 end ) as total_firefly_ad_click_uv,
    sum(case when event = 266 then pv else 0 end ) as total_firefly_ad_click_pv,
    sum(case when event = 269 then 1 else 0 end ) as total_adcloud_ad_click_uv,
    sum(case when event = 269 then pv else 0 end ) as total_adcloud_ad_click_pv,
    sum(case when event =6 then 1 else 0 end ) as total_ads_load_succeed_uv,
    sum(case when event =6 then pv else 0 end ) as total_ads_load_succeed_pv,
    sum(case when event =7 then 1 else 0 end ) as total_ads_load_fail_uv,
    sum(case when event =7 then pv else 0 end ) as total_ads_load_fail_pv,
    case when sum(case when event =7 then pv else 0 end )+ sum(case when event =6 then pv else 0 end ) != 0
        then 1.0*sum(case when event =7 then pv else 0 end )/ (sum(case when event =7 then pv else 0 end )+ sum(case when event =6 then pv else 0 end ))
        else null end as ads_load_fail_rate,
    sum(case when event =8 then 1 else 0 end ) as total_ads_show_succeed_uv,
    sum(case when event =8 then pv else 0 end ) as total_ads_show_succeed_pv,
    sum(case when event =9 then 1 else 0 end ) as total_ads_show_fail_uv,
    sum(case when event =9 then pv else 0 end ) as total_ads_show_fail_pv,
    case when sum(case when event =9 then pv else 0 end )+ sum(case when event =8 then pv else 0 end ) != 0
        then 1.0*sum(case when event =9 then pv else 0 end )/ (sum(case when event =9 then pv else 0 end )+ sum(case when event =8 then pv else 0 end ))
        else null end as ads_show_fail_rate
from dws_traffic_user_ad_log_1d
where d_date >= '2024-09-01' and d_date <= current_date-1                -- 测试日期
group by d_date,
         coalesce(area, 'UNKNOWN'),
         coalesce(country_name, 'UNKNOWN'),
         coalesce(lang_name, 'UNKNOWN'),
         coalesce(os, 'UNKNOWN');
