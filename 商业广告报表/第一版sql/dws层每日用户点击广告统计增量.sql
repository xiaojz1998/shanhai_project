set timezone ='UTC-0';
-- 增量表
delete from tmp.tmp_dws_traffic_user_ad_log_1d where d_date>= (current_date+interval '-2 day')::date;
insert into tmp.tmp_dws_traffic_user_ad_log_1d
with tmp_user_ad_log as (
    -- 埋点表和性能埋点表 取到广告相关条
    select
        uid,
        created_at,
        to_timestamp(created_at)::date as d_date,
        country_code,
        event,
        event_name,
        case when event in (39,36) then '签到广告'
            when event = 262 then '开屏/插屏广告'
            when event = 5 then '剧集解锁广告'
            when event = 41 then '阶梯广告'
            when event = 266 then 'firefly广告'
            when event = 269 then 'adcloud广告' end as ad_type
    from app_user_track_log
    where to_timestamp(created_at)::date >= '2024-09-01'    -- 2024/9/14 有广告数据
         and (event in (262,266,269,39,36,5) or (event = 41 and GET_JSON_OBJECT(ext_body,'$.task') = 'Watch Ads' ))
         and to_timestamp(created_at)::date >= (current_date+interval '-2 day')::date

    union all

     select
        uid,
        created_at,
        to_timestamp(created_at)::date as d_date,
        country_code,
        event,
        event_name,
        case when type = '0' then '阶梯广告'
            when event = '1' or event = '2' then '签到广告'
            when event = '3' then '剧集解锁广告' end as ad_type
    from app_performance_event_log
    where to_timestamp(created_at)::date >= '2024-10-15'    -- 2024/10/15 有广告数据
        and event in (6,7,8,9) and type in ('0','1','2','3')
        and to_timestamp(created_at)::date >= (current_date+interval '-2 day')::date
),
    country_info as (
        -- 补全 国家名和区域
        SELECT area,
            country_code,
            country_name
        FROM "v_dim_country_area"
),
    user_info as (
        -- 补全 用户信息 语言和os
        -- 用于判断是否是新增用户
        select
            d_date:: date as reg_date,
            uid::int8,        --text
            -- country_code,
            lang,
            lang_name,
            os
        from public.dwd_user_info
),
    user_pay_info as (
        -- 用于判断是否是付费用户
        SELECT
            uid,
            to_timestamp(created_at)::date as pay_date
        FROM all_order_log
        WHERE to_timestamp(created_at)::date >= '2024-07-01'
          AND environment = 1
          AND status = 1
        group by uid,to_timestamp(created_at)::date
)
select
    uid,
    d_date,
    country_code,
    event,
    event_name,
    ad_type,
    country_name,
    area,
    lang,
    lang_name,
    os,
    count(*) as pv,
    is_new_user,
    is_pay_user
from (select
    t1.uid,
    t1.created_at,
    t1.d_date,
    t1.country_code,
    t1.event,
    t1.event_name,
    t1.ad_type,
    t2.country_name,
    t2.area,
    t3.lang,
    t3.lang_name,
    t3.os,
    case when t1.d_date > t3.reg_date then 0 else 1 end as is_new_user,
    case when t4.pay_date is null then 0 else 1 end as is_pay_user
from tmp_user_ad_log t1
left join country_info t2 on  t1.country_code = t2.country_code
left join user_info t3 on t1.uid = t3.uid
left join user_pay_info t4 on t1.uid = t4.uid and t1.d_date > t4.pay_date
group by t1.uid,t1.created_at,t1.d_date,t1.country_code,t1.event,t1.event_name,
         t1.ad_type,t2.country_name,t2.area,t3.lang,t3.lang_name,t3.os,
         case when t1.d_date > t3.reg_date then 0 else 1 end,
         case when t4.pay_date is null then 0 else 1 end ) t5
group by t5.uid,t5.d_date,t5.country_code,t5.event,t5.event_name,t5.ad_type,
         t5.country_name,t5.area,t5.lang,t5.lang_name,t5.os,
         t5.is_new_user,t5.is_pay_user;

delete from public.dws_traffic_user_ad_log_1d where d_date>= (current_date+interval '-2 day')::date;
insert into public.dws_traffic_user_ad_log_1d
select * from tmp.tmp_dws_traffic_user_ad_log_1d where d_date>= (current_date+interval '-2 day')::date;