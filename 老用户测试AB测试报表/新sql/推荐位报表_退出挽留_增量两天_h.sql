------------------------------------------
-- file: 推荐位报表_退出挽留_增量两天_h.sql
-- author: xiaoj
-- time: 2025/4/27 10:37
-- description:
------------------------------------------
set timezone ='UTC-0';
------------------------------------------
--  建表
------------------------------------------
-- 老用户首页播放页退出挽留弹窗
-- drop table if exists public.dw_recommend_playretain_olduser;
create table if not exists dw_recommend_playretain_olduser (
    --
    date date,
    user_type text,
    show_title text,
    area text,
    country_name text,
    ad_channel text,
    user_group text,
    lang_name text,
    registration_period text,
    --
    exposure_users bigint,
    click_users bigint,
    play_users bigint,
    recharge_submission_users bigint,
    episode_unlock_users bigint,
    exposure_times bigint,
    click_times bigint,
    play_times bigint,
    recharge_submission_times bigint,
    episode_unlocks bigint,
    watch_duration_minutes bigint,
    successful_payment_users bigint,
    successful_payment_times bigint,
    total_payment_amount numeric(20,2),
    k_consume_amount numeric(20,2)
);

-- 老用户端内表现播放页退出挽留窗口
drop table if exists dw_recommend_playretain_app_olduser;
create table if not exists dw_recommend_playretain_app_olduser (
    -- 维度
    date date NOT NULL,
    user_type text,
    group_type text,
    area text,
    country_name text,
    ad_channel text,
    lang_name text,
    registration_period text,
    -- 计算字段
    exposure_users bigint,
    play_users bigint,
    submit_recharge_users bigint,
    submit_recharge_times bigint,
    unlock_episodes_users bigint,
    unlock_episodes_times bigint,
    total_watch_time_minutes numeric(20,2),
    successful_recharge_users bigint,
    successful_recharge_times bigint,
    total_recharge_amount numeric(20,2),
    k_consume_amount numeric(20,2),
    daily_active_users bigint,
    retention_next_day bigint,
    retention_3_days bigint,
    retention_7_days bigint
);

-- 老用户一级指标播放也退出挽留窗口
-- drop table if exists dw_recommend_retain_index_olduser;
create table if not exists dw_recommend_retain_index_olduser(
    "实验分组" text,
    "观察日期内曝光人数" bigint,
    "观察日期内充值金额" numeric(15,2),
    "观察日期内活跃天数" bigint
);

------------------------------------------
--  更新
------------------------------------------
drop table if exists tmp.tmp_dw_recommend_playretain_olduser;
create table tmp.tmp_dw_recommend_playretain_olduser as
WITH user_registration AS (
    SELECT
        d_date AS register_date,
        uid::int8 AS uid,
        area,
        country_name,
        ad_channel,
        lang_name,
        CASE
            WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('10','11','12','13','14','15','16','17','18','19') THEN '对照组'
            WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('00','01','02','03','04','05','06','07','08','09') THEN '实验组'
            ELSE NULL
        END AS user_group
    FROM dwd_user_info
),
user_type_info AS (
    SELECT
        d.uid,
        d.d_date AS 日期,
        COALESCE(d.user_type, 'Unknown') AS user_type,
        d.show_title,           -- dim_playretain_user
        ur.area,
        ur.country_name,
        ur.ad_channel,
        ur.user_group,
        ur.lang_name,
        case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
             else '未知' END AS registration_period
    FROM public.dim_playretain_user d
    LEFT JOIN user_registration ur ON d.uid = ur.uid
    -- 增量更新
    -- WHERE d.d_date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    WHERE d.d_date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    -- 限定老用户
    and d.user_type='olduser'
),
exposure_data AS (
    SELECT
        TO_TIMESTAMP(ed.created_at)::date AS date,
        ed.uid,
        uti.user_type,
        CAST(ed.ext_body::json ->> 'show_title' AS text) AS show_title,
        uti.area,
        uti.country_name,
        uti.ad_channel,
        uti.user_group,
        uti.lang_name,
        uti.registration_period
    FROM public.app_user_cover_show_log ed
    LEFT JOIN user_type_info uti ON ed.uid = uti.uid AND TO_TIMESTAMP(ed.created_at)::date = uti.日期
    WHERE event = 111
    -- 增量更新
    -- AND TO_TIMESTAMP(ed.created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    AND TO_TIMESTAMP(ed.created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    AND CAST(ext_body::json ->> 'page' AS int) = 3
    AND ext_body::json ->> 'show_title' = 'playRetain'
),
click_data AS (
    SELECT
        TO_TIMESTAMP(cd.created_at)::date AS date,
        cd.uid,
        event,
        watch_time,
        -- NULLIF(order_id, '')::bigint AS order_id,
        case when order_id like '%SH%' then order_id else CONCAT('SH', order_id) end AS order_id,
        vid,
        eid,
        uti.user_type,
        CAST(cd.ext_body::json ->> 'show_title' AS text) AS show_title,
        uti.area,
        uti.country_name,
        uti.ad_channel,
        uti.user_group,
        uti.lang_name,
        uti.registration_period
    FROM public.app_user_track_log cd
    LEFT JOIN user_type_info uti ON cd.uid = uti.uid AND TO_TIMESTAMP(cd.created_at)::date = uti.日期
    WHERE event IN (112, 1, 192, 191, 2, 13, 14)
    -- 增量更新
    -- AND TO_TIMESTAMP(cd.created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    AND TO_TIMESTAMP(cd.created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    AND CAST(ext_body::json ->> 'page' AS int) = 3
    AND ext_body::json ->> 'show_title' = 'playRetain'
),
payment_data AS (
    SELECT
        TO_TIMESTAMP(pd.created_at)::date AS date,
        pd.uid,
        -- SUBSTRING(order_num FROM 3)::bigint AS order_id,
        order_num,
        money * 1.0 / 100 AS total_payment_amount
    FROM public.all_order_log pd
    WHERE status = 1 AND environment = 1
    -- 增量更新
    -- AND TO_TIMESTAMP(pd.created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    AND TO_TIMESTAMP(pd.created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
),
-- k币消耗信息
k_consume_data as (
    select
        t.date,
        t.uid,
        goods_id,
        goods_sku_id,
        money
    from (select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,
        goods_sku_id,
        money
    from "middle_user_consume_record_00"
    where type = 0
    -- 增量更新
    -- AND TO_TIMESTAMP(created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,
        goods_sku_id,
        money
    from "middle_user_consume_record_01"
    where type = 0
    -- 增量更新
    -- AND TO_TIMESTAMP(created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,
        goods_sku_id,
        money
    from "middle_user_consume_record_02"
    where type = 0
    -- 增量更新
    -- AND TO_TIMESTAMP(created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    and to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,
        goods_sku_id,
        money
    from "middle_user_consume_record_03"
    where type = 0
    -- 增量更新
    -- AND TO_TIMESTAMP(created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,
        goods_sku_id,
        money
    from "middle_user_consume_record_04"
    where type = 0
    -- 增量更新
    -- AND TO_TIMESTAMP(created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day') ) t
),
-- 对k币消耗信息进行聚合
-- 要用event = 191 uid和剧id关联
aggregated_k_consume as (
    select
        cd.date,
        cd.user_type,
        cd.area,
        cd.country_name,
        cd.ad_channel,
        cd.user_group,
        cd.show_title,
        cd.lang_name,
        cd.registration_period,
        sum(money) as k_consume_amount
    from click_data cd
    left join k_consume_data on cd.vid = k_consume_data.goods_id and cd.eid = k_consume_data.goods_sku_id and k_consume_data.date = cd.date and k_consume_data.uid = cd.uid
    and cd.event = 191
    group by cd.date, cd.user_type, cd.area,cd.country_name, cd.ad_channel, cd.user_group,cd.show_title, cd.lang_name, cd.registration_period
),
aggregated_exposure AS (
    SELECT
        date,
        user_type,
        show_title,
        area,
        country_name,
        ad_channel,
        user_group,
        lang_name,
        registration_period,
        COUNT(DISTINCT uid) AS exposure_users,
        COUNT(*) AS exposure_times
    FROM exposure_data
    GROUP BY date, user_type, show_title, area,country_name, ad_channel, user_group,lang_name,registration_period
),
aggregated_click AS (
    SELECT
        date,
        user_type,
        show_title,
        area,
        country_name,
        ad_channel,
        user_group,
        lang_name,
        registration_period,
        COUNT(DISTINCT CASE WHEN event = 112 THEN uid END) AS click_users,
        COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_users,
        COUNT(DISTINCT CASE WHEN event = 192 THEN uid END) AS recharge_submission_users,
        COUNT(DISTINCT CASE WHEN event = 191 THEN uid END) AS episode_unlock_users,
        COUNT(CASE WHEN event = 112 THEN uid END) AS click_times,
        COUNT(CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_times,
        COUNT(CASE WHEN event = 192 THEN uid END) AS recharge_submission_times,
        COUNT(CASE WHEN event = 191 THEN uid END) AS episode_unlocks,
        ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) AS watch_duration_minutes
    FROM click_data
    GROUP BY date, user_type, show_title,area, country_name, ad_channel, user_group,lang_name,registration_period
),
aggregated_payment AS (
    SELECT
        pd.date,
        pd.user_type,
        pd.show_title,
        pd.area,
        pd.country_name,
        pd.ad_channel,
        pd.user_group,
        pd.lang_name,
        pd.registration_period,
        COALESCE(COUNT(DISTINCT cd.uid), 0) AS successful_payment_users,
        COALESCE(COUNT(DISTINCT cd.order_num), 0) AS successful_payment_times,
        COALESCE(SUM(cd.total_payment_amount), 0) AS total_payment_amount
    FROM click_data pd
    LEFT JOIN payment_data cd ON pd.uid = cd.uid AND pd.date = cd.date AND pd.event = 192
    and pd.order_id=cd.order_num
    GROUP BY pd.date, pd.user_type, pd.show_title,pd.area, pd.country_name, pd.ad_channel, pd.user_group,pd.lang_name,pd.registration_period
)
SELECT
    -- 维度
    ae.date,
    cast(ae.user_type as text) as user_type,
    ae.show_title,
    ae.area,
    ae.country_name ,
    ae.ad_channel,
    ae.user_group,
    ae.lang_name,
    ae.registration_period,
    -- 计算字段
    COALESCE(ae.exposure_users, 0) AS exposure_users,
    COALESCE(ac.click_users, 0) AS click_users,
    COALESCE(ac.play_users, 0) AS play_users,
    COALESCE(ac.recharge_submission_users, 0) AS recharge_submission_users,
    COALESCE(ac.episode_unlock_users, 0) AS episode_unlock_users,
    COALESCE(ae.exposure_times, 0) AS exposure_times,
    COALESCE(ac.click_times, 0) AS click_times,
    COALESCE(ac.play_times, 0) AS play_times,
    COALESCE(ac.recharge_submission_times, 0) AS recharge_submission_times,
    COALESCE(ac.episode_unlocks, 0) AS episode_unlocks,
    cast(COALESCE(ac.watch_duration_minutes, 0) as bigint)AS watch_duration_minutes,
    COALESCE(ap.successful_payment_users, 0) AS successful_payment_users,
    COALESCE(ap.successful_payment_times, 0) AS successful_payment_times,
    cast(COALESCE(ap.total_payment_amount, 0) as numeric(20,2)) AS total_payment_amount,
    cast(coalesce(akc.k_consume_amount,0) as numeric(20,2))as k_consume_amount
FROM aggregated_exposure ae
LEFT JOIN aggregated_click ac ON ae.date = ac.date AND ae.user_type = ac.user_type AND ae.show_title = ac.show_title and ae.area = ac.area AND ae.country_name = ac.country_name AND ae.ad_channel = ac.ad_channel AND ae.user_group = ac.user_group and ae.lang_name = ac.lang_name and ae.registration_period = ac.registration_period
LEFT JOIN aggregated_payment ap ON ae.date = ap.date AND ae.user_type = ap.user_type AND ae.show_title = ap.show_title and ae.area = ap.area AND ae.country_name = ap.country_name AND ae.ad_channel = ap.ad_channel AND ae.user_group = ap.user_group and ae.lang_name = ap.lang_name and ae.registration_period = ap.registration_period
left join aggregated_k_consume akc on ae.date = akc.date and ae.user_type = akc.user_type and ae.show_title = akc.show_title and ae.area = akc.area and ae.country_name = akc.country_name and ae.ad_channel = akc.ad_channel and ae.user_group = akc.user_group and ae.lang_name = akc.lang_name and ae.registration_period = akc.registration_period
WHERE ae.user_group IS NOT NULL
ORDER BY ae.date, ae.user_type, ae.show_title,ae.area, ae.country_name, ae.ad_channel, ae.user_group,ae.lang_name,ae.registration_period;

-- 增量
-- delete from public.dw_recommend_playretain_olduser where date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day');
-- insert into public.dw_recommend_playretain_olduser select * from tmp.tmp_dw_recommend_playretain_olduser;
-- 全量
truncate table public.dw_recommend_playretain_olduser;
insert into public.dw_recommend_playretain_olduser select * from tmp.tmp_dw_recommend_playretain_olduser;




-- 端内
drop table if exists tmp.tmp_dw_recommend_playretain_app_olduser;
create table tmp.tmp_dw_recommend_playretain_app_olduser as
WITH user_registration AS (
    SELECT
        d_date AS register_date,
        uid::int8 AS uid,
        area,
        country_name,
        ad_channel,
        lang_name,
        CASE
            WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('10','11','12','13','14','15','16','17','18','19') THEN '对照组'
            WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('00','01','02','03','04','05','06','07','08','09') THEN '实验组'
            ELSE NULL
        END AS user_group
    FROM dwd_user_info
),
user_type_derivation AS (
        SELECT
        d.uid,
        d.d_date AS 日期,
        COALESCE(d.user_type, 'Unknown') AS user_type,
        ur.area,
        ur.country_name,
        ur.ad_channel,
        ur.user_group,
        ur.lang_name,
        case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
             else '未知' END AS registration_period
    FROM public.dim_playretain_user d
    LEFT JOIN user_registration ur ON d.uid = ur.uid
    -- 增量更新
    -- WHERE d.d_date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
    -- 全量更新
    WHERE d.d_date BETWEEN '2025-04-01' AND (current_date+interval '-1 day') and d.user_type='olduser'
    group by d.uid, d.d_date, COALESCE(d.user_type, 'Unknown'), ur.area, ur.country_name, ur.ad_channel, ur.user_group, ur.lang_name,
             case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
             else '未知' END
),
-- k币消耗信息
k_consume_data as (
    select
        t.date,
        t.uid,
        goods_id,
        goods_sku_id,
        money
    from (select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,
        goods_sku_id,
        money
    from "middle_user_consume_record_00"
    where type = 0
    -- 增量更新
    -- and to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,
        goods_sku_id,
        money
    from "middle_user_consume_record_01"
    where type = 0
    -- 增量更新
    -- and to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,
        goods_sku_id,
        money
    from "middle_user_consume_record_02"
    where type = 0
    -- 增量更新
    -- and to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,
        goods_sku_id,
        money
    from "middle_user_consume_record_03"
    where type = 0
    -- 增量更新
    -- and to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,
        goods_sku_id,
        money
    from "middle_user_consume_record_04"
    where type = 0
    -- 增量更新
    -- and to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day') ) t
),
-- 点击信息
pv_counts AS (
    SELECT
        uid,
        to_timestamp(created_at)::date as 日期,
        COUNT(1) AS pv
    FROM "app_user_track_log"
    WHERE
        -- 增量更新
        -- to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- 全量更新
        to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
        AND event  in(1,2,13,14)
    GROUP BY uid, 日期
),
recharge_counts AS (
    SELECT
        uid,
        to_timestamp(created_at)::date as 日期,
        COUNT(1) AS recharge_times
    FROM "app_user_track_log"
    WHERE
        -- 增量更新
        -- to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- 全量更新
        to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
        AND event_name = 'submitRecharge'
    GROUP BY uid, 日期
),
unlock_episodes AS (
    SELECT
        uid,
        to_timestamp(created_at)::date as 日期,
        vid,
        eid,
        COUNT(1) AS unlock_episodes_times
    FROM "app_user_track_log"
    WHERE
        -- 增量更新
        -- to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- 全量更新
        to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
        AND event_name = 'unlockEpisodes' and event = 191
    GROUP BY uid, 日期,vid,eid
),
unlock_episodes_counts as (
    select
        uid,
        日期,
        sum(unlock_episodes_times) as unlock_episodes_times
    from unlock_episodes
    group by uid , 日期
),
k_consume_sum as (
  select
      ue.uid,
      ue.日期,
      sum(money) as k_consume_amount
  from unlock_episodes ue left join k_consume_data k
      on ue.vid = k.goods_id and ue.eid=k.goods_sku_id and ue.uid = k.uid and ue.日期 = k.date
  group by ue.uid, ue.日期
),
watch_time_counts AS (
    SELECT
        uid,
        to_timestamp(created_at)::date as 日期,
        SUM(watch_time) / 60 AS watch_time_minutes
    FROM "app_user_track_log"
    WHERE
        -- 增量更新
        -- to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- 全量更新
        to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
        AND event_name = 'drama_watch_time'
    GROUP BY uid, 日期
),
successful_recharges AS (
    SELECT
        a.uid,
        to_timestamp(created_at)::date as 日期,
        -- COUNT(DISTINCT a.uid) AS successful_recharge_times,
        SUM(a.money)/100.0  AS total_recharge_amount,
        count(distinct SUBSTRING(order_num FROM 3)::bigint) as successful_recharge_times
        -- SUBSTRING(order_num FROM 3)::bigint AS order_id
    from public.all_order_log a
    WHERE
        -- 增量更新
        -- to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- 全量更新
        to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
        AND status = 1
        AND environment = 1
    GROUP BY a.uid, to_timestamp(created_at)::date    --, SUBSTRING(order_num FROM 3)::bigint
),
active_users AS (
    SELECT
        uid,
        d_date AS active_date
    FROM public.dwd_user_active
    WHERE
        -- 增量更新
        -- d_date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- 全量更新
        d_date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    GROUP BY uid,d_date
),
active_day_2 AS (
    SELECT DISTINCT
        d_date AS 活跃日期,
        uid
    FROM public.dwd_user_active
    WHERE
        d_date >=  '2025-04-02'
    GROUP BY uid,d_date
),
active_day_4 AS (
    SELECT DISTINCT
        d_date AS 活跃日期,
        uid
    FROM public.dwd_user_active
    WHERE
        d_date >=  '2025-04-04'
    GROUP BY uid,d_date
),
active_day_8 AS (
    SELECT DISTINCT
        d_date AS 活跃日期,
        uid
    FROM public.dwd_user_active
    WHERE
        d_date >=  '2025-04-08'
    GROUP BY uid,d_date
)

SELECT
    -- 维度
    us.日期 as date,
    cast (us.user_type as text) as user_type,
    us.user_group as group_type,
    us.area,
    us.country_name AS country_name,
    us.ad_channel,
    us.lang_name,
    us.registration_period,
    --
    COUNT(DISTINCT us.uid) AS exposure_users,
    COUNT(DISTINCT CASE WHEN pv_counts.pv IS NOT NULL THEN us.uid END) AS play_users,
    COUNT(DISTINCT CASE WHEN recharge_counts.recharge_times IS NOT NULL THEN us.uid END) AS submit_recharge_users,
    COALESCE(SUM(recharge_counts.recharge_times), 0) AS submit_recharge_times,
    COUNT(DISTINCT CASE WHEN unlock_episodes_counts.unlock_episodes_times IS NOT NULL THEN us.uid END) AS unlock_episodes_users,
    COALESCE(SUM(unlock_episodes_counts.unlock_episodes_times), 0) AS unlock_episodes_times,
    cast(COALESCE(SUM(watch_time_counts.watch_time_minutes), 0) as numeric(20,2)) AS total_watch_time_minutes,
    COUNT(DISTINCT CASE WHEN sr.uid IS NOT NULL THEN us.uid END) AS successful_recharge_users,
--    COUNT(DISTINCT sr.uid) AS 充值成功人数,
    sum(sr.successful_recharge_times)  AS successful_recharge_times,
    cast(COALESCE(SUM(sr.total_recharge_amount),  0) as numeric(20,2))as total_recharge_amount ,
    cast(COALESCE(sum(kcs.k_consume_amount),0) as numeric(20,2))as k_consume_amount,
    COUNT(DISTINCT au.uid) as daily_active_users,
    COUNT(DISTINCT ad2.uid) as retention_next_day,
    COUNT(DISTINCT ad4.uid)  as retention_3_days,
    COUNT(DISTINCT ad8.uid)  as retention_7_days
FROM user_type_derivation us
LEFT JOIN pv_counts ON us.uid = pv_counts.uid AND us.日期 = pv_counts.日期
LEFT JOIN recharge_counts ON us.uid = recharge_counts.uid AND us.日期 = recharge_counts.日期
LEFT JOIN unlock_episodes_counts ON us.uid = unlock_episodes_counts.uid AND us.日期 = unlock_episodes_counts.日期
LEFT JOIN watch_time_counts ON us.uid = watch_time_counts.uid AND us.日期 = watch_time_counts.日期
LEFT JOIN successful_recharges sr ON us.uid = sr.uid AND us.日期 = sr.日期
left join k_consume_sum kcs on us.uid = kcs.uid and us.日期 = kcs.日期
LEFT JOIN active_users au ON us.uid = au.uid AND au.active_date = us.日期
LEFT JOIN active_day_2 ad2 ON us.uid = ad2.uid AND ad2.活跃日期 = us.日期 + INTERVAL '1 day'
LEFT JOIN active_day_4 ad4 ON us.uid = ad4.uid AND ad4.活跃日期 = us.日期 + INTERVAL '3 day'
LEFT JOIN active_day_8 ad8 ON us.uid = ad8.uid AND ad8.活跃日期 = us.日期 + INTERVAL '7 day'
where us.user_group is not null
GROUP BY us.日期,
         us.user_type,
         us.user_group,
         us.area,
         us.country_name,
         us.ad_channel,
         us.lang_name,
         us.registration_period
ORDER BY us.日期, us.user_type;

-- 增量更新
-- delete from public.dw_recommend_playretain_app_olduser where date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day');
-- insert into public.dw_recommend_playretain_app_olduser select * from tmp.tmp_dw_recommend_playretain_app_olduser;

-- 全量更新
truncate table public.dw_recommend_playretain_app_olduser;
insert into public.dw_recommend_playretain_app_olduser select * from tmp.tmp_dw_recommend_playretain_app_olduser;


drop table if exists tmp.tmp_dw_recommend_retain_index_olduser;
create table tmp.tmp_dw_recommend_retain_index_olduser as
WITH user_selection AS (
    SELECT DISTINCT
           uid,
           CASE
            WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('10','11','12','13','14','15','16','17','18','19') THEN '对照组'
            WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('00','01','02','03','04','05','06','07','08','09') THEN '实验组'
            ELSE NULL
            END AS type,
           to_timestamp(created_at)::date as 日期
    FROM "app_user_cover_show_log"
    WHERE
      to_timestamp(created_at)::date between '2025-04-01' and  (CURRENT_DATE - INTERVAL '1 day')
      AND event_name = 'drama_cover_show'
      AND CAST(ext_body::json ->> 'page' AS int) = 3
      AND ext_body::json ->> 'show_title' = 'playRetain'

),
first_exposure AS (
    SELECT
           uid,
           type,
           MIN(日期) as first_exposure_date
    FROM user_selection
    GROUP BY uid,type
),
successful_recharges AS (
  SELECT
         a.uid,
         to_timestamp(created_at)::date as 日期,
         SUM(a.money) AS total_recharge_amount
  FROM public.all_order_log a
  JOIN first_exposure fe ON a.uid = fe.uid
  WHERE to_timestamp(created_at)::date >= fe.first_exposure_date
      and to_timestamp(created_at)::date between '2025-04-01' and (CURRENT_DATE - INTERVAL '1 day')
      AND status = 1
      AND environment = 1
  GROUP BY a.uid, 日期
),
user_active_days AS (
  SELECT
         a.uid,
         COUNT(DISTINCT to_timestamp(a.created_at)::date) AS user_active_days
  FROM "app_user_track_log" a
  JOIN first_exposure fe ON a.uid = fe.uid
  WHERE event IN (1, 16)
    AND to_timestamp(created_at)::date >= fe.first_exposure_date
    and to_timestamp(created_at)::date between '2025-04-01' and (CURRENT_DATE - INTERVAL '1 day')
   GROUP BY a.uid
),

combined_data AS (
  SELECT
         us.uid,
         us.type,
         COALESCE(SUM(sr.total_recharge_amount), 0) AS total_recharge_amount,
         uad.user_active_days
  FROM first_exposure us
  LEFT JOIN successful_recharges sr ON us.uid = sr.uid
  LEFT JOIN user_active_days uad ON us.uid = uad.uid
  GROUP BY us.uid, us.type
  , uad.user_active_days
),
user_type_info AS (
    SELECT
        d.uid,
        d.d_date AS 日期,
        COALESCE(d.user_type, 'Unknown') AS user_type,
        ur.type
    FROM public.dim_playretain_user d
    LEFT JOIN user_selection ur ON d.uid = ur.uid
     WHERE d.d_date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    and d.user_type='olduser'
)
SELECT
       cd.type as 实验分组,
       COUNT(DISTINCT cd.uid) AS 观察日期内曝光人数,
       cast(SUM(cd.total_recharge_amount)/100.0 as numeric(15,2)) AS 观察日期内充值金额,
       SUM(cd.user_active_days) AS 观察日期内活跃天数
FROM combined_data cd
JOIN user_type_info uti ON cd.uid = uti.uid
where cd.type is not null
GROUP BY cd.type
ORDER BY cd.type;

-- 全量更新
truncate table public.dw_recommend_retain_index_olduser;
insert into public.dw_recommend_retain_index_olduser select * from tmp.tmp_dw_recommend_retain_index_olduser;