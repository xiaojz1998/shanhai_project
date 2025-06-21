---------------------------------------------
-- File: 推荐位报表_老用户扩量_增量两天_h_v1.sql
-- Time: 2025/6/19 11:19
-- User: xiaoj
-- Description:  
---------------------------------------------
set timezone ='UTC-0';
------------------------------------------
--  建表
------------------------------------------
-- 老用户首页推荐位
-- drop table if exists public.dw_recommend_home_olduser_scale_up;
CREATE TABLE if not exists public.dw_recommend_home_olduser_scale_up (
    -- 维度
    d_date date,
    user_type text,
    recommend_id text,
    recommend_name text,
    area text ,
    country_name text,
    ad_channel text,
    user_group text,
    lang_name text ,
    registration_period text ,
    -- 计算字段
    exposure_uv bigint,
    click_uv bigint,
    play_uv bigint,
    recharge_submission_uv bigint,
    episode_unlock_uv bigint,
    exposure_pv bigint,
    click_pv bigint,
    episode_play bigint,
    recharge_submission_pv bigint,
    episode_unlocks bigint,
    watch_duration_minutes bigint,
    successful_payment_uv bigint,
    successful_payment_pv bigint,
    total_payment_amount numeric(20,2),
    k_consume_amount numeric(20,2)
);

-- 老用户端内表现
-- drop table if exists public.dw_recommend_home_app_olduser_scale_up;
CREATE TABLE if not exists public.dw_recommend_home_app_olduser_scale_up (
    -- 维度
    date date NOT NULL,
    user_type text,
    group_type text,
    area text,
    country_name text,
    ad_channel text,
    lang_name text ,
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


-- 老用户一级指标
--drop table if exists public.dw_recommend_home_index_olduser_scale_up;
CREATE TABLE if not exists public.dw_recommend_home_index_olduser_scale_up (
    "实验分组" text,
    "观察日期内曝光人数" bigint,
    "观察日期内充值金额" numeric(20,2),
    "观察日期内活跃天数" bigint
);

------------------------------------------
--  更新
------------------------------------------
drop table if exists tmp.tmp_dw_recommend_home_olduser_scale_up;
create table tmp.tmp_dw_recommend_home_olduser_scale_up as
-- 获取用户曝光行为
-- 维度：uid 日期
-- 补充属性
with tmp_user_exposure as (
    select
        a.uid
        , to_timestamp(a.created_at)::date as d_date                    -- 日志日期
        , a.model_id::text as recommend_id
        , c.chinese_name as recommend_name
        , min(to_timestamp(a.created_at)) as min_exposure_time          -- 用户当天推荐位最小曝光时间戳
        , b.d_time::timestamp as register_timestamp
        , b.country_name
        , b.area
        , b.lang_name
        , b.ad_channel
        , CASE
            WHEN a.uid::bigint % 100 >= 10 and a.uid::bigint %100 <= 59 THEN '对照组'
            WHEN (a.uid::bigint % 100 >= 0 and a.uid::bigint %100 <= 9) or (a.uid::bigint %100 >= 60 and a.uid::bigint %100 <= 99) THEN '实验组'
            ELSE NULL
            END AS user_group            -- 对uid进行分组
    from public.app_user_cover_show_log a
    left join dwd_user_info b on a.uid = b.uid::bigint                          -- 关联用户信息
    inner join "oversea-api_osd_recommend" c on a.model_id::int = c.id   -- 关联推荐位表 用于过滤推荐位
    where 1=1
        and event = 111                                                 -- 曝光埋点
        and to_timestamp(a.created_at)::date = '${dt}'::date            -- 限制时间
        and CAST(ext_body::json ->> 'page' AS int) = 1                  -- 页数
        and c.user_newuser_alg_status = 1                               -- 应用新用户推荐策略的推荐位
        -- and c.chinese_name not like '%泰语%'                          -- 泰语不扩量
    group by a.uid
           , to_timestamp(a.created_at)::date
           , a.model_id
           , c.chinese_name
           , b.d_time::timestamp
           , b.country_name
           , b.area
           , b.lang_name
           , b.ad_channel
)
-- 获取用户曝光行为、用户信息
-- 维度：uid 日期
-- 要处理null的维度
, tmp_user_exposure_info as (
    select
        uid
        , d_date
        , recommend_id
        , recommend_name
        , CASE WHEN register_timestamp IS NULL THEN '未知'
            WHEN min_exposure_time <= (register_timestamp + INTERVAL '24 hours') THEN '新用户'
            ELSE '老用户' END AS user_type
        , coalesce(area,  '未知') as area
        , coalesce(country_name, '未知') as country_name
        , coalesce(ad_channel, '未知') as ad_channel
        , coalesce(user_group, '未知') as user_group
        , coalesce(lang_name, '未知') as lang_name
        , case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
            when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
            when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
            when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
            else '未知' END AS registration_period
    from tmp_user_exposure
    where register_timestamp IS not NULL and min_exposure_time > (register_timestamp + INTERVAL '24 hours') -- 限制老用户
)
, tmp_exposure_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , b.user_type
        , b.area
        , b.country_name
        , b.ad_channel
        , b.user_group
        , b.recommend_id
        , b.recommend_name
        , b.lang_name
        , b.registration_period
    from public.app_user_cover_show_log a
    inner join tmp_user_exposure_info b on a.uid = b.uid and to_timestamp(a.created_at)::date = b.d_date and a.model_id::text = b.recommend_id-- 限制当天有曝光的
    where event = 111                                                   -- 曝光埋点
        and user_type = '老用户'                                         -- 限制老用户
        and to_timestamp(a.created_at)::date = '${dt}'::date
        and CAST(ext_body::json ->> 'page' AS int) = 1                  -- 页数
)
, tmp_click_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , event
        , case when order_id like '%SH%' then order_id else CONCAT('SH', order_id) end as order_id  -- 用于匹配支付
        , watch_time
        -- , money::numeric(20,2) as money                 -- 用于计算预估广告收入
        , vid                   -- k币
        , eid                   -- k币
        , recommend_id
        , recommend_name
        --, CAST(a.ext_body::json ->> 'type' AS text) as type         -- 用于看广告解锁
        , b.user_type
        , b.area
        , b.country_name
        , b.ad_channel
        , b.user_group
        , b.lang_name
        , b.registration_period
    from public.app_user_track_log a
    inner join tmp_user_exposure_info b on a.uid = b.uid and to_timestamp(a.created_at)::date = b.d_date and NULLIF(a.column1, '')::text = b.recommend_id
    where event in (112, 1, 192, 191, 2, 13, 14)                    -- 取的事件
        and to_timestamp(a.created_at)::date = '${dt}'::date       -- 限制时间
        and CAST(ext_body::json ->> 'page' AS int) = 1                  -- 页数
        and (NULLIF(a.column1, '') ~ '^\d+$')
)
-- 支付信息
, tmp_payment_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , order_num
        , money * 1.0 / 100 AS total_payment_amount
    from public.all_order_log a
    where status = 1                                                    -- 支付成功
        and environment = 1                                             -- 生产环境
        and to_timestamp(a.created_at)::date = '${dt}'::date       -- 限制时间
)
-- k币消耗信息
, tmp_k_consume_data as (
    select
        t.d_date
        , t.uid
        , goods_id
        , goods_sku_id
        , money
    from (select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_00"
    where type = 0
        and to_timestamp(created_at)::date = '${dt}'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_01"
    where type = 0
        and to_timestamp(created_at)::date = '${dt}'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_02"
    where type = 0
        and to_timestamp(created_at)::date = '${dt}'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_03"
    where type = 0
        and to_timestamp(created_at)::date = '${dt}'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_04"
    where type = 0
        and to_timestamp(created_at)::date = '${dt}'::date       -- 限制时间
    ) t
)
-- 对k币消耗信息进行聚合
-- 要用event = 191 uid和剧id关联
, tmp_aggregated_k_consume as (
    select
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.ad_channel
        , a.user_group
        , a.recommend_id
        , a.recommend_name
        , a.lang_name
        , a.registration_period
        , sum(b.money) as k_consume_amount
    from tmp_click_data a
    left join tmp_k_consume_data b on a.vid = b.goods_id and a.eid = b.goods_sku_id and a.d_date = b.d_date and a.uid = b.uid
    and a.event = 191
    group by a.d_date, a.user_type, a.area,a.country_name, a.ad_channel, a.user_group,a.recommend_id,a.recommend_name, a.lang_name,a.registration_period
)
-- 对曝光信息进行聚合
, tmp_aggregated_exposure as (
    select
        d_date
        , user_type
        , recommend_id
        , recommend_name
        , area
        , country_name
        , ad_channel
        , user_group
        , lang_name
        , registration_period
        , COUNT(DISTINCT uid) AS exposure_uv
        , COUNT(*) AS exposure_pv
    from tmp_exposure_data
    group by d_date, user_type, recommend_id, recommend_name, area, country_name, ad_channel, user_group, lang_name, registration_period
)
-- 对点击信息进行聚合
, tmp_aggregated_click as (
    select
        d_date
        , user_type
        , recommend_id
        , recommend_name
        , area
        , country_name
        , ad_channel
        , user_group
        , lang_name
        , registration_period
        , COUNT(DISTINCT CASE WHEN event = 112 THEN uid END) AS click_uv                -- 点击人数
        , COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_uv     -- 播放人数
        , COUNT(DISTINCT CASE WHEN event = 192 THEN uid END) AS recharge_submission_uv  -- 充值提交人数
        , COUNT(DISTINCT CASE WHEN event = 191 THEN uid END) AS episode_unlock_uv       -- 解锁剧集人数
        , COUNT(CASE WHEN event = 112 THEN uid END) AS click_pv                         -- 点击次数
        , COUNT(CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS episode_play-- 播放集数
        , COUNT(CASE WHEN event = 192 THEN uid END) AS recharge_submission_pv           -- 充值提交次数
        , COUNT(CASE WHEN event = 191 THEN uid END) AS episode_unlocks         -- 解锁剧集集数
        -- , count(distinct CASE WHEN event = 191 and type = '1' THEN concat(uid,'-',eid) END) as ad_episode_unlocks   --  广告解锁剧集集数
        -- , count(distinct CASE WHEN event = 191 and type != '1' THEN concat(uid,'-',eid) END) as non_ad_episode_unlocks -- 非广告解锁剧集集数
        -- , COALESCE(SUM(CASE WHEN event = 263 THEN money ELSE 0 END), 0) as estimated_ad_income  -- 预估广告收入
        , ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) AS watch_duration_minutes  -- 观看时长（分钟）
    from tmp_click_data
    group by d_date, user_type, recommend_id , recommend_name, area, country_name, ad_channel, user_group, lang_name , registration_period
)
-- 对支付信息进行聚合
, tmp_aggregated_payment as (
    SELECT
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.ad_channel
        , a.user_group
        , a.recommend_id
        , a.recommend_name
        , a.lang_name
        , a.registration_period
        , COALESCE(COUNT(DISTINCT b.uid), 0) AS successful_payment_uv
        , COALESCE(COUNT(DISTINCT b.order_num), 0) AS successful_payment_pv
        , COALESCE(SUM(b.total_payment_amount), 0) AS total_payment_amount
    FROM tmp_click_data a
    left join tmp_payment_data b ON a.uid = b.uid AND a.d_date = b.d_date and a.event = 192 and a.order_id=b.order_num
    GROUP BY a.d_date, a.user_type,a.area , a.country_name, a.ad_channel, a.user_group, a.recommend_id,a.recommend_name,a.lang_name,a.registration_period
)
select
    -- 分组维度
    a.d_date
    , a.user_type
    , a.recommend_id
    , a.recommend_name
    , a.area
    , a.country_name
    , a.ad_channel
    , a.user_group
    , a.lang_name
    , a.registration_period
    -- 统计指标
    , COALESCE(a.exposure_uv, 0) AS exposure_uv                             -- 曝光人数
    , COALESCE(b.click_uv, 0) AS click_uv                                -- 点击人数
    , COALESCE(b.play_uv, 0) AS play_uv                                  -- 播放人数
    , COALESCE(b.recharge_submission_uv, 0) AS recharge_submission_uv       -- 充值提交人数
    , COALESCE(b.episode_unlock_uv, 0) AS episode_unlock_uv                 -- 剧集解锁人数
    , COALESCE(a.exposure_pv, 0) AS exposure_pv
    , COALESCE(b.click_pv, 0) AS click_pv                               -- 点击次数
    , COALESCE(b.episode_play, 0) AS episode_play                       -- 播放集数
    , COALESCE(b.recharge_submission_pv, 0) AS recharge_submission_pv   -- 充值提交次数
    , COALESCE(b.episode_unlocks, 0) AS episode_unlocks                 -- 解锁剧集集数
    -- , COALESCE(b.ad_episode_unlocks, 0) AS ad_episode_unlocks           -- 广告解锁剧集集数
    -- , COALESCE(b.non_ad_episode_unlocks, 0) AS non_ad_episode_unlocks   -- 非广告解锁剧集集数
    , cast(COALESCE(b.watch_duration_minutes, 0) as bigint)AS watch_duration_minutes
    , COALESCE(c.successful_payment_uv, 0) AS successful_payment_uv     --  成功支付人数
    , COALESCE(c.successful_payment_pv, 0) AS successful_payment_pv     -- 成功支付次数
    , cast(COALESCE(c.total_payment_amount, 0) as numeric(20,2)) AS total_payment_amount -- 总支付金额
    -- , cast(coalesce(b.estimated_ad_income,0) as numeric(20,2))as estimated_ad_income -- 预估广告收入
    , cast(coalesce(d.k_consume_amount,0) as numeric(20,2))as k_consume_amount  -- K币消耗
from tmp_aggregated_exposure a
left join tmp_aggregated_click b on a.d_date = b.d_date and a.user_type = b.user_type and a.recommend_id = b.recommend_id and a.recommend_name = b.recommend_name and a.area = b.area and a.country_name = b.country_name  and a.ad_channel = b.ad_channel and a.user_group = b.user_group and a.lang_name = b.lang_name and a.registration_period = b.registration_period
left join tmp_aggregated_payment c on a.d_date = c.d_date and a.user_type = c.user_type and a.recommend_id = b.recommend_id and a.recommend_name = c.recommend_name and a.area = c.area and a.country_name = c.country_name  and a.ad_channel = c.ad_channel and a.user_group = c.user_group and a.lang_name = c.lang_name and a.registration_period = b.registration_period
left join tmp_aggregated_k_consume d on a.d_date = d.d_date and a.user_type = d.user_type and a.recommend_id = b.recommend_id and a.recommend_name = d.recommend_name and a.area = d.area and a.country_name = d.country_name  and a.ad_channel = d.ad_channel and a.user_group = d.user_group and a.lang_name = d.lang_name and a.registration_period = b.registration_period
where a.user_group is not null;

delete from public.dw_recommend_home_olduser_scale_up where d_date = '${dt}'::date;
insert into public.dw_recommend_home_olduser_scale_up select * from tmp.tmp_dw_recommend_home_olduser_scale_up;





-- 端内
drop table if exists tmp.tmp_dw_recommend_home_app_olduser_scale_up;
create table tmp.tmp_dw_recommend_home_app_olduser_scale_up as
with user_type_derivation as (
    SELECT DISTINCT
        a.uid
        , CASE
            WHEN a.uid % 100 >= 10 and a.uid %100 <= 59 THEN '对照组'         -- 扩量后
            WHEN (a.uid % 100 >= 0 and a.uid %100 <= 9) or (a.uid %100 >= 60 and a.uid %100 <= 99) THEN '实验组'   -- 扩量后
            ELSE NULL END AS user_group
        , to_timestamp(a.created_at)::date as 日期  -- 日期
        , '老用户' as user_type
        , b.area
        , b.country_name
        , b.ad_channel
        , b.lang_name
        , case when EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600 > 24 and EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600<= 48 then '注册1天'
             when EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600 > 48 and EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600<= 96 then '注册2-3天'
             when EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600 > 96 and EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600<= 192 then '注册4-7天'
             when EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600 > 192 then '注册7天以上'
             else '未知' END AS registration_period
    FROM "app_user_cover_show_log"  a
    left join public.dwd_user_info b on a.uid::text = b.uid                     -- 关联用户表  限制老用户
    left join "oversea-api_osd_recommend" c on a.model_id::int = c.id           -- 关联推荐位  限制推荐位
    WHERE 1=1
        and to_timestamp(a.created_at)::date  BETWEEN '${dt}'::date-7 and  '${dt}'::date --从5月26日开始
        AND event = 111                                         -- 曝光event
        AND CAST(ext_body::json ->> 'page' AS int) = 1          -- 首页
        and c.user_newuser_alg_status = 1                       -- 应用新用户推荐策略的推荐位
        -- and c.chinese_name not like '%泰语%'                  -- 扩量不扩泰语
    group by a.uid , to_timestamp(a.created_at)::date,b.d_time,b.area, b.country_name, b.ad_channel, b.lang_name -- 按照uid 日期 推荐位 分组
    having min(to_timestamp(a.created_at)) > b.d_time::timestamp + INTERVAL '24 hours' -- 限制老用户
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
    and to_timestamp(created_at)::date BETWEEN '${dt}'::date-7 and  '${dt}'::date
    -- 全量更新
    -- and to_timestamp(created_at)::date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
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
    and to_timestamp(created_at)::date BETWEEN '${dt}'::date-7 and  '${dt}'::date
    -- 全量更新
    -- and  to_timestamp(created_at)::date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
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
    and to_timestamp(created_at)::date BETWEEN '${dt}'::date-7 and  '${dt}'::date
    -- 全量更新
    -- and to_timestamp(created_at)::date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
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
    and to_timestamp(created_at)::date BETWEEN '${dt}'::date-7 and  '${dt}'::date
    -- 全量更新
    -- and  to_timestamp(created_at)::date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
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
    and to_timestamp(created_at)::date BETWEEN '${dt}'::date-7 and  '${dt}'::date
    -- 全量更新
    -- and  to_timestamp(created_at)::date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
    ) t
),
pv_counts AS (
    SELECT
        uid,
        to_timestamp(created_at)::date as 日期,
        COUNT(1) AS pv
    FROM "app_user_track_log"
    WHERE
        -- 增量更新
        to_timestamp(created_at)::date BETWEEN '${dt}'::date-7 and  '${dt}'::date
        -- 全量更新
        -- to_timestamp(created_at)::date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
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
        to_timestamp(created_at)::date BETWEEN '${dt}'::date-7 and  '${dt}'::date
        -- 全量更新
        -- to_timestamp(created_at)::date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
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
        to_timestamp(created_at)::date BETWEEN '${dt}'::date-7 and  '${dt}'::date
        -- 全量更新
        -- to_timestamp(created_at)::date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
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
      on ue.vid = k.goods_id  and ue.eid = k.goods_sku_id and ue.uid = k.uid and ue.日期 = k.date
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
        to_timestamp(created_at)::date BETWEEN '${dt}'::date-7 and  '${dt}'::date
        -- 全量更新
        -- to_timestamp(created_at)::date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
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
    from public.all_order_log a
    inner join recharge_counts rc on to_timestamp(a.created_at)::date = rc.日期 and a.uid = rc.uid
    WHERE
        -- 增量更新
        to_timestamp(created_at)::date BETWEEN '${dt}'::date-7 and  '${dt}'::date
        -- 全量更新
        -- to_timestamp(created_at)::date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
        AND status = 1
        AND environment = 1
    GROUP BY a.uid, to_timestamp(created_at)::date
),
active_users AS (
    SELECT
        uid,
        d_date AS active_date
    FROM public.dwd_user_active
    WHERE
        -- 增量更新
        d_date BETWEEN '${dt}'::date-7 and  '${dt}'::date
        -- 全量更新
        -- d_date BETWEEN '2025-05-12' AND (current_date+interval '-1 day')
    GROUP BY uid,d_date
),
active_day_2 AS (
    SELECT DISTINCT
        d_date AS 活跃日期,
        uid
    FROM public.dwd_user_active
    WHERE
        d_date >=  '2025-05-27'
    GROUP BY uid,d_date
),
active_day_4 AS (
    SELECT DISTINCT
        d_date AS 活跃日期,
        uid
    FROM public.dwd_user_active
    WHERE
        d_date >=  '2025-05-29'
    GROUP BY uid,d_date
),
active_day_8 AS (
    SELECT DISTINCT
        d_date AS 活跃日期,
        uid
    FROM public.dwd_user_active
    WHERE
        d_date >=  '2025-06-01'
    GROUP BY uid,d_date
)

SELECT
    -- 维度
    us.日期 as date,
    cast(us.user_type as text ) as user_type,
    us.user_group as group_type,
    us.area,
    us.country_name,
    us.ad_channel,
    us.lang_name,
    us.registration_period,
    -- 计算字段
    COUNT(DISTINCT us.uid) AS exposure_users,
    COUNT(DISTINCT CASE WHEN pv_counts.pv IS NOT NULL THEN us.uid END) AS play_users,
    COUNT(DISTINCT CASE WHEN recharge_counts.recharge_times IS NOT NULL THEN us.uid END) AS submit_recharge_users,
    COALESCE(SUM(recharge_counts.recharge_times), 0) AS submit_recharge_times,
    COUNT(DISTINCT CASE WHEN unlock_episodes_counts.unlock_episodes_times IS NOT NULL THEN us.uid END) AS unlock_episodes_users,
    COALESCE(SUM(unlock_episodes_counts.unlock_episodes_times), 0) AS unlock_episodes_times,
    cast(COALESCE(SUM(watch_time_counts.watch_time_minutes), 0) as numeric(20,2))AS total_watch_time_minutes,
    COUNT(DISTINCT CASE WHEN sr.uid IS NOT NULL THEN us.uid END) AS successful_recharge_users,
    sum(sr.successful_recharge_times)  AS successful_recharge_times,
    cast(COALESCE(SUM(sr.total_recharge_amount),  0)as numeric(20,2)) as total_recharge_amount ,
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
delete from public.dw_recommend_home_app_olduser_scale_up where  date BETWEEN '${dt}'::date-7 and  '${dt}'::date;
insert into public.dw_recommend_home_app_olduser_scale_up select * from tmp.tmp_dw_recommend_home_app_olduser_scale_up;



-- 应用新用户策略算法 一级指标
-- 全量更新 从6月9日开始
drop table if exists tmp.tmp_dw_recommend_home_index_olduser_scale_up;
create table tmp.tmp_dw_recommend_home_index_olduser_scale_up as
WITH tmp_user_log AS (
    SELECT DISTINCT
        a.uid
        , CASE
            WHEN a.uid % 100 >= 10 and a.uid %100 <= 59 THEN '对照组'         -- 扩量后
            WHEN (a.uid % 100 >= 0 and a.uid %100 <= 9) or (a.uid %100 >= 60 and a.uid %100 <= 99) THEN '实验组'   -- 扩量后
            ELSE NULL END AS user_group
        , to_timestamp(a.created_at)::date as d_date  -- 日期
        , c.id as recommend_id                        -- 推荐位id
    FROM "app_user_cover_show_log"  a
    left join public.dwd_user_info b on a.uid::text = b.uid                     -- 关联用户表  限制老用户
    left join "oversea-api_osd_recommend" c on a.model_id::int = c.id           -- 关联推荐位  限制推荐位
    WHERE 1=1
        and to_timestamp(a.created_at)::date between '2025-06-09' and  (CURRENT_DATE - INTERVAL '1 day') --从6月9日开始
        AND event = 111                                         -- 曝光event
        AND CAST(ext_body::json ->> 'page' AS int) = 1          -- 首页
        and c.user_newuser_alg_status = 1                       -- 应用新用户推荐策略的推荐位
        -- and c.chinese_name not like '%泰语%'                  -- 扩量不扩泰语
    group by a.uid , to_timestamp(a.created_at)::date , c.id,b.d_time    -- 按照uid 日期 推荐位 分组
    having min(to_timestamp(a.created_at)) > b.d_time::timestamp + INTERVAL '24 hours' -- 限制老用户
)
-- 求出用户最早的曝光日期
-- 维度：uid
, tmp_user_first_exposure as (
    select
        uid
        , user_group
        , min(d_date) as first_exposure_date    -- 用户最早的曝光日期
    from tmp_user_log
    group by uid
           , user_group
)
-- 最早曝光日期之后的充值行为
-- 维度 uid 日期
, tmp_successful_recharges as (
    select
        a.uid
        , to_timestamp(created_at)::date as d_date
        , sum(a.money) as total_recharge_amount -- 充值金额
    from public.all_order_log a
    join tmp_user_first_exposure b on a.uid = b.uid
    where to_timestamp(created_at)::date >= b.first_exposure_date
        and to_timestamp(created_at)::date between '2025-06-09' and  (CURRENT_DATE - INTERVAL '1 day')
        and status = 1              -- 充值成功
        and environment = 1         -- 生产环境
    group by a.uid, d_date
)
-- 用户的活跃时间
-- 维度：uid
, tmp_user_active_days as (
    select
        a.uid
        , count(distinct to_timestamp(created_at)::date) as user_active_days
    from "app_user_track_log" a
    join tmp_user_first_exposure b on a.uid = b.uid
    where event in (1,16)
        and to_timestamp(created_at)::date >= b.first_exposure_date
        and to_timestamp(created_at)::date between '2025-06-09' and  (CURRENT_DATE - INTERVAL '1 day')
    group by a.uid
)
-- 合并数据
-- 维度：uid
, tmp_combined_data as (
    select
        a.uid
        , a.user_group
        , coalesce(sum(total_recharge_amount),0) as total_recharge_amount
        , user_active_days
    from tmp_user_first_exposure a
    left join tmp_successful_recharges b on a.uid = b.uid
    left join tmp_user_active_days c on a.uid = c.uid
    group by a.uid , a.user_group, user_active_days
)
select
    a.user_group as "实验分组"
    , COUNT(DISTINCT a.uid) AS 观察日期内曝光人数
    , cast(SUM(a.total_recharge_amount)/100.0 as numeric(20,2)) AS 观察日期内充值金额
    , SUM(a.user_active_days) AS 观察日期内活跃天数
from tmp_combined_data a
where a.user_group is not null
group by a.user_group
order by a.user_group;

-- 全量更新
truncate table public.dw_recommend_home_index_olduser_scale_up;
insert into public.dw_recommend_home_index_olduser_scale_up select * from tmp.tmp_dw_recommend_home_index_olduser_scale_up;






























-- 应用新用户策略算法 一级指标
-- 全量更新 从6月9日开始
drop table if exists tmp.tmp_dw_recommend_home_index_olduser_scale_up;
create table tmp.tmp_dw_recommend_home_index_olduser_scale_up as
WITH tmp_user_log AS (
    SELECT DISTINCT
        a.uid
        , CASE
            WHEN a.uid % 100 >= 10 and a.uid %100 <= 59 THEN '对照组'         -- 扩量后
            WHEN (a.uid % 100 >= 0 and a.uid %100 <= 9) or (a.uid %100 >= 60 and a.uid %100 <= 99) THEN '实验组'   -- 扩量后
            ELSE NULL END AS user_group
        , to_timestamp(a.created_at)::date as d_date  -- 日期
        , c.id as recommend_id                        -- 推荐位id
    FROM "app_user_cover_show_log"  a
    left join public.dwd_user_info b on a.uid::text = b.uid                     -- 关联用户表  限制老用户
    left join "oversea-api_osd_recommend" c on a.model_id::int = c.id           -- 关联推荐位  限制推荐位
    WHERE 1=1
        and to_timestamp(a.created_at)::date between '2025-06-09' and  (CURRENT_DATE - INTERVAL '1 day') --从6月9日开始
        AND event = 111                                         -- 曝光event
        AND CAST(ext_body::json ->> 'page' AS int) = 1          -- 首页
        and c.user_newuser_alg_status = 1                       -- 应用新用户推荐策略的推荐位
        -- and c.chinese_name not like '%泰语%'                  -- 扩量不扩泰语
    group by a.uid , to_timestamp(a.created_at)::date , c.id,b.d_time    -- 按照uid 日期 推荐位 分组
    having min(to_timestamp(a.created_at)) > b.d_time::timestamp + INTERVAL '24 hours' -- 限制老用户
)
-- 求出用户最早的曝光日期
-- 维度：uid
, tmp_user_first_exposure as (
    select
        uid
        , user_group
        , min(d_date) as first_exposure_date    -- 用户最早的曝光日期
    from tmp_user_log
    group by uid
           , user_group
)
-- 最早曝光日期之后的充值行为
-- 维度 uid 日期
, tmp_successful_recharges as (
    select
        a.uid
        , to_timestamp(created_at)::date as d_date
        , sum(a.money) as total_recharge_amount -- 充值金额
    from public.all_order_log a
    join tmp_user_first_exposure b on a.uid = b.uid
    where to_timestamp(created_at)::date >= b.first_exposure_date
        and to_timestamp(created_at)::date between '2025-06-09' and  (CURRENT_DATE - INTERVAL '1 day')
        and status = 1              -- 充值成功
        and environment = 1         -- 生产环境
    group by a.uid, d_date
)
-- 用户的活跃时间
-- 维度：uid
, tmp_user_active_days as (
    select
        a.uid
        , count(distinct to_timestamp(created_at)::date) as user_active_days
    from "app_user_track_log" a
    join tmp_user_first_exposure b on a.uid = b.uid
    where event in (1,16)
        and to_timestamp(created_at)::date >= b.first_exposure_date
        and to_timestamp(created_at)::date between '2025-06-09' and  (CURRENT_DATE - INTERVAL '1 day')
    group by a.uid
)
-- 合并数据
-- 维度：uid
, tmp_combined_data as (
    select
        a.uid
        , a.user_group
        , coalesce(sum(total_recharge_amount),0) as total_recharge_amount
        , user_active_days
    from tmp_user_first_exposure a
    left join tmp_successful_recharges b on a.uid = b.uid
    left join tmp_user_active_days c on a.uid = c.uid
    group by a.uid , a.user_group, user_active_days
)
select
    a.user_group as "实验分组"
    , COUNT(DISTINCT a.uid) AS 观察日期内曝光人数
    , cast(SUM(a.total_recharge_amount)/100.0 as numeric(20,2)) AS 观察日期内充值金额
    , SUM(a.user_active_days) AS 观察日期内活跃天数
from tmp_combined_data a
where a.user_group is not null
group by a.user_group
order by a.user_group;

-- 全量更新
truncate table public.dw_recommend_home_index_olduser_scale_up;
insert into public.dw_recommend_home_index_olduser_scale_up select * from tmp.tmp_dw_recommend_home_index_olduser_scale_up;