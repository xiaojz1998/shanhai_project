---------------------------------------------
-- File: 推荐位报表_播放页完播弹窗_增量_d.sql
-- Time: 2025/5/27 17:10
-- User: xiaoj
-- Description:  
---------------------------------------------
SET timezone = 'UTC-0';
------------------------------------------
--  建表
------------------------------------------

-- 首页播放页完播弹窗
-- drop table if exists public.dw_recommend_playfinish;
create table if not exists public.dw_recommend_playfinish (
    -- 分组维度
    d_date date             -- 日期
    , user_type text        -- 用户类型
    , recommend_name text       -- 推荐位名
    , area text             -- 区域
    , country_name text     -- 国家
    , ad_channel text       -- 渠道
    , user_group text       -- 用户分组
    , lang_name text        -- 语言
    , registration_period text  -- 注册周期
    -- 计算字段
    , exposure_uv bigint -- 曝光人数
    , click_uv bigint -- 点击人数
    , play_uv bigint -- 播放人数
    , recharge_submission_uv bigint -- 提交充值人数
    , episode_unlock_uv bigint   -- 剧集解锁人数
    , exposure_pv bigint -- 曝光次数
    , click_pv bigint -- 点击次数
    , play_pv bigint -- 播放次数
    , recharge_submission_pv bigint -- 提交充值次数
    , episode_unlocks bigint -- 解锁集数
    , ad_episode_unlocks bigint      -- 广告解锁集数
    , non_ad_episode_unlocks bigint  -- 非广告解锁集数
    , watch_duration_minutes bigint  -- 观看时长
    , successful_payment_uv bigint -- 支付成功人数
    , successful_payment_pv bigint -- 支付成功次数
    , total_payment_amount numeric(20,2) -- 总支付金额
    , estimated_ad_income numeric(20,2) -- 预估广告收入
    , k_consume_amount numeric(20,2)    -- 消耗k币数
);

-- 老用户端内表现播放页退出挽留窗口
-- drop table if exists dw_recommend_playretain_app_olduser;
-- create table if not exists dw_recommend_playretain_app_olduser (
--     -- 维度
--     date date NOT NULL,
--     user_type text,
--     group_type text,
--     area text,
--     country_name text,
--     ad_channel text,
--     lang_name text,
--     registration_period text,
--     -- 计算字段
--     exposure_users bigint,
--     play_users bigint,
--     submit_recharge_users bigint,
--     submit_recharge_times bigint,
--     unlock_episodes_users bigint,
--     unlock_episodes_times bigint,
--     total_watch_time_minutes numeric(20,2),
--     successful_recharge_users bigint,
--     successful_recharge_times bigint,
--     total_recharge_amount numeric(20,2),
--     k_consume_amount numeric(20,2),
--     daily_active_users bigint,
--     retention_next_day bigint,
--     retention_3_days bigint,
--     retention_7_days bigint
-- );


-- 播放页完播弹窗一级指标
-- drop table if exists public.dw_recommend_playfinish_index;
create table if not exists public.dw_recommend_playfinish_index(
    "实验分组" text
    , "观察日期内曝光人数" bigint
    , "观察日期内充值金额" numeric(20,2)
    , "观察日期内活跃天数" bigint
);

------------------------------------------
-- 更新
------------------------------------------
-- 播放页完播弹窗- 首页
-- drop table if exists tmp.tmp_dw_recommend_playfinish;
-- create table tmp.tmp_dw_recommend_playfinish as

-- 获取注册时间戳
-- 维度：uid
with tmp_user_info as (
    select
        uid::bigint
        , d_time::timestamp as register_timestamp
        , country_name
        , area
        , lang_name
        , case when uid::bigint % 2 = 0 then '对照组'
               when uid::bigint % 2 = 1 then '实验组'
           end as user_group            -- 对uid进行分组
        , ad_channel
    from dwd_user_info
)
-- 获取用户曝光行为
-- 维度：uid 日期
, tmp_user_exposure as (
    select
        a.uid
        , to_timestamp(a.created_at)::date as d_date -- 日志日期
        , CAST(a.ext_body::json ->> 'show_title' AS text) as recommend_name
        , min(to_timestamp(a.created_at)) as min_exposure_time  -- 每天最小曝光时间戳
        , b.register_timestamp
        , b.country_name
        , b.area
        , b.lang_name
        , b.ad_channel
        , b.user_group
    from public.app_user_cover_show_log a
    left join tmp_user_info b on a.uid = b.uid                          -- 关联用户信息
    where event = 111                                                   -- 曝光埋点
        and to_timestamp(a.created_at)::date = '2025-05-20'::date       -- 限制时间
        and CAST(ext_body::json ->> 'page' AS int) = 3                  -- 页数
        and ext_body::json ->> 'show_title' = 'playFinish'              -- 播放页完播弹窗
    group by a.uid
           , to_timestamp(a.created_at)::date
           , CAST(a.ext_body::json ->> 'show_title' AS text)
           , b.register_timestamp
           , b.country_name
           , b.area
           , b.lang_name
           , b.ad_channel
           , b.user_group
)
-- 获取用户曝光行为、用户信息
-- 维度：uid 日期
, tmp_user_exposure_info as (
    select
        uid
        , d_date
        , recommend_name
        , CASE WHEN register_timestamp IS NULL THEN '未知'
            WHEN min_exposure_time <= (register_timestamp + INTERVAL '24 hours') THEN 'newuser'
            ELSE 'olduser' END AS user_type
        , area
        , country_name
        , ad_channel
        , user_group
        , lang_name
        , case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
             else '未知' END AS registration_period
    from tmp_user_exposure
)












-- 播放页完播弹窗一级指标
-- 全量更新 日期从 5.22 号开始
drop table if exists tmp.tmp_dw_recommend_playfinish_index;
create table tmp.tmp_dw_recommend_playfinish_index as
-- 用户点击playfinish 推荐位日志行为，并对用户分组
with tmp_user_log as (
    select
        uid
        , case when uid % 2 = 0 then '对照组'
               when uid % 2 = 1 then '实验组'
           end as user_group            -- 对uid进行分组
        , to_timestamp(created_at)::date as d_date  -- 日期
    from "app_user_cover_show_log"
    where 1=1
        and to_timestamp(created_at)::date between '2025-05-22' and  (CURRENT_DATE - INTERVAL '1 day')
        and event = 111
        and CAST(ext_body::json ->> 'page' AS int) = 3
        and ext_body::json ->> 'show_title' = 'playFinish'
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
        and to_timestamp(created_at)::date between '2025-05-22' and  (CURRENT_DATE - INTERVAL '1 day')
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
        and to_timestamp(created_at)::date between '2025-05-22' and  (CURRENT_DATE - INTERVAL '1 day')
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
truncate table public.dw_recommend_playfinish_index;
insert into public.dw_recommend_playfinish_index select * from tmp.tmp_dw_recommend_playfinish_index;