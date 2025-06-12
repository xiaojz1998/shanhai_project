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
    -- , recommend_id text       -- 推荐位名
    , recommend_name text       -- 推荐位名
    , area text             -- 区域
    , country_name text     -- 国家
    , country_grade text    -- 国家等级
    , ad_channel text       -- 渠道
    , user_group text       -- 用户分组
    , lang_name text        -- 语言
    --, registration_period text  -- 注册周期
    -- 计算字段
    , exposure_uv bigint -- 曝光人数
    , click_uv bigint -- 点击人数
    , play_uv bigint -- 播放人数
    , recharge_submission_uv bigint -- 提交充值人数
    , episode_unlock_uv bigint   -- 剧集解锁人数
    , exposure_pv bigint -- 曝光次数
    , click_pv bigint -- 点击次数
    , episode_play bigint -- 播放次数
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

-- 端内播放页完播弹窗
-- drop table if exists public.dw_recommend_playfinish_app;
create table if not exists public.dw_recommend_playfinish_app (
    -- 维度
    d_date date NOT NULL
    , user_type text
    , user_group text
    , area text
    , country_name text
    , country_grade text  -- 国家等级
    , ad_channel text
    , lang_name text
    -- , registration_period text
    -- 计算字段
    , exposure_uv bigint -- 曝光人数
    , play_uv bigint -- 播放人数
    , recharge_submission_uv bigint -- 提交充值人数
    , episode_unlock_uv bigint   -- 剧集解锁人数
    , episode_play bigint -- 播放集数
    , recharge_submission_pv bigint -- 提交充值次数
    , episode_unlocks bigint -- 解锁集数
    , ad_episode_unlocks bigint      -- 广告解锁集数
    , non_ad_episode_unlocks bigint  -- 非广告解锁集数
    , watch_duration_minutes bigint  -- 观看时长
    , successful_payment_uv bigint -- 支付成功人数
    , successful_payment_pv bigint -- 支付成功次数
    , total_payment_amount numeric(20,2) -- 总支付金额
    , k_consume_amount numeric(20,2)    -- 消耗k币数
    , retention_1_days bigint        -- 1日留存
    , retention_3_days bigint        -- 3日留存
    , retention_7_days bigint        -- 7日留存
);


-- 播放页完播弹窗一级指标
--drop table if exists public.dw_recommend_playfinish_index;
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
drop table if exists tmp.tmp_dw_recommend_playfinish;
create table tmp.tmp_dw_recommend_playfinish as

-- 获取注册时间戳
-- 维度：uid
with tmp_user_info as (
    select
        uid::bigint
        , d_time::timestamp as register_timestamp
        , t.country_name
        , t0.country_grade  as country_grade
        , t.area
        , lang_name
        , case when uid::bigint % 2 = 0 then '对照组'
               when uid::bigint % 2 = 1 then '实验组'
           end as user_group            -- 对uid进行分组
        , ad_channel
    from dwd_user_info t
    left join v_dim_country_area t0 on t.country_code = t0.country_code
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
        , b.country_grade
        , b.area
        , b.lang_name
        , b.ad_channel
        , b.user_group
    from public.app_user_cover_show_log a
    left join tmp_user_info b on a.uid = b.uid                          -- 关联用户信息
    where event = 111                                                   -- 曝光埋点
        and to_timestamp(a.created_at)::date = '2025-05-10'::date       -- 限制时间
        and CAST(ext_body::json ->> 'page' AS int) = 3                  -- 页数
        and ext_body::json ->> 'show_title' = 'playFinish'              -- 播放页完播弹窗
    group by a.uid
           , to_timestamp(a.created_at)::date
           , CAST(a.ext_body::json ->> 'show_title' AS text)
           , b.register_timestamp
           , b.country_name
           , b.country_grade
           , b.area
           , b.lang_name
           , b.ad_channel
           , b.user_group
)
-- 获取用户曝光行为、用户信息
-- 维度：uid 日期
-- 要处理null的维度
, tmp_user_exposure_info as (
    select
        uid
        , d_date
        , recommend_name
        , CASE WHEN register_timestamp IS NULL THEN '未知'
            WHEN d_date::date = register_timestamp::date THEN '新用户'
            when d_date::date > register_timestamp::date then '老用户'
            ELSE '未知' END AS user_type
        , coalesce(area,  '未知') as area
        , coalesce(country_name, '未知') as country_name
        , coalesce(country_grade, '未知') as country_grade
        , coalesce(ad_channel, '未知') as ad_channel
        , coalesce(user_group, '未知') as user_group
        , coalesce(lang_name, '未知') as lang_name
        --, case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
        --     when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
        --     when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
        --     when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
        --     else '未知' END AS registration_period
    from tmp_user_exposure
)
, tmp_exposure_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , CAST(a.ext_body::json ->> 'show_title' AS text) as recommend_name
        , b.user_type
        , b.area
        , b.country_name
        , b.country_grade
        , b.ad_channel
        , b.user_group
        , b.lang_name
    from public.app_user_cover_show_log a
    inner join tmp_user_exposure_info b on a.uid = b.uid and to_timestamp(a.created_at)::date = b.d_date   -- 限制当天有曝光的
    where event = 111                                                   -- 曝光埋点
        and to_timestamp(a.created_at)::date = '2025-05-10'::date
        and CAST(ext_body::json ->> 'page' AS int) = 3                  -- 页数
        and ext_body::json ->> 'show_title' = 'playFinish'              -- 播放页完播弹窗
)
, tmp_click_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , event
        , case when order_id like '%SH%' then order_id else CONCAT('SH', order_id) end as order_id  -- 用于匹配支付
        , watch_time
        , money::numeric(20,2) as money                 -- 用于计算预估广告收入
        , vid                   -- k币
        , eid                   -- k币
        , CAST(a.ext_body::json ->> 'show_title' AS text) as recommend_name
        , CAST(a.ext_body::json ->> 'type' AS text) as type         -- 用于看广告解锁
        , b.user_type
        , b.area
        , b.country_name
        , b.country_grade
        , b.ad_channel
        , b.user_group
        , b.lang_name
    from public.app_user_track_log a
    inner join tmp_user_exposure_info b on a.uid = b.uid and to_timestamp(a.created_at)::date = b.d_date
    where event in (112, 1, 192, 191, 2, 13, 14,263)                    -- 取的事件
        and to_timestamp(a.created_at)::date = '2025-05-10'::date       -- 限制时间
        and CAST(ext_body::json ->> 'page' AS int) = 3                  -- 页数
        and ext_body::json ->> 'show_title' = 'playFinish'              -- 播放页完播弹窗
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
        and to_timestamp(a.created_at)::date = '2025-05-10'::date       -- 限制时间
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
        and to_timestamp(created_at)::date = '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_01"
    where type = 0
        and to_timestamp(created_at)::date = '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_02"
    where type = 0
        and to_timestamp(created_at)::date = '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_03"
    where type = 0
        and to_timestamp(created_at)::date = '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_04"
    where type = 0
        and to_timestamp(created_at)::date = '2025-05-10'::date       -- 限制时间
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
        , a.country_grade
        , a.ad_channel
        , a.user_group
        , a.recommend_name
        , a.lang_name
        , sum(b.money) as k_consume_amount
    from tmp_click_data a
    left join tmp_k_consume_data b on a.vid = b.goods_id and a.eid = b.goods_sku_id and a.d_date = b.d_date and a.uid = b.uid
    and a.event = 191
    group by a.d_date, a.user_type, a.area,a.country_name, a.country_grade, a.ad_channel, a.user_group,a.recommend_name, a.lang_name
)
-- 对曝光信息进行聚合
, tmp_aggregated_exposure as (
    select
        d_date
        , user_type
        , recommend_name
        , area
        , country_name
        , country_grade
        , ad_channel
        , user_group
        , lang_name
        , COUNT(DISTINCT uid) AS exposure_uv
        , COUNT(*) AS exposure_pv
    from tmp_exposure_data
    group by d_date, user_type, recommend_name, area, country_name,country_grade , ad_channel, user_group, lang_name
)
-- 对点击信息进行聚合
, tmp_aggregated_click as (
    select
        d_date
        , user_type
        , recommend_name
        , area
        , country_name
        , country_grade
        , ad_channel
        , user_group
        , lang_name
        , COUNT(DISTINCT CASE WHEN event = 112 THEN uid END) AS click_uv                -- 点击人数
        , COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_uv     -- 播放人数
        , COUNT(DISTINCT CASE WHEN event = 192 THEN uid END) AS recharge_submission_uv  -- 充值提交人数
        , COUNT(DISTINCT CASE WHEN event = 191 THEN uid END) AS episode_unlock_uv       -- 解锁剧集人数
        , COUNT(CASE WHEN event = 112 THEN uid END) AS click_pv                         -- 点击次数
        , COUNT(distinct CASE WHEN event IN (1, 2, 13, 14) THEN eid END) AS episode_play              -- 播放次数
        , COUNT(CASE WHEN event = 192 THEN uid END) AS recharge_submission_pv           -- 充值提交次数
        , COUNT(distinct CASE WHEN event = 191 THEN eid END) AS episode_unlocks         -- 解锁剧集集数
        , count(distinct CASE WHEN event = 191 and type = '1' THEN eid END) as ad_episode_unlocks   --  广告解锁剧集集数
        , count(distinct CASE WHEN event = 191 and type = '2' THEN eid END) as non_ad_episode_unlocks -- 非广告解锁剧集集数
        , COALESCE(SUM(CASE WHEN event = 263 THEN money ELSE 0 END), 0) as estimated_ad_income  -- 预估广告收入
        , ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) AS watch_duration_minutes  -- 观看时长（分钟）
    from tmp_click_data
    group by d_date, user_type, recommend_name, area, country_name, country_grade, ad_channel, user_group, lang_name
)
-- 对支付信息进行聚合
, tmp_aggregated_payment as (
    SELECT
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.country_grade
        , a.ad_channel
        , a.user_group
        , a.recommend_name
        , a.lang_name
        , COALESCE(COUNT(DISTINCT b.uid), 0) AS successful_payment_uv
        , COALESCE(COUNT(DISTINCT b.order_num), 0) AS successful_payment_pv
        , COALESCE(SUM(b.total_payment_amount), 0) AS total_payment_amount
    FROM tmp_click_data a
    left join tmp_payment_data b ON a.uid = b.uid AND a.d_date = b.d_date and a.event = 192 and a.order_id=b.order_num
    GROUP BY a.d_date, a.user_type,a.area , a.country_name, a.country_grade, a.ad_channel, a.user_group, a.recommend_name,a.lang_name
)
select
    -- 分组维度
    a.d_date
    , a.user_type
    , a.recommend_name
    , a.area
    , a.country_name
    , cast(a.country_grade as text) as country_grade
    , a.ad_channel
    , a.user_group
    , a.lang_name
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
    , COALESCE(b.ad_episode_unlocks, 0) AS ad_episode_unlocks           -- 广告解锁剧集集数
    , COALESCE(b.non_ad_episode_unlocks, 0) AS non_ad_episode_unlocks   -- 非广告解锁剧集集数
    , cast(COALESCE(b.watch_duration_minutes, 0) as bigint)AS watch_duration_minutes
    , COALESCE(c.successful_payment_uv, 0) AS successful_payment_uv     --  成功支付人数
    , COALESCE(c.successful_payment_pv, 0) AS successful_payment_pv     -- 成功支付次数
    , cast(COALESCE(c.total_payment_amount, 0) as numeric(20,2)) AS total_payment_amount -- 总支付金额
    , cast(coalesce(b.estimated_ad_income,0) as numeric(20,2))as estimated_ad_income -- 预估广告收入
    , cast(coalesce(d.k_consume_amount,0) as numeric(20,2))as k_consume_amount  -- K币消耗
from tmp_aggregated_exposure a
left join tmp_aggregated_click b on a.d_date = b.d_date and a.user_type = b.user_type and a.recommend_name = b.recommend_name and a.area = b.area and a.country_name = b.country_name and a.country_grade = b.country_grade and a.ad_channel = b.ad_channel and a.user_group = b.user_group and a.lang_name = b.lang_name
left join tmp_aggregated_payment c on a.d_date = c.d_date and a.user_type = c.user_type and a.recommend_name = c.recommend_name and a.area = c.area and a.country_name = c.country_name and a.country_grade = c.country_grade and a.ad_channel = c.ad_channel and a.user_group = c.user_group and a.lang_name = c.lang_name
left join tmp_aggregated_k_consume d on a.d_date = d.d_date and a.user_type = d.user_type and a.recommend_name = d.recommend_name and a.area = d.area and a.country_name = d.country_name and a.country_grade = d.country_grade and a.ad_channel = d.ad_channel and a.user_group = d.user_group and a.lang_name = d.lang_name
where a.user_group is not null;

delete from public.dw_recommend_playfinish where d_date = '2025-05-10'::date;
insert into public.dw_recommend_playfinish select * from tmp.tmp_dw_recommend_playfinish;

-- 播放页完播弹窗- 端内
drop table if exists tmp.tmp_dw_recommend_playfinish_app;
create table tmp.tmp_dw_recommend_playfinish_app as
-- 获取注册时间戳
-- 维度：uid
with tmp_user_info as (
    select
        uid::bigint
        , d_time::timestamp as register_timestamp
        , t.country_name
        , t0.country_grade  as country_grade
        , t.area
        , lang_name
        , case when uid::bigint % 2 = 0 then '对照组'
               when uid::bigint % 2 = 1 then '实验组'
           end as user_group            -- 对uid进行分组
        , ad_channel
    from dwd_user_info t
    left join v_dim_country_area t0 on t.country_code = t0.country_code
)
-- 获取用户曝光行为
-- 维度：uid 日期
, tmp_user_exposure as (
    select
        a.uid
        , to_timestamp(a.created_at)::date as d_date -- 日志日期
        -- , CAST(a.ext_body::json ->> 'show_title' AS text) as recommend_name
        , min(to_timestamp(a.created_at)) as min_exposure_time  -- 每天最小曝光时间戳
        , b.register_timestamp
        , b.country_name
        , b.country_grade
        , b.area
        , b.lang_name
        , b.ad_channel
        , b.user_group
    from public.app_user_cover_show_log a
    left join tmp_user_info b on a.uid = b.uid                          -- 关联用户信息
    where event = 111                                                   -- 曝光埋点
        and to_timestamp(a.created_at)::date between '2025-05-10'::date-7 and  '2025-05-10'::date       -- 限制时间
        and CAST(ext_body::json ->> 'page' AS int) = 3                  -- 页数
        and ext_body::json ->> 'show_title' = 'playFinish'              -- 播放页完播弹窗
    group by a.uid
           , to_timestamp(a.created_at)::date
           -- , CAST(a.ext_body::json ->> 'show_title' AS text)
           , b.register_timestamp
           , b.country_name
           , b.country_grade
           , b.area
           , b.lang_name
           , b.ad_channel
           , b.user_group
)
-- 获取用户曝光行为、用户信息
-- 维度：uid 日期
-- 要处理null的维度
, tmp_user_exposure_info as (
    select
        uid
        , d_date
        -- , recommend_name
        , CASE WHEN register_timestamp IS NULL THEN '未知'
            WHEN d_date::date = register_timestamp::date THEN '新用户'
            when d_date::date > register_timestamp::date then '老用户'
            ELSE '未知' END AS user_type
        , coalesce(area,  '未知') as area
        , coalesce(country_name, '未知') as country_name
        , coalesce(country_grade, '未知') as country_grade
        , coalesce(ad_channel, '未知') as ad_channel
        , coalesce(user_group, '未知') as user_group
        , coalesce(lang_name, '未知') as lang_name
        -- , case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
        --      when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
        --      when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
        --      when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
        --      else '未知' END AS registration_period
    from tmp_user_exposure
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
        and to_timestamp(created_at)::date between '2025-05-10'::date-7 and  '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_01"
    where type = 0
        and to_timestamp(created_at)::date between '2025-05-10'::date-7 and  '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_02"
    where type = 0
        and to_timestamp(created_at)::date between '2025-05-10'::date-7 and  '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_03"
    where type = 0
        and to_timestamp(created_at)::date between '2025-05-10'::date-7 and  '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_04"
    where type = 0
        and to_timestamp(created_at)::date between '2025-05-10'::date-7 and  '2025-05-10'::date       -- 限制时间
    ) t
)
-- 点击信息
, tmp_click_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , event
        , case when order_id like '%SH%' then order_id else CONCAT('SH', order_id) end as order_id  -- 用于匹配支付
        , watch_time
        , money::numeric(20,2) as money                 -- 用于计算预估广告收入
        , vid                   -- k币
        , eid                   -- k币
        , CAST(a.ext_body::json ->> 'type' AS text) as type         -- 用于看广告解锁
        , b.user_type
        , b.area
        , b.country_name
        , b.country_grade
        , b.ad_channel
        , b.user_group
        , b.lang_name
    from public.app_user_track_log a
    inner join tmp_user_exposure_info b on a.uid = b.uid and to_timestamp(a.created_at)::date = b.d_date    -- 找到当天有曝光的用户的点击行为
    where event in (112, 1, 192, 191, 2, 13, 14)                    -- 取的事件
        and to_timestamp(a.created_at)::date between '2025-05-10'::date-7 and  '2025-05-10'::date       -- 限制时间
)
-- 支付信息
, tmp_payment_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , order_num
        , money * 1.0 / 100 AS total_payment_amount
        , b.user_type
        , b.area
        , b.country_name
        , b.country_grade
        , b.ad_channel
        , b.user_group
        , b.lang_name
    from public.all_order_log a
    inner join tmp_user_exposure_info b on a.uid = b.uid and to_timestamp(a.created_at)::date = b.d_date -- 过滤出当天有曝光的用户
    where status = 1                                                    -- 支付成功
        and environment = 1                                             -- 生产环境
        and to_timestamp(a.created_at)::date between '2025-05-10'::date-7 and  '2025-05-10'::date       -- 限制时间
)
-- 对曝光信息进行聚合
, tmp_aggregated_exposure as (
    select
        d_date
        , user_type
        , area
        , country_name
        , country_grade
        , ad_channel
        , user_group
        , lang_name
        , count(distinct uid) as exposure_uv            -- 曝光人数
    from tmp_user_exposure_info
    group by d_date, user_type, area, country_name,country_grade , ad_channel, user_group, lang_name
)
-- 对点击信息进行聚合
, tmp_aggregated_click as (
    select
        d_date
        , user_type
        , area
        , country_name
        , country_grade
        , ad_channel
        , user_group
        , lang_name
        , COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_uv     -- 播放人数
        , COUNT(DISTINCT CASE WHEN event = 192 THEN uid END) AS recharge_submission_uv  -- 充值提交人数
        , COUNT(DISTINCT CASE WHEN event = 191 THEN uid END) AS episode_unlock_uv       -- 解锁剧集人数
        , COUNT(distinct CASE WHEN event IN (1, 2, 13, 14) THEN eid END) AS episode_play-- 播放集数
        , COUNT(CASE WHEN event = 192 THEN uid END) AS recharge_submission_pv           -- 充值提交次数
        , COUNT(distinct CASE WHEN event = 191 THEN eid END) AS episode_unlocks         -- 解锁剧集集数
        , count(distinct CASE WHEN event = 191 and type = '1' THEN eid END) as ad_episode_unlocks   --  广告解锁剧集集数
        , count(distinct CASE WHEN event = 191 and type = '2' THEN eid END) as non_ad_episode_unlocks -- 非广告解锁剧集集数
        , ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) AS watch_duration_minutes  -- 观看时长（分钟）
    from tmp_click_data
    group by d_date, user_type, area, country_name, country_grade, ad_channel, user_group, lang_name
)
-- 对支付信息进行聚合
, tmp_aggregated_payment as (
    SELECT
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.country_grade
        , a.ad_channel
        , a.user_group
        , a.lang_name
        , COALESCE(COUNT(DISTINCT a.uid), 0) AS successful_payment_uv
        , COALESCE(COUNT(DISTINCT a.order_num), 0) AS successful_payment_pv
        , COALESCE(SUM(a.total_payment_amount), 0) AS total_payment_amount
    FROM tmp_payment_data a
    GROUP BY a.d_date, a.user_type,a.area , a.country_name, a.country_grade, a.ad_channel, a.user_group,a.lang_name
)
-- 对k币信息进行聚合
, tmp_aggregated_k_consume as (
    select
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.country_grade
        , a.ad_channel
        , a.user_group
        , a.lang_name
        , sum(b.money) as k_consume_amount
    from tmp_click_data a           -- 曝光过滤过后的
    left join tmp_k_consume_data b on a.vid = b.goods_id and a.eid = b.goods_sku_id and a.d_date = b.d_date and a.uid = b.uid
    and a.event = 191
    group by a.d_date, a.user_type, a.area,a.country_name, a.country_grade, a.ad_channel, a.user_group, a.lang_name
)
, tmp_aggregated_retention as (
    select
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.country_grade
        , a.ad_channel
        , a.user_group
        , a.lang_name
        , count(distinct b.uid) as retention_1_days
        , count(distinct c.uid) as retention_3_days
        , count(distinct d.uid) as retention_7_days
    from tmp_user_exposure_info a
    left join (select uid,d_date from dwd_user_active where d_date between '2025-05-10'::date-7 and  '2025-05-10'::date group by  uid,d_date) b on a.uid = b.uid and b.d_date::date = a.d_date::date + 1
    left join (select uid,d_date from dwd_user_active where d_date between '2025-05-10'::date-7 and  '2025-05-10'::date group by  uid,d_date) c on a.uid = c.uid and c.d_date::date = a.d_date::date + 3
    left join (select uid,d_date from dwd_user_active where d_date between '2025-05-10'::date-7 and  '2025-05-10'::date group by  uid,d_date) d on a.uid = d.uid and d.d_date::date = a.d_date::date + 7
    group by a.d_date, a.user_type, a.area,a.country_name, a.country_grade, a.ad_channel, a.user_group, a.lang_name
)
select
    -- 分组维度
    a.d_date
    , a.user_type
    , a.user_group
    , a.area
    , a.country_name
    , cast(a.country_grade as text) as country_grade
    , a.ad_channel
    , a.lang_name
    -- 统计指标
    , coalesce(exposure_uv,0) as exposure_uv
    , coalesce(play_uv,0) as play_uv
    , coalesce(recharge_submission_uv,0) as recharge_submission_uv  -- 提交充值人数
    , coalesce(episode_unlock_uv,0) as episode_unlock_uv    -- 剧集解锁人数
    , coalesce(episode_play,0) as episode_play  -- 播放集数
    , coalesce(recharge_submission_pv,0) as recharge_submission_pv  -- 提交充值次数
    , coalesce(episode_unlocks,0) as episode_unlocks  -- 解锁集数
    , coalesce(ad_episode_unlocks,0)  as ad_episode_unlocks     -- 广告解锁集数
    , coalesce(non_ad_episode_unlocks,0)  as non_ad_episode_unlocks -- 非广告解锁集数
    , cast(coalesce(watch_duration_minutes,0)as bigint) as watch_duration_minutes  -- 观看时长
    , coalesce(successful_payment_uv,0) as successful_payment_uv  -- 支付成功人数
    , coalesce(successful_payment_pv,0) as successful_payment_pv -- 支付成功次数
    , cast(coalesce(total_payment_amount,0)as numeric(20,2)) as total_payment_amount -- 总支付金额
    , cast(coalesce(k_consume_amount,0)as numeric(20,2)) as k_consume_amount   -- 消耗k币数
    , coalesce(retention_1_days,0) as retention_1_days        -- 1日留存
    , coalesce(retention_3_days,0) as retention_3_days        -- 3日留存
    , coalesce(retention_7_days,0) as retention_7_days        -- 7日留存
from tmp_aggregated_exposure a
left join tmp_aggregated_click b on a.d_date = b.d_date and a.user_type = b.user_type  and a.area = b.area and a.country_name = b.country_name and a.country_grade = b.country_grade and a.ad_channel = b.ad_channel and a.user_group = b.user_group and a.lang_name = b.lang_name
left join tmp_aggregated_payment c on a.d_date = c.d_date and a.user_type = c.user_type and a.area = c.area and a.country_name = c.country_name and a.country_grade = c.country_grade and a.ad_channel = c.ad_channel and a.user_group = c.user_group and a.lang_name = c.lang_name
left join tmp_aggregated_k_consume d on a.d_date = d.d_date and a.user_type = d.user_type  and a.area = d.area and a.country_name = d.country_name and a.country_grade = d.country_grade and a.ad_channel = d.ad_channel and a.user_group = d.user_group and a.lang_name = d.lang_name
left join tmp_aggregated_retention e on a.d_date = e.d_date and a.user_type = e.user_type  and a.area = e.area and a.country_name = e.country_name and a.country_grade = e.country_grade and a.ad_channel = e.ad_channel and a.user_group = e.user_group and a.lang_name = e.lang_name
where a.user_group is not null;


delete from public.dw_recommend_playfinish_app where d_date between '2025-05-10'::date - 7 and '2025-05-10'::date;
insert into public.dw_recommend_playfinish_app select * from tmp.tmp_dw_recommend_playfinish_app;



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