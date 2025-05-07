------------------------------------------
-- file: 测试端内.sql
-- author: xiaoj
-- time: 2025/4/30 15:06
-- description:
------------------------------------------

-----------------------------------------
-- 测试端内曝光
-----------------------------------------
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
-- 每一日用户曝光的基本信息维表
-- 维度： 日期 uid
-- 用来补全信息  排除土耳其语和越南语用户 判断新老用户
user_type_derivation AS (
        SELECT
        d.uid,
        d.d_date AS 日期,
        COALESCE(d.user_type, 'Unknown') AS user_type,
--         d.id,
--         d.chinese_name,
--         ur.area,
--         ur.country_name,
--         ur.ad_channel,
        ur.user_group
--         ur.lang_name,
--         case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
--              when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
--              when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
--              when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
--              else '未知' END AS registration_period
    FROM public.dim_homepage_user d
    LEFT JOIN user_registration ur ON d.uid = ur.uid
    -- 增量更新
    -- WHERE d.d_date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
    -- 全量更新
    WHERE d.d_date BETWEEN '2025-04-01' AND (current_date+interval '-1 day') and d.user_type='olduser'
    group by d.uid,d.d_date,COALESCE(d.user_type, 'Unknown'),ur.user_group
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
    and to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
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
        -- 曾连更新
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
    from public.all_order_log a
    inner join recharge_counts rc
        on to_timestamp(a.created_at)::date = rc.日期 and a.uid = rc.uid
    WHERE
        -- 增量更新
        -- to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- 全量更新
        to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
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
select
    date,
    group_type,
    sum(successful_recharge_users) as successful_recharge_users,
    sum(successful_recharge_times) as successful_recharge_times,
    sum(total_recharge_amount) as total_recharge_amount
from (select
    us.日期 as date,
    cast(us.user_type as text ) as user_type,
    us.user_group as group_type,
    COUNT(DISTINCT CASE WHEN sr.uid IS NOT NULL THEN us.uid END) AS successful_recharge_users,
    sum(sr.successful_recharge_times)  AS successful_recharge_times,
    cast(COALESCE(SUM(sr.total_recharge_amount),  0)as numeric(20,2)) as total_recharge_amount
from user_type_derivation us
LEFT JOIN successful_recharges sr ON us.uid = sr.uid AND us.日期 = sr.日期
where us.user_group is not null
GROUP BY us.日期,
         us.user_type,
         us.user_group) t
where date = '2025-04-21'
group by date , group_type