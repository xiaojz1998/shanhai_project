------------------------------------------
-- file: 推荐位新用户_用户维度_增量一天_d.sql
-- author: xiaoj
-- time: 2025/5/21 14:18
-- description:
------------------------------------------
SET timezone = 'UTC-0';

-- truncate table public.dim_playretain_user;
-- INSERT INTO public.dim_playretain_user
delete from public.dim_playretain_user where d_date between (current_date+interval '-2 day') and (current_date+interval '-1 day');
INSERT INTO public.dim_playretain_user
WITH user_registration AS (
    SELECT
        TO_TIMESTAMP(created_at) :: date AS register_date,
        TO_TIMESTAMP(created_at) AS register_timestamp,
        uid::int8 AS uid,
        country_code,
        ad_channel
    FROM (
        SELECT
            created_at,
            uid,
            area AS country_code,
            ad_channel,
            ROW_NUMBER() OVER (PARTITION BY uid ORDER BY created_at) AS rk
        FROM "user_log"
        WHERE event = 1
          AND created_date >= 20240701
    ) a
    WHERE rk = 1
),
user_exposure AS (
    SELECT
        atl.uid,
        TO_TIMESTAMP(atl.created_at)::date AS d_date,
        CAST(atl.ext_body::json ->> 'show_title' AS text) AS show_title,
        MIN(TO_TIMESTAMP(atl.created_at)) AS min_exposure_time,
        ur.register_timestamp
    FROM public.app_user_cover_show_log atl
    LEFT JOIN user_registration ur ON atl.uid = ur.uid
    WHERE event = 111
    --   AND TO_TIMESTAMP(atl.created_at)::date BETWEEN '2025-03-01' AND (current_date + INTERVAL '-1 day')
      AND TO_TIMESTAMP(atl.created_at)::date BETWEEN (current_date+interval '-2 day') and (current_date+interval '-1 day')
      AND CAST(ext_body::json ->> 'page' AS int) = 3
      AND ext_body::json ->> 'show_title' = 'playRetain'
    GROUP BY atl.uid, TO_TIMESTAMP(atl.created_at)::date, ur.register_timestamp, CAST(atl.ext_body::json ->> 'show_title' AS text)
)
SELECT
    ue.uid,
    ue.d_date,
    CASE
        WHEN ue.register_timestamp IS NULL THEN 'Unknown'
        WHEN ue.min_exposure_time <= (ue.register_timestamp + INTERVAL '24 hours') THEN 'newuser'
        ELSE 'olduser'
    END AS user_type,
    ue.show_title,
    ue.min_exposure_time,
    ue.register_timestamp
FROM user_exposure ue
;


-- truncate table public.dim_homepage_user;
-- INSERT INTO public.dim_homepage_user
delete from public.dim_homepage_user where d_date between (current_date+interval '-2 day') and (current_date+interval '-1 day');
INSERT INTO public.dim_homepage_user
-- 从user_log 表中获得用户注册信息
WITH user_registration AS (
    SELECT
        TO_TIMESTAMP(created_at) :: date AS register_date,
        TO_TIMESTAMP(created_at) AS register_timestamp,
        uid::int8 AS uid,
        country_code,
        ad_channel
    FROM (
        SELECT
            created_at,
            uid,
            area AS country_code,
            ad_channel,
            ROW_NUMBER() OVER (PARTITION BY uid ORDER BY created_at) AS rk --用于去重
        FROM "user_log"
        WHERE event = 1
          AND created_date >= 20240701
    ) a
    WHERE rk = 1
),
--------------------------------------------------------------------------------------------------------
-- 只有曝光数据有时间限制，而其他的都是全量数据，
-- 意味着每天跑出来的数据都是那天的前两个推荐位的信息，而不是观察数据当天的推荐位信息
-- 这个表不能全量跑
--------------------------------------------------------------------------------------------------------
user_exposure AS (
    SELECT
        atl.uid,                                                -- 用户id
        TO_TIMESTAMP(atl.created_at)::date AS d_date,           -- 当天日期
        model_id,                                               -- 推荐位id
        MIN(TO_TIMESTAMP(atl.created_at)) AS min_exposure_time, -- 当天的最小曝光时间
        ur.register_timestamp                           -- 注册时间
    FROM public.app_user_cover_show_log atl             -- 用户行为埋点记录表
    LEFT JOIN user_registration ur ON atl.uid = ur.uid
    WHERE event = 111
      AND TO_TIMESTAMP(atl.created_at)::date BETWEEN (current_date+interval '-2 day') and (current_date+interval '-1 day')
    --   AND TO_TIMESTAMP(atl.created_at)::date BETWEEN '2025-03-10' and (current_date+interval '-1 day')
      AND CAST(ext_body::json ->> 'page' AS int) = 1
    GROUP BY atl.uid, TO_TIMESTAMP(atl.created_at)::date, ur.register_timestamp, model_id
),
tabs AS (
    SELECT DISTINCT CAST(unnest(string_to_array(tab_ids, ',')) AS INTEGER) AS tab_id --关联的顶部tab id 用，分割
    FROM "oversea-api_osd_home_page"            -- 客户端首页表
    WHERE status = 1                            -- 状态是开启的
    AND deleted_at IS NULL                      -- 没有被软删除
    AND name NOT LIKE '%官网%'                   -- 名称
),
recommends AS (
    SELECT DISTINCT
        id AS tab_id,                           -- tab id
        CAST(unnest(string_to_array(recommend_ids, ',')) AS INTEGER) AS recommend_id    -- 推荐位
    FROM "oversea-api_osd_tabs"                 -- 首页顶部tab表
    WHERE status = 1                            -- 首页状态 1开启 2关闭
    AND deleted_at IS NULL                      -- 没有被软删除
    AND sort = 1                                -- tab排序
    AND name NOT LIKE '%官网%'                   -- tab后台名称
),
-- 上述两个临时表汇总信息： 有很多个客户端首页，一个客户端首页有很多个tab ，一个tab有很多个推荐位

--
ranked_recommendations AS (
    SELECT
        b.tab_id,
        a.id,
        a.chinese_name,
        a.english_name,
        a.sort,                                     -- 推荐位排序
        ROW_NUMBER() OVER (
            PARTITION BY b.tab_id
            ORDER BY
                CASE WHEN a.sort=0 THEN 1 ELSE 0 END, -- NULLs come last
                a.sort ASC -- Then sort by actual sort value
        ) AS rank
    FROM "oversea-api_osd_recommend" a              -- 推荐位表
    INNER JOIN recommends b ON a.id = b.recommend_id --限制目前有tabid 的推荐位
    INNER JOIN tabs c ON b.tab_id = c.tab_id        -- 限制目前首页有用到的tab id
    WHERE a.status = 1                              -- 启用的推荐位
    AND a.deleted_at IS NULL                        -- 没有被软删除
),
final_recommendations AS (
    SELECT
        id,
        chinese_name,
        english_name,
        sort
    FROM (
        SELECT
            tab_id,
            id,
            chinese_name,
            english_name,
            sort
        FROM ranked_recommendations
        WHERE rank <= 2                          -- 获取前2个推荐位
        ORDER BY tab_id, sort ASC
    ) tt
    GROUP BY id, chinese_name, english_name, sort
)
SELECT
    ue.uid,
    ue.d_date,
    CASE
        WHEN ue.register_timestamp IS NULL THEN 'Unknown'
        WHEN ue.min_exposure_time <= (ue.register_timestamp + INTERVAL '24 hours') THEN 'newuser'
        ELSE 'olduser'
    END AS user_type,
    -- ue.model_id,
    fr.id,
    fr.chinese_name,
    ue.min_exposure_time,
    ue.register_timestamp
FROM user_exposure ue
LEFT JOIN final_recommendations fr ON ue.model_id::int = fr.id
where fr.id is not null                 -- 排除掉关联不到前两个推荐位的 推荐位曝光记录
;