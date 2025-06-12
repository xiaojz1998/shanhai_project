---------------------------------------------
-- File: 规划核心指标看板.sql
-- Time: 2025/6/10 15:08
-- User: xiaoj
-- Description:  
---------------------------------------------
SET timezone = 'UTC-0';
---------------------------------------------
-- 建表
---------------------------------------------
-- drop table if exists public.dw_core_indicators;
CREATE TABLE if not exists public.dw_core_indicators (
    week_start date,
    week_end date,
    area text,
    country_name text,
    lang_name text,
    failed_order_count bigint,
    generated_order_count bigint,
    total_pay_users bigint,
    dau bigint,
    total_pay_orders bigint,
    total_orders_including_failed bigint,
    total_pay_amount numeric(18,2),
    advertising_income numeric(18,2),
    renewal_users bigint,
    expiring_members bigint,
    total_second_day_login_users bigint,
    total_3_day_retention bigint,
    total_7_day_retention bigint,
    total_30_day_retention bigint,
    total_60_day_retention bigint,
    total_120_day_retention bigint,
    reached_users bigint,
    clicked_users bigint,
    total_watch_duration bigint,
    watch_users bigint,
    total_episode_watches bigint,
    task_doers bigint,
    pre_pay_watch_uv_complete bigint,
    free_episodes_watch_uv bigint,
    new_bound_users bigint,
    new_dau bigint,
    new_dau_2login bigint,
    total_user bigint,
    week_dau bigint
);

-- drop table if exists tmp.dw_core_indicators_tmp01;
CREATE TABLE if not exists tmp.dw_core_indicators_tmp01 (
    week_start date,
    week_end date,
    area text,
    country_name text,
    lang_name text,
    failed_order_count bigint,
    generated_order_count bigint,
    total_pay_users bigint,
    dau bigint,
    total_pay_orders bigint,
    total_orders_including_failed bigint,
    total_pay_amount numeric(18,2),
    advertising_income numeric(18,2),
    renewal_users bigint,
    expiring_members bigint,
    total_second_day_login_users bigint,
    total_3_day_retention bigint,
    total_7_day_retention bigint,
    total_30_day_retention bigint,
    total_60_day_retention bigint,
    total_120_day_retention bigint,
    reached_users bigint,
    clicked_users bigint,
    total_watch_duration bigint,
    watch_users bigint,
    total_episode_watches bigint,
    task_doers bigint,
    pre_pay_watch_uv_complete bigint,
    free_episodes_watch_uv bigint,
    new_bound_users bigint,
    new_dau bigint,
    new_dau_2login bigint,
    total_user bigint,
    week_dau bigint
);



---------------------------------------------
-- 更新
---------------------------------------------
truncate table tmp.dw_core_indicators_tmp01;
INSERT INTO tmp.dw_core_indicators_tmp01
WITH weekly_date_ranges AS (
    WITH start_date AS (
        SELECT '2024-07-01'::date AS start
    ),
    date_series AS (
        SELECT generate_series(start,current_date::date, '1 day') AS date
        FROM start_date
    ),
    weekly_start AS (
        SELECT
            date_trunc('week', date + INTERVAL '1 day')::date       -- 对每个日期截断成每周的开始date
            AS week_start
        FROM date_series
        GROUP BY week_start                                         -- 对截断后的日期去重
    )
    SELECT
        week_start,
        (week_start + INTERVAL '6 days')::date AS week_end          -- 增加列属性
    FROM weekly_start
    ORDER BY week_start
),

orders_summary AS (
    SELECT
        week_start,
        week_end,
        SUM(支付失败订单数) as failed_order_count,
        SUM(生成订单数) as generated_order_count,
        -- 报表维度
        区域,
        国家,
        'UNKNOWN' as 语言
    FROM public.dw_order_failed                                 --  从订单失败表取值
    JOIN weekly_date_ranges
    ON 日期::date BETWEEN week_start AND week_end
    GROUP BY week_start, week_end, 国家 , 区域
),

operate_view_summary AS (
    SELECT
        week_start,
        week_end,
        SUM(pay_user) as total_pay_users,
        SUM(dau) as dau,                                    -- dau
        SUM(new_dau) as new_dau,
        SUM(pay_order) as total_pay_orders,
        SUM(all_pay_order) as total_orders_including_failed,
        SUM(pay_amt) as total_pay_amount,                   -- 总充值金额
        SUM(ad_income_amt) as advertising_income,
        SUM(repay_user) as renewal_users,
        SUM(due_user) as expiring_members,
        SUM(dau_2login) as total_second_day_login_users,
        sum(new_dau_2login) as new_dau_2login,              -- 新用户第二日登录人数
        -- 报表维度
        area as 区域,
        country_name as 国家,
        lang_name as 语言
    FROM public.dw_operate_view                             -- 从经营概览取值
    JOIN weekly_date_ranges
    ON d_date::date BETWEEN week_start AND week_end
    GROUP BY week_start, week_end, country_name, lang_name ,  area
),

retention_summary AS (
    SELECT
        week_start,
        week_end,
        SUM(总3日留存) as total_3_day_retention,
        SUM(总7日留存) as total_7_day_retention,
        SUM(总30日留存) as total_30_day_retention,
        SUM(dau_60login) as total_60_day_retention,
        SUM(dau_120login) as total_120_day_retention,
        -- 报表维度
        区域,
        国家,
        -- 'UNKNOWN' as 语言
        lang_name as 语言
    FROM public.dw_retention_daily
    JOIN weekly_date_ranges
    ON active_date::date BETWEEN week_start AND week_end
    GROUP BY week_start, week_end, 国家,lang_name,区域
),
-- 取到push相关信息
push_summary AS (
    with language_data AS (
        SELECT
            push_unt,
            click_unt,
            d_date,
            TRIM(unnest(string_to_array(regexp_replace(lang_name, '[{}]', '', 'g'), ','))) AS lang
        FROM public.dw_push_view
        )
    SELECT
        week_start,
        week_end,
        SUM(push_unt) AS reached_users,
        SUM(click_unt) AS clicked_users,
        '未知' AS 区域,
        '未知' AS 国家,
        CASE
            WHEN lang ='英语阿拉伯语' THEN '阿拉伯语'
            WHEN lang = '""' THEN 'UNKNOWN'
            ELSE lang
        END AS 语言
    FROM language_data
    JOIN weekly_date_ranges ON d_date::date BETWEEN week_start AND week_end
    GROUP BY week_start, week_end,
        CASE
            WHEN lang ='英语阿拉伯语' THEN '阿拉伯语'
            WHEN lang = '""' THEN 'UNKNOWN'
            ELSE lang
        END
        -- SELECT
        --     week_start,
        --     week_end,
        --     SUM(push_unt) as reached_users,
        --     SUM(click_unt) as clicked_users,
        --     '未知' as 国家,
        --     'UNKNOWN' as 语言
        -- FROM public.dw_push_view
        -- JOIN weekly_date_ranges
        -- ON push_time::date BETWEEN week_start AND week_end
        -- GROUP BY week_start, week_end
),
-- 从运营月报表取值
schedule_summary AS (
    SELECT
        week_start,
        week_end,
        SUM(watch_duration) as total_watch_duration,
        SUM(watch_user) as watch_users,
        SUM(eid_watch_cnt) as total_episode_watches,
        area as 区域,
        country_name as 国家,
        lang_name as 语言
    FROM public.dw_operate_schedule
    JOIN weekly_date_ranges
    ON d_date::date BETWEEN week_start AND week_end
    GROUP BY week_start, week_end, area ,country_name,lang_name
),
-- 从福利页底表取值
task_summary AS (
    SELECT
        week_start,
        week_end,
        b.area as 区域,
        b.country_name as 国家,
         -- 'UNKNOWN' as 语言
        lang_name as 语言,
        SUM(做任务人数) as task_doers
--        SUM(DAU) as DAU,
    FROM public.dw_rewards_view a
    JOIN weekly_date_ranges
    ON d_date::date BETWEEN week_start AND week_end
     JOIN v_dim_country_area b ON a.country_code = b.country_code
    GROUP BY week_start, week_end
        ,b.area
        ,b.country_name
        ,lang_name
--     WITH newuser_tb AS (
--         SELECT
--             uid,
--             d_date,
--             v_date,
--             country_code,
--             ad_channel,
--             lang_name
--         FROM public.dwd_user_info
--     ),
--      combined_task AS (
--     SELECT DISTINCT
--         uid,
--         TO_TIMESTAMP(created_at)::date AS d_date
--     FROM "oversea-api_osd_user_task"
--     WHERE
--         TO_TIMESTAMP(created_at)::date >= '2024-07-01'
--         AND status IN (1, 2, 3)
--     UNION
--     SELECT DISTINCT
--         uid,
--         TO_TIMESTAMP(created_at)::date AS d_date
--     FROM "app_user_track_log"
--     WHERE
--         TO_TIMESTAMP(created_at)::date >= '2024-07-01'
--         AND event IN (41)
-- )
-- SELECT
--         DATE_TRUNC('week', n1.d_date)::date AS week_start,
--         (DATE_TRUNC('week', n1.d_date) + INTERVAL '6 days')::date AS week_end,
--         b.country_name AS 国家,
--         COALESCE(nn.lang_name, 'UNKNOWN') AS 语言,
--         COUNT(DISTINCT n1.uid) AS  task_doers
--     FROM combined_task n1
--     LEFT JOIN newuser_tb nn ON n1.uid::text = nn.uid
--     JOIN v_dim_country_area b ON nn.country_code = b.country_code
--     GROUP BY DATE_TRUNC('week', n1.d_date), b.country_name, COALESCE(nn.lang_name, 'UNKNOWN')
),
-- 观看统计
newuser_watch_summary AS (
    WITH newuser_tb AS (
        SELECT
            uid,
            d_date,                                 -- 注册日期
            v_date,
            country_code,
            ad_channel,
            lang_name
        FROM public.dwd_user_info                   -- 用户信息表，包括用户的注册信息
    ),
    -- 统计观看数据
    daily_watch_summary AS (
        SELECT
            TO_TIMESTAMP(a.created_at)::date AS d_date,
            a.vid,
            a.uid,
            COUNT(CASE WHEN event IN (1, 2, 13, 14) AND eid > 0 THEN uid ELSE NULL END) AS watch_pv,
            -- COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) AND b.sort = (v1.pay_num - 1) THEN uid ELSE NULL END) AS pre_pay_watch_uv,
            -- COUNT(DISTINCT CASE WHEN event = 87 AND b.sort = (v1.pay_num - 1) THEN uid ELSE NULL END) AS pre_pay_watch_uv_complete,
            COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) AND b.sort = (v1.pay_num - 1) THEN uid ELSE NULL END) AS pre_pay_watch_uv_complete,
            COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) AND b.sort < v1.pay_num THEN uid ELSE NULL END) AS free_episodes_watch_uv
        FROM public.app_user_track_log a
        INNER JOIN "oversea-api_osd_video_episodes" b ON a.eid = b.id
        LEFT JOIN public."oversea-api_osd_videos" v1 ON a.vid = v1.id
        WHERE
        -- event IN (1, 2, 13, 14, 87)
        event IN (1, 2, 13, 14)
          AND a.vid = b.vid
          AND TO_TIMESTAMP(a.created_at)::date >= '2024-07-01'
        GROUP BY TO_TIMESTAMP(a.created_at)::date, a.vid, a.uid
    )
    SELECT
        DATE_TRUNC('week', n1.d_date)::date AS week_start,
        (DATE_TRUNC('week', n1.d_date) + INTERVAL '6 days')::date AS week_end,
        b.area  as 区域,
        b.country_name AS 国家,
        COALESCE(nn.lang_name, 'UNKNOWN') AS 语言,
        SUM(CASE WHEN n1.pre_pay_watch_uv_complete > 0 THEN 1 ELSE 0 END) AS pre_pay_watch_uv_complete,
        SUM(CASE WHEN n1.free_episodes_watch_uv > 0 THEN 1 ELSE 0 END) AS free_episodes_watch_uv
    FROM daily_watch_summary n1
    LEFT JOIN newuser_tb nn ON n1.uid::text = nn.uid
    JOIN v_dim_country_area b ON nn.country_code = b.country_code
    GROUP BY DATE_TRUNC('week', n1.d_date),b.area, b.country_name, COALESCE(nn.lang_name, 'UNKNOWN')
),
-- 新增用户
newuser_bind_summary AS (
    WITH newuser_tb AS (
    SELECT
        uid,
        d_date,
        v_date,
        country_code,
        ad_channel,
        lang_name
    FROM public.dwd_user_info
),
weekly_stats AS (
    SELECT
        DATE_TRUNC('week', TO_TIMESTAMP(a.created_at)::date)::date AS week_start,
        (DATE_TRUNC('week', TO_TIMESTAMP(a.created_at)::date) + INTERVAL '6 days')::date AS week_end,
        b.area as 区域,
        b.country_name AS 国家,
        COALESCE(nn.lang_name, 'UNKNOWN') AS 语言,
        COUNT(DISTINCT CASE WHEN a.type <> 0 THEN a.uid END) AS 新增绑定账号用户数,
        COUNT(DISTINCT a.uid) AS 总用户数
    FROM public."oversea-api_osd_user" a
    LEFT JOIN newuser_tb nn ON a.uid::text = nn.uid
    JOIN v_dim_country_area b ON nn.country_code = b.country_code
    WHERE TO_TIMESTAMP(a.created_at)::date >= '2024-07-01'
    GROUP BY DATE_TRUNC('week', TO_TIMESTAMP(a.created_at)::date),b.area, b.country_name, COALESCE(nn.lang_name, 'UNKNOWN')
)
SELECT
    ws.week_start,
    ws.week_end,
    ws.区域,
    ws.国家,
    ws.语言,
    SUM(ws.新增绑定账号用户数) OVER (PARTITION BY ws.区域,ws.国家, ws.语言 ORDER BY ws.week_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS 绑定账号的累计注册用户数,
    SUM(ws.总用户数) OVER (PARTITION BY ws.区域,ws.国家, ws.语言 ORDER BY ws.week_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS 累计注册用户数
FROM weekly_stats ws
ORDER BY ws.week_start
),
-- 计算每周的去重dau
week_active_user as (
   WITH newuser_tb AS (
        SELECT
            uid,
            d_date,
            v_date,
            country_code,
            ad_channel,
            lang_name
        FROM public.dwd_user_info
    )
   select
        date_trunc('week',nn.d_date::date) as week_start,
        (date_trunc('week',nn.d_date::date) + INTERVAL '6 days')::date AS week_end,
        b.area as 区域,
        b.country_name AS 国家,
        nn.lang_name AS 语言,
        count(distinct a.uid) as week_dau
   from public.dwd_user_active a
   left join newuser_tb nn on a.uid::text = nn.uid
   JOIN v_dim_country_area b ON nn.country_code = b.country_code
   group by date_trunc('week',nn.d_date::date),b.area, b.country_name, nn.lang_name
)
SELECT
    COALESCE(o.week_start, ov.week_start, r.week_start, p.week_start, s.week_start, t.week_start, nw.week_start, nub.week_start,wau.week_start) AS week_start,
    COALESCE(o.week_end, ov.week_end, r.week_end, p.week_end, s.week_end, t.week_end, nw.week_end, nub.week_end,wau.week_end) AS week_end,
    -- COALESCE(o.国家, ov.国家, r.国家, p.国家, s.国家, t.国家, nw.国家, nub.国家) AS country_name,
    -- COALESCE(o.语言, ov.语言, r.语言, p.语言, s.语言, t.语言, nw.语言, nub.语言) AS lang_name,
    o.区域 as area,
    o.国家 as country_name,
    o.语言 AS lang_name,
    ov.failed_order_count,
    ov.generated_order_count,
    o.total_pay_users,
    o.dau,
    o.total_pay_orders,
    o.total_orders_including_failed,
    o.total_pay_amount,
    o.advertising_income,
    o.renewal_users,
    o.expiring_members,
    o.total_second_day_login_users,
    r.total_3_day_retention,
    r.total_7_day_retention,
    r.total_30_day_retention,
    r.total_60_day_retention,
    r.total_120_day_retention,
    p.reached_users,
    p.clicked_users,
    s.total_watch_duration,
    s.watch_users,
    s.total_episode_watches,
    t.task_doers,
    nw.pre_pay_watch_uv_complete,
    nw.free_episodes_watch_uv,
    nub.绑定账号的累计注册用户数 as new_bound_users,
    o.new_dau,
    o.new_dau_2login,
    nub.累计注册用户数 as total_user,
    wau.week_dau
-- FROM orders_summary o
FROM operate_view_summary o
-- FULL JOIN operate_view_summary ov
--     ON o.week_start = ov.week_start AND o.week_end = ov.week_end AND o.国家 = ov.国家 AND o.语言 = ov.语言
    FULL JOIN orders_summary ov
    ON o.week_start = ov.week_start AND o.week_end = ov.week_end AND o.国家 = ov.国家 AND o.语言 = ov.语言 and o.区域 = ov.区域
FULL JOIN retention_summary r
    ON o.week_start = r.week_start AND o.week_end = r.week_end AND o.国家 = r.国家 AND o.语言 = r.语言 and o.区域 = r.区域
FULL JOIN push_summary p
    ON o.week_start = p.week_start AND o.week_end = p.week_end AND o.国家 = p.国家 AND o.语言 = p.语言 and o.区域 = p.区域
FULL JOIN schedule_summary s
    ON o.week_start = s.week_start AND o.week_end = s.week_end AND o.国家 = s.国家 AND o.语言 = s.语言 and o.区域 = s.区域
FULL JOIN task_summary t
    ON o.week_start = t.week_start AND o.week_end = t.week_end AND o.国家 = t.国家 AND o.语言 = t.语言 and o.区域 = t.区域
FULL JOIN newuser_watch_summary nw
    ON o.week_start = nw.week_start AND o.week_end = nw.week_end AND o.国家 = nw.国家 AND o.语言 = nw.语言 and o.区域 = nw.区域
FULL JOIN newuser_bind_summary nub
    ON o.week_start = nub.week_start AND o.week_end = nub.week_end AND o.国家 = nub.国家 AND o.语言 = nub.语言 and  o.区域 = nub.区域
full join week_active_user wau
    ON o.week_start = wau.week_start AND o.week_end = wau.week_end AND o.国家 = wau.国家 AND o.语言 = wau.语言 and o.区域 = wau.区域
ORDER BY COALESCE(o.week_start, ov.week_start, r.week_start, p.week_start, s.week_start, t.week_start, nw.week_start, nub.week_start,wau.week_start) desc
--,o.failed_order_count desc
;
truncate table public.dw_core_indicators;
INSERT INTO public.dw_core_indicators select * from tmp.dw_core_indicators_tmp01;




-- SET timezone = 'UTC-0';

truncate table tmp.dw_core_indicators_xmp_tmp01;
INSERT INTO tmp.dw_core_indicators_xmp_tmp01
WITH weekly_date_ranges AS (
    WITH start_date AS (
        SELECT '2024-07-01'::date AS start
    ),
    date_series AS (
        SELECT generate_series(start, current_date::date, '1 day') AS date
        FROM start_date
    ),
    weekly_start AS (
        SELECT
            date_trunc('week', date)::date AS week_start
        FROM date_series
        GROUP BY week_start
    )
    SELECT
        week_start,
        (week_start + INTERVAL '6 days')::date AS week_end
    FROM weekly_start
    ORDER BY week_start
),
ad_data AS (
WITH organic_revenue AS (
    SELECT
        date_trunc('week', (install_at)::date)::date AS week_start,
        SUM(event_revenue_usd) / 100.0 AS af_revenue_organic
--        from public.app_purchase_event_log_pull
        -- from public.app_purchase_event_log
        from public.dwd_app_purchase_event_log
    where ad_channel = 'organic'
    AND (install_at)::date >= '2024-12-17'
    and install_date=created_date
    GROUP BY
    date_trunc('week', (install_at)::date)
    order by date_trunc('week', (install_at)::date)
)

SELECT
    tb.week_start,
    xmp.total_cost,
    tb.erchuang_cost,
    xmp.af_revenue_1 as af1,
    COALESCE(org.af_revenue_organic, 0) + xmp.af_revenue_1 AS af_revenue_1,
    tb.impression,
    tb.click,
    tb.video_play,
    tb.video_play_p25
FROM (
    SELECT
        date_trunc('week', TO_TIMESTAMP(tb.created_at)::date)::date AS week_start,
        -- SUM(tb.cost_amount) / 100.0 AS total_cost,
        SUM(CASE WHEN SPLIT_PART(tb.material_name, '_', 8) = '二创' THEN tb.cost_amount ELSE 0 END) / 100.0 AS erchuang_cost,
        -- SUM(tb.af_revenue_1) / 100.0 AS af_revenue_1,
        SUM(tb.impression) AS impression,
        SUM(tb.click) AS click,
        SUM(tb.video_play) AS video_play,
        SUM(tb.video_play_p25) as video_play_p25
    FROM public.ad_material_data_log tb
    LEFT JOIN public.v_dim_ad_campaign_info tc ON tb.campaign_id = tc.campaign_id
    WHERE
--    TO_TIMESTAMP(tb.created_at)::date = '2024-12-01'
    TO_TIMESTAMP(tb.created_at)::date >= '2024-07-01'
    GROUP BY date_trunc('week', TO_TIMESTAMP(tb.created_at)::date)
) tb
LEFT JOIN organic_revenue org ON tb.week_start = org.week_start
LEFT JOIN (
    SELECT
        date_trunc('week', TO_TIMESTAMP(created_at)::date)::date AS week_start,
        SUM(af_revenue_1) / 100.0 AS af_revenue_1,
        SUM(cost) / 100.0 AS total_cost
    FROM public.creative_indicators_data_xmp_log
    WHERE TO_TIMESTAMP(created_at)::date >= '2024-07-01'
    GROUP BY date_trunc('week', TO_TIMESTAMP(created_at)::date)
) xmp ON tb.week_start = xmp.week_start
),
operate_data AS (
    SELECT
        date_trunc('week', d_date::date)::date AS week_start,
        SUM(new_pay_amt) as new_pay_amt,
        SUM(new_dau) as new_dau,
        SUM(pay_user) as pay_user,
        SUM(dau) as dau
    FROM public.dw_operate_view
    WHERE d_date::date >= '2024-07-01'
    GROUP BY date_trunc('week', d_date::date)
)
SELECT
    wdr.week_start,
    wdr.week_end,
    COALESCE(ad.total_cost, 0) AS weekly_total_cost,
    COALESCE(ad.erchuang_cost, 0) AS weekly_erchuang_cost,
    COALESCE(ad.af_revenue_1, 0) AS weekly_af_revenue_1,
    COALESCE(ad.impression, 0) AS weekly_impression,
    COALESCE(ad.click, 0) AS weekly_click,
    COALESCE(ad.video_play, 0) AS weekly_video_play,
    COALESCE(ad.video_play_p25, 0) AS weekly_video_play_p25,
    COALESCE(od.new_pay_amt, 0) AS weekly_new_pay_amt,
    COALESCE(od.new_dau, 0) AS weekly_new_dau,
    COALESCE(od.pay_user, 0) AS weekly_pay_user,
    COALESCE(od.dau, 0) AS weekly_dau
FROM weekly_date_ranges wdr
LEFT JOIN ad_data ad ON wdr.week_start = ad.week_start
LEFT JOIN operate_data od ON wdr.week_start = od.week_start
ORDER BY wdr.week_start;

truncate table public.dw_core_indicators_xmp;
INSERT INTO public.dw_core_indicators_xmp select * from tmp.dw_core_indicators_xmp_tmp01;