---------------------------------------------
-- File: bi.sql
-- Time: 2025/6/7 14:04
-- User: xiaoj
-- Description:  
---------------------------------------------

WITH LatestWeekStart AS (
    SELECT MAX(week_start) AS max_week_start
    FROM public.dw_core_indicators
)
SELECT
    week_start AS 开始日期,
    week_end AS 结束日期,
    country_name AS 国家,
    lang_name AS 语言,
    failed_order_count AS 支付失败订单数,
    generated_order_count AS 生成订单数,
    total_pay_users AS 总充值人数,
    dau AS dau,
    new_dau AS 新增用户数,
    total_pay_orders AS 充值订单数,
    total_orders_including_failed AS 总订单数（包含失败）,
    total_pay_amount AS 总充值金额,
    advertising_income AS 商业化广告收入,
    renewal_users AS 续订人书,
    expiring_members AS 会员到期人数,
    total_second_day_login_users AS 次留,
    total_3_day_retention AS "3留",
    total_7_day_retention AS "7留",
    total_30_day_retention AS "30留",
    total_60_day_retention AS "60留",
    total_120_day_retention AS "120留",
    reached_users AS 推送触达人数,
    clicked_users AS 推送点击人数,
    total_watch_duration AS 总观看时长,
    watch_users AS 播放人数,
    total_episode_watches AS 总观看集数,
    task_doers AS 做任务人数,
    pre_pay_watch_uv_complete AS 付费前一集完播人数,
    free_episodes_watch_uv AS 剧免费集播放人数,
    new_bound_users AS 新增绑定用户数,
    total_user as 累计新增用户数
FROM public.dw_core_indicators, LatestWeekStart
WHERE week_start <= LatestWeekStart.max_week_start - INTERVAL '7 day'
ORDER BY week_start DESC