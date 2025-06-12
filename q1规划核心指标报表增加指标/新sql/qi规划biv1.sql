---------------------------------------------
-- File: qi规划biv1.sql
-- Time: 2025/6/7 16:54
-- User: xiaoj
-- Description:  
---------------------------------------------
-- 用于取到最新周的上一周，只取完整的周
WITH LatestWeekStart AS (
    SELECT MAX(week_start) AS max_week_start
    FROM public.dw_core_indicators
)
SELECT
    week_start AS 开始日期,
    week_end AS 结束日期,
    area as 区域,
    country_name AS 国家,
    lang_name AS 语言,
    failed_order_count AS 支付失败订单数,
    lag(failed_order_count) over(partition by area,country_name,lang_name order by week_start) AS 上周支付失败订单数,
    generated_order_count AS 生成订单数,
    lag(generated_order_count) over (partition by area,country_name,lang_name order by week_start) as 上周生成订单数,
    total_pay_users AS 总充值人数,
    lag(total_pay_users) over (partition by area,country_name,lang_name order by week_start) as 上周总充值人数,
    dau AS dau,
    lag(dau) over (partition by area,country_name,lang_name order by week_start) as 上周dau,
    new_dau AS 新增用户数,
    lag(new_dau) over (partition by area,country_name,lang_name order by week_start) as 上周新增用户数,
    new_dau_2login as 新增用户次日留存数,
    lag(new_dau_2login) over (partition by area,country_name,lang_name order by week_start) as 上周新增用户次日留存数,
    total_pay_orders AS 充值订单数,
    lag(total_pay_orders) over (partition by area,country_name,lang_name order by week_start) as 上周充值订单数,
    total_orders_including_failed AS 总订单数（包含失败）,
    lag(total_orders_including_failed) over (partition by area,country_name,lang_name order by week_start) as 上周总订单数（包含失败）,
    total_pay_amount AS 总充值金额,
    lag(total_pay_amount) over (partition by area,country_name,lang_name order by week_start) as 上周总充值金额,
    advertising_income AS 商业化广告收入,
    lag(advertising_income) over (partition by area,country_name,lang_name order by week_start) as 上周商业化广告收入,
    renewal_users AS 续订人数,
    lag(renewal_users) over (partition by area,country_name,lang_name order by week_start) as 上周续订人数,
    expiring_members AS 会员到期人数,
    lag(expiring_members) over (partition by area,country_name,lang_name order by week_start) as 上周会员到期人数,
    total_second_day_login_users AS 次留,
    lag(total_second_day_login_users) over (partition by area,country_name,lang_name order by week_start) as 上周次留,
    total_3_day_retention AS "3留",
    lag(total_3_day_retention) over (partition by area,country_name,lang_name order by week_start) as 上周3留,
    total_7_day_retention AS "7留",
    lag(total_7_day_retention) over (partition by area,country_name,lang_name order by week_start) as 上周7留,
    total_30_day_retention AS "30留",
    lag(total_30_day_retention) over (partition by area,country_name,lang_name order by week_start) as 上周30留,
    total_60_day_retention AS "60留",
    lag(total_60_day_retention) over (partition by area,country_name,lang_name order by week_start) as 上周60留,
    total_120_day_retention AS "120留",
    lag(total_120_day_retention) over (partition by area,country_name,lang_name order by week_start) as 上周120留,
    reached_users AS 推送触达人数,
    lag(reached_users) over (partition by area,country_name,lang_name order by week_start) as 上周推送触达人数,
    clicked_users AS 推送点击人数,
    lag(clicked_users) over (partition by area,country_name,lang_name order by week_start) as 上周推送点击人数,
    total_watch_duration AS 总观看时长,
    lag(total_watch_duration) over (partition by area,country_name,lang_name order by week_start) as 上周总观看时长,
    watch_users AS 播放人数,
    lag(watch_users) over (partition by area,country_name,lang_name order by week_start) as 上周播放人数,
    total_episode_watches AS 总观看集数,
    lag(total_episode_watches) over (partition by area,country_name,lang_name order by week_start) as 上周总观看集数,
    task_doers AS 做任务人数,
    lag(task_doers) over (partition by area,country_name,lang_name order by week_start) as 上周做任务人数,
    pre_pay_watch_uv_complete AS 付费前一集完播人数,
    lag(pre_pay_watch_uv_complete) over (partition by area,country_name,lang_name order by week_start) as 上周付费前一集完播人数,
    free_episodes_watch_uv AS 剧免费集播放人数,
    lag(free_episodes_watch_uv) over (partition by area,country_name,lang_name order by week_start) as 上周剧免费集播放人数,
    new_bound_users AS 新增绑定用户数,
    lag(new_bound_users) over (partition by area,country_name,lang_name order by week_start) as 上周新增绑定用户数,
    total_user as 累计新增用户数,
    lag(total_user) over (partition by area,country_name,lang_name order by week_start) as 上周累计新增用户数
FROM public.dw_core_indicators, LatestWeekStart
WHERE week_start <= LatestWeekStart.max_week_start - INTERVAL '7 day'
ORDER BY week_start DESC