---------------------------------------------
-- File: 测试.sql
-- Time: 2025/5/24 16:13
-- User: xiaoj
-- Description:  
---------------------------------------------
-- 测试运营月进度报表数据量  140w
select count(*) from dw_operate_schedule;

-- 运营月进度报表有 4个 比值字段，其实可以去掉
-- 观看行为统计
-- pay7 和 累计支付
-- 退款金额
-- 推送push行为

-- 验证tmp_watch
select
    sum( vid_watch_cnt),
    sum(eid_watch_cnt),
    sum(eidpay_watch_cnt),
    sum(eidfree_watch_cnt),
    sum(watch_duration)
from (select
    to_timestamp(a.created_at) :: date as d_date
    ,a.country_code
    ,a.uid
    ,count(distinct a.vid) as vid_watch_cnt -- 每人看短剧数
    ,count(distinct a.eid) as eid_watch_cnt -- 每人看剧集数
    ,count(distinct case when e.sort >= c.pay_num then a.eid else null end) as eidpay_watch_cnt     -- 付费集观看数量
    ,count(distinct case when e.sort <  c.pay_num then a.eid else null end) as eidfree_watch_cnt    -- 免费集观看数量
    ,sum(case when a.event=2 then watch_time else 0 end) as watch_duration -- "看剧时长(分钟)"
from public.app_user_track_log a
left join "oversea-api_osd_videos" c on a.vid = c.id
left join "oversea-api_osd_video_episodes" e on a.eid = e.id
where 1=1
    and a.event in (1,2,13,14)
    and a.vid>0 and a.eid>0
    -- and a.watch_time >3
    -- 全量更新
    -- and to_timestamp(a.created_at) :: date>='2024-11-01'
    -- 增量更新
    and to_timestamp(a.created_at) :: date = '2025-05-30'
group by to_timestamp(a.created_at) :: date
    ,a.country_code
    ,a.uid) t

-- 验证tmp_pay
with tmp_pay as (
    select
        d_date,
        country_code,
        lang,
        sum(case when d_date::date between (p_date::date+interval'- 7 d')::date and p_date::date  and (d_date::date+interval' 7 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_7,
        sum(case when p_date::date <= current_date - 1 then new_pay_amt else null end)::decimal(20,2) as pay_total
    from (
    select
        t1.d_date,
        t2.d_date as p_date,
        coalesce(t1.country_code,'UNKNOWN') as country_code,
        coalesce(t1.lang,'UNKNOWN') as lang,
        sum(t2.pay_amt)::decimal(20,2) as new_pay_amt
    from new_reg_users t1
    left join (
        -- 每日用户充值金额
        select
            d_date::date as d_date
             ,uid
            ,sum(pay_amt) as pay_amt -- 新用户充值金额（未减退款，与指标概览保持一致）
            from(
                -- 每日用户订单统计
                select
                to_char( to_timestamp(created_at),'YYYY-MM-DD')as d_date
                ,uid
                ,sum(case when o.status = 1 then o.money*0.01 else 0 end) as  pay_amt  -- 成功充值金额
                from public.all_order_log o                                 -- 用户订单表
                where o.environment = 1 and o.os in('android','ios')
                -- 全量更新
                and created_date>=20240701
                -- 增量更新
                -- and to_timestamp(created_at)::date  >=(current_date+interval '-2 day')::date
                group by
                to_char( to_timestamp(created_at),'YYYY-MM-DD')
                ,uid
            )a
            group by d_date,uid
    ) t2 on t1.uid = t2.uid and t1.d_date <= t2.d_date
    group by t1.d_date,t2.d_date, coalesce(t1.country_code,'UNKNOWN'), coalesce(t1.lang,'UNKNOWN')) t3
    group by d_date,country_code, lang
)










