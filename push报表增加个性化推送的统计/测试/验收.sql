---------------------------------------------
-- File: 验收.sql
-- Time: 2025/6/9 10:32
-- User: xiaoj
-- Description:  
---------------------------------------------
-- 个性化push 效果
select
    to_timestamp(t2.created_at) ::date as d_date
    , sum(sent_count) as sent_cnt
    , sum(delivered_count) as push_cnt
    , sum(click_count) as click_cnt
from public."oversea-api_osd_personalized_push_configs" t
    left join public."oversea-api_osd_videos" t0 on t.vid = t0.id
    left join public."oversea-api_osd_categories" t1 on t0.category_id = t1.id
    left join public."oversea-api_osd_personalize_push_statistic" t2 on t.id = t2.push_id
where vid = 2697
group by to_timestamp(t2.created_at) ::date



-- 观看指标验证
with tmp_push_log as(
	select
        push_id
        , d_date
        , sum(popup_pv) as popup_pv                                             -- 充值面板弹出次数
        , count(distinct case when popup_pv>0 then uid else null end) popup_uv  -- 充值面板弹出人数
        , sum(watch_pv) as watch_pv                                             -- 看剧次数
        , count(distinct case when watch_pv>0 then uid else null end) watch_uv  -- 看剧人数
        , sum(watch_duration) as watch_duration                                 -- 看剧时长(分)
        , sum(watch_eid) as watch_eid                                           -- 看剧集数
	from(
		select
            to_timestamp(created_at)::date as d_date    -- 数据日期
            , push_id                                   -- push_id
            , uid                                       -- 用户id
            , count(case when event=58 then uid else null end) as popup_pv
            , count(case when event in(1,2,13,14) and vid>0 and eid>0 then uid else null end) as watch_pv
            , round(sum(case when event=2 and vid>0 and eid>0 then watch_time else 0 end)/60.0,2) as watch_duration
            , count(distinct case when event in(1,2,13,14) and vid>0 and eid>0 then eid else null end) as watch_eid
		from public.app_user_track_log a
		where 1=1
            and event in (58,1,2,13,14)
            and push_id<>''
		    and created_date > 20241101
			-- and (push_id='3640' or push_id='3049' or push_id='3923')
			-- 58 进入充值弹窗就上报
		group by
            to_timestamp(created_at)::date
            , push_id
            , uid
	)t0
	group by
		push_id
        , d_date
)
select
    *
from tmp_push_log
where push_id = 'person_2697'

-- 付费指标验证
with tmp_push_order as(
	select
        push_id
        , to_timestamp(created_at)::date as d_date          -- 数据日期
        , count(distinct o.order_num) as all_pay_order      -- 下单总数（包含失败单）
        , count(distinct case when o.status = 1 then o.order_num else null end) as  pay_order   -- 成功下单数
        , count(distinct case when o.status = 1 then concat(o.order_num,o.created_at,o.order_type) else null end) as  pay_cnt -- 成功充值次数
        , count(distinct case when o.status = 1 then o.uid else null end) as  pay_user  -- 成功充值人数
        , sum(case when o.status = 1 then o.money*0.01 else 0 end) as  pay_amt  -- 成功充值金额
	from(
		select
		    t1.*
            , t2.status
            , t3.push_id
		from(
			select
			    distinct order_num,order_type,created_at,created_date,uid ,money
			from public.all_order_log o
			where 1=1
			    and o.environment = 1                                           -- 生产环境
			    and to_timestamp(created_at)::date >'2024-11-01'
		)t1
		left join(
			select
			    order_num
			    , max(status ) as status
			from public."oversea-api_osd_order" o                              -- 从订单表获取订单状态
			where 1=1
			    and o.environment = 1
			    and to_timestamp(created_at)::date >'2024-11-01'
			-- and order_num='SH120232395921248256'
			group by order_num
		)t2 on t1.order_num=t2.order_num
		left join(
			select
			    distinct order_num,push_id                                    -- 提取每个订单号的push_id
			from public.all_order_log o
			where 1=1
			    and o.environment = 1
			    and push_id is not null and push_id<>''
			    and to_timestamp(created_at)::date >'2024-11-01'
			-- and order_type >=4
		)t3 on t1.order_num=t3.order_num
	)o
	group by
	    push_id
	    , to_timestamp(created_at)::date
)
select
    push_id
    , to_timestamp(created_at)::date
    , order_num
    , status
from "oversea-api_osd_order"
where order_num = 'SH184689470206554112'