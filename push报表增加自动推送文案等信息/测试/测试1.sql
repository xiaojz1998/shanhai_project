---------------------------------------------
-- File: 测试1.sql
-- Time: 2025/6/18 13:18
-- User: xiaoj
-- Description:  
---------------------------------------------
-- 测试 订单部分
select
    sum(money)
from all_order_log
where push_id = 'person_6321'
    and environment = 1
    and status = 1
    and to_timestamp(created_at)::date = '2025-06-17';

-- 自测

select
    count(distinct uid)
    , sum(money)
from all_order_log
where push_id = 'B27_4_en_AE'
    and environment = 1
    and status = 1
    and to_timestamp(created_at)::date = '2025-06-17';

-- 14422
select
    count(distinct uid)
    , sum(money)
from all_order_log
where push_id = '14422'
    and environment = 1
    and status = 1
    and to_timestamp(created_at)::date = '2025-05-19';



























-- 订单部分重写
select
    push_id
    , to_timestamp(created_at)::date as d_date                                                                          -- 数据日期
    , count(distinct t3.order_num) as all_pay_order                                                                     -- 下单总数（包含失败单）
    , count(distinct case when t3.status = 1 and t3.is_refund is null then t3.order_num else null end) as  pay_order    -- 成功下单数
    , count(distinct case when t3.status = 1 and t3.is_refund is null then concat(t3.order_num,t3.created_at,t3.order_type) else null end) as  pay_cnt -- 成功充值次数
    , count(distinct case when t3.status = 1 and t3.is_refund is null then t3.uid else null end) as  pay_user           -- 成功充值人数
    , sum(case when t3.status = 1 and t3.is_refund is null then t3.money*0.01 else 0 end) as  pay_amt                                          -- 成功充值金额
from(
	select
	    push_id
	    , t1.order_num
	    , order_type
	    , status                -- 订单状态
	    , created_at
	    , uid
	    , money                 -- 订单金额
        , t2.is_refund          -- 用于判断是否退款
	from(
		select
		    push_id
		    , order_num
		    , order_type
		    , status
		    , created_at
		    , uid
		    , money
		from public.all_order_log o
		where 1=1
		    and o.environment = 1                                               -- 生产环境
		    and push_id is not null and push_id != ''                           -- 过滤掉不合法push_id
		    and to_timestamp(created_at)::date >'2024-11-01'
		group by push_id,order_num,order_type,status,created_at,uid ,money
	)t1
	left join(
	    -- 判断当天的订单是否是退款单
	    select
	        order_num
	        , 1::bigint is_refund
	        , to_timestamp(created_at)::date as d_date
	    from public.all_refund_order_log
	    where 1 = 1
	        and environment = 1
	        and status = 1
	        and to_timestamp(created_at)::date >'2024-11-01'
	    group by order_num, to_timestamp(created_at)::date
	)t2 on t1.order_num = t2.order_num and to_timestamp(t1.created_at)::date = t2.d_date
) t3
group by
    push_id
    , to_timestamp(created_at)::date