---------------------------------------------
-- File: push_推送效果_全量_d.sql
-- Time: 2025/5/24 18:25
-- User: xiaoj
-- Description:  
---------------------------------------------
set timezone ='UTC-0';
---------------------------------------------
-- 全量更新
---------------------------------------------
truncate table analysis.dw_push_view_tmp01;
insert into analysis.dw_push_view_tmp01
-- 推送信息
with tmp_push_info as(
	select
	 push_id
	,push_time
	,string_agg(layered_name,';' order by layered_name) as layered_name
	,array_agg(distinct lang_name) as lang_name
	,push_title
	,push_content
	,jump_type
	,push_hz
	,sent_unt
	,push_unt
	,click_unt
	from(
		select t1.*,t2.lang_name
		from(
			select
			id::text as push_id
			,to_timestamp(pushed_at) at time zone 'UTC-8' as push_time
			,user_layered_configs                       -- user_layered_configs
			,json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'id' as layered_id
			,json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'name' as layered_name
			,title as push_title                        -- 推送标题
			,"content" as push_content                  -- 推送内容
			,jump_type                                  -- 类型
			,push_retry_times as push_hz                -- 推送次数
			,sent_count as sent_unt                     -- 命中用户数
			,delivered_count as push_unt                -- 送达人数
			,click_count as click_unt                   -- 点击人数
			from public."oversea-api_osd_pushed" x      -- push推送表
		)t1
		left join(
			select t1.*,t2."name" as lang_name      -- 用户分层配置表 id 名称 语言码 和 语言
			from(
			select id ,"name"
			,lang_config::json ->> 0 as lang_code
			from public."oversea-api_osd_user_layered_configs"  -- 用户分层配置表
			where 1=1
			and lang_config <>'[]'
			)t1
			left join public."oversea-api_osd_lang" t2 on t1.lang_code=t2.lang_code     -- 补充语言
		)t2 on t1.layered_id=t2.id::text
	)a
	group by
	push_id
	,push_time
	,push_title
	,push_content
	,jump_type
	,push_hz
	,sent_unt
	,push_unt
	,click_unt
)
,tmp_push_log as(
	select
	push_id
	,d_date
	,sum(popup_pv) as popup_pv
	,count(distinct case when popup_pv>0 then uid else null end) popup_uv
	,sum(watch_pv) as watch_pv
	,count(distinct case when watch_pv>0 then uid else null end) watch_uv
	,sum(watch_duration) as watch_duration
	,sum(watch_eid) as watch_eid
	from(
		select
		to_timestamp(created_at)::date as d_date
		,push_id
		,uid
		,count(case when event=58 then uid else null end) as popup_pv
		,count(case when event in(1,2,13,14) and vid>0 and eid>0 then uid else null end) as watch_pv
		,round(sum(case when event=2 and vid>0 and eid>0 then watch_time else 0 end)/60.0,2) as watch_duration
		,count(distinct case when event in(1,2,13,14) and vid>0 and eid>0 then eid else null end) as watch_eid
		from public.app_user_track_log a
		where 1=1
		and event in(58 ,1,2,13,14)
		and push_id<>''
			-- and (push_id='3640' or push_id='3049' or push_id='3923')
			-- 58 进入充值弹窗就上报
		and created_date >20241101
		group by
		to_timestamp(created_at)::date
		,push_id
		,uid
	)t0
	group by
		push_id
	,d_date
)
,tmp_push_order as(
	select
	push_id
	,to_timestamp(created_at)::date as d_date
	,count(distinct o.order_num) as all_pay_order
	,count(distinct case when o.status = 1 then o.order_num else null end) as  pay_order
	,count(distinct case when o.status = 1 then concat(o.order_num,o.created_at,o.order_type) else null end) as  pay_cnt
	,count(distinct case when o.status = 1 then o.uid else null end) as  pay_user
	,sum(case when o.status = 1 then o.money*0.01 else 0 end) as  pay_amt  -- 成功充值金额
	from(
		select t1.*
		,t2.status
		,t3.push_id
		from(
			select distinct order_num,order_type,created_at,created_date,uid ,money -- ,price
			from public.all_order_log o
			where 1=1 and o.environment = 1
			and to_timestamp(created_at)::date >'2024-11-01'
		)t1
		left join(
			select order_num ,max(status ) as status
			from public."oversea-api_osd_order" o
			where 1=1 and o.environment = 1
			and to_timestamp(created_at)::date >'2024-11-01'
			-- and order_num='SH120232395921248256'
			group by order_num
		)t2 on t1.order_num=t2.order_num
		left join(
			select distinct order_num,push_id
			from public.all_order_log o
			where 1=1 and o.environment = 1
			and push_id is not null and push_id<>''
			and to_timestamp(created_at)::date >'2024-11-01'
			-- and order_type >=4
		)t3 on t1.order_num=t3.order_num
		where 1=1
	)o
	group by
	push_id
	,to_timestamp(created_at)::date

)
-- 基础维度主表
,tmp_primary as(
	select distinct push_id,d_date from(
		select distinct push_id,d_date from tmp_push_log
		union all
		select distinct push_id,d_date from tmp_push_order
	)a
	where push_id is not null and d_date is not null
)

select
tp.push_id
,t0.push_time::timestamp as push_time
,t0.layered_name
,t0.lang_name
,t0.push_title
,t0.push_content
,t0.jump_type
,t0.push_hz
,t0.sent_unt
,t0.push_unt
,t0.click_unt
,coalesce(tp.d_date::text,'') as d_date
,t1.popup_pv
,t1.popup_uv
,t1.watch_pv
,t1.watch_uv
,t1.watch_duration
,t1.watch_eid
,t2.all_pay_order
,t2.pay_amt
,t2.pay_order
,t2.pay_user
,t2.pay_cnt
from tmp_primary tp
left join tmp_push_info t0 on tp.push_id=t0.push_id
left join tmp_push_log t1 on tp.push_id=t1.push_id and tp.d_date=t1.d_date
left join tmp_push_order t2 on tp.push_id=t2.push_id and tp.d_date=t2.d_date
where 1=1
and t0.push_id is not null
;

-- 全量更新
truncate table public.dw_push_view  ;
insert into public.dw_push_view  select * from analysis.dw_push_view_tmp01;








