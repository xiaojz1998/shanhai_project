set timezone ='UTC-0';
		truncate table analysis.dw_push_view_tmp01;
		insert into analysis.dw_push_view_tmp01
	-- SQL脚本
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
					--app_id ,
					 id::text as push_id
					,to_timestamp(pushed_at) at time zone 'UTC-8' as push_time  --,to_timestamp(pushed_at) at time zone 'UTC-8'
					,user_layered_configs
					-- json_array_element 函数是一个炸裂函数
					,json_array_elements(user_layered_configs::json ) ->> 'id' as layered_id
					,json_array_elements(user_layered_configs::json ) ->> 'name' as layered_name -- "推送人群（分层）"
					,title as push_title
					,"content" as push_content
					,jump_type
					,push_retry_times as push_hz
					,sent_count as sent_unt
					,delivered_count as push_unt
					,click_count as click_unt
					from public."oversea-api_osd_pushed" x
					where 1=1
					 -- and (id=3640  or id=3049 or id=3923)
					 -- and user_layered_configs like '%首页推荐-泰语%'
				)t1
				left join(
					select t1.*,t2."name" as lang_name
					from(
					select id ,"name" -- ,lang_config -- ,country_config
					,lang_config::json ->> 0 as lang_code
					from public."oversea-api_osd_user_layered_configs"
					where 1=1
					and lang_config <>'[]'
					-- and "name" like '%英语PUSH-全量用户%'
					)t1
					left join public."oversea-api_osd_lang" t2 on t1.lang_code=t2.lang_code
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
				-- created_date
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
			    	-- and to_timestamp(created_at)::date >= (current_date+interval'-1 day')::date -- 增
				group by
				-- created_date
				to_timestamp(created_at)::date
				,push_id
				,uid
			)t0
			group by
			 push_id
			,d_date
			-- order by d_date
		)
		,tmp_push_order as(
			select
			-- app_id,app_name ,
			 push_id
			,to_timestamp(created_at)::date as d_date
			,count(distinct o.order_num) as all_pay_order  -- 总订单数(包含失败)
			,count(distinct case when o.status = 1 then o.order_num else null end) as  pay_order  -- 成功充值订单数
			,count(distinct case when o.status = 1 then concat(o.order_num,o.created_at,o.order_type) else null end) as  pay_cnt  -- 成功充值次数
			,count(distinct case when o.status = 1 then o.uid else null end) as  pay_user  -- 成功充值人数
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
			    		-- and to_timestamp(created_at)::date >= (current_date+interval'-1 day')::date -- 增
				)t1
				left join(
					select order_num ,max(status ) as status
					-- select *
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
			-- app_id ,app_name ,
			 push_id
			,to_timestamp(created_at)::date
			-- order by to_timestamp(created_at)::date asc

		)
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

			truncate table public.dw_push_view  ;
			insert into public.dw_push_view  select * from analysis.dw_push_view_tmp01;






