set timezone ='UTC-0';

		truncate  analysis.dw_operate_roi_v2_calc01 ;
		insert into  analysis.dw_operate_roi_v2_calc01
		-- 用于补充信息
			with new_reg_users as(
				select v_date as created_date
				,d_date as d_date
				,substr(d_date,1,7) as d_month
				,uid::int8 as uid
				,country_code ,ad_channel
				,lang
				,lang_name
				,is_campaign
				,count(uid) over(partition by d_date ,country_code,ad_channel ) as new_user_cnt_daily
				,count(uid) over(partition by substr(d_date,1,7) ,country_code,ad_channel ) as new_user_cnt_month
				from public.dwd_user_info
			)
			,tmp_pay as(
				select created_date,d_date,d_month,uid
				,sum(pay_orderall) as pay_orderall
				,sum(pay_order) as pay_order
				,sum(pay_amt) as pay_amt -- 新用户充值金额（未减退款，与指标概览保持一致）
				from(
					select created_date
					,to_char( to_timestamp(created_at),'YYYY-MM-DD') as d_date
					,to_char( to_timestamp(created_at),'YYYY-MM') as d_month
					,uid
					,count(distinct order_num) as pay_orderall
					,count(distinct case when o.status = 1 then o.order_num else null end) as  pay_order  -- 成功充值订单数
					,sum(case when o.status = 1 then o.money*0.01 else 0 end) as  pay_amt  -- 成功充值金额
					from public.all_order_log o
					where o.environment = 1 and o.os in('android','ios')
					and created_date>=20240701
						-- and to_char( to_timestamp(created_at),'YYYY-MM-DD') <= (current_date+interval'1 days')::date::text
					group by created_date
					,to_char( to_timestamp(created_at),'YYYY-MM-DD')
					,to_char( to_timestamp(created_at),'YYYY-MM')
					,uid

					-- union all
				    -- select r.refund_date
					-- ,to_char( to_timestamp(r.refund_time),'YYYY-MM-DD') as d_date
					-- ,to_char( to_timestamp(r.refund_time),'YYYY-MM') as d_month
					-- ,uid
					-- ,0 as pay_orderall ,0 as pay_order
				    -- ,-1*sum(r.total_money*0.01) as refund_amt
				    -- from public.all_refund_order_log r
				    -- where r.status = 1 and r.environment = 1 and r.os in('android','ios')
					-- and r.refund_date>=20240701
				    -- 	-- and refund_date between 20241001 and 20241024
				    -- group by r.refund_date
					-- ,to_char( to_timestamp(r.refund_time),'YYYY-MM-DD')
					-- ,to_char( to_timestamp(r.refund_time),'YYYY-MM')
					-- ,uid
				)a
				group by created_date,d_date,d_month,uid

			)
			,tmp_total_pay as(
				select o.created_date
				,to_char( to_timestamp(o.created_at),'YYYY-MM-DD') as d_date
				,to_char( to_timestamp(o.created_at),'YYYY-MM') as d_month
				,coalesce(u0.country_code,'UNKNOWN') as country_code
				,coalesce(u0.ad_channel,'UNKNOWN') as ad_channel
				,count(distinct o.order_num) as pay_orderall -- 生成订单数
				,count(distinct case when o.status = 1 then o.order_num else null end) as  pay_order  -- 成功充值订单数
				,sum(case when o.status = 1 then o.money*0.01 else 0 end) as  pay_amt  -- 成功充值金额
				from public.all_order_log o
				left join new_reg_users u0 on o.uid=u0.uid
				where o.environment = 1 and o.os in('android','ios')
				and o.created_date>=20240701
				group by o.created_date
				,to_char( to_timestamp(o.created_at),'YYYY-MM-DD')
				,to_char( to_timestamp(o.created_at),'YYYY-MM')
				,coalesce(u0.country_code,'UNKNOWN')
				,coalesce(u0.ad_channel,'UNKNOWN')

			)
			,tmp_total_refund as(
			    select r.refund_date
				,to_char( to_timestamp(r.refund_time),'YYYY-MM-DD') as d_date
				,to_char( to_timestamp(r.refund_time),'YYYY-MM') as d_month
				,coalesce(u0.country_code,'UNKNOWN') as country_code
				,coalesce(u0.ad_channel,'UNKNOWN') as ad_channel
			    ,sum(r.total_money*0.01) as refund_amt
			    from public.all_refund_order_log r
			    left join new_reg_users u0 on r.uid=u0.uid
			    where r.status = 1 and r.environment = 1 and r.os in('android','ios')
				and r.refund_date>=20240701
			    group by r.refund_date
				,to_char( to_timestamp(r.refund_time),'YYYY-MM-DD')
				,to_char( to_timestamp(r.refund_time),'YYYY-MM')
				,coalesce(u0.country_code,'UNKNOWN')
				,coalesce(u0.ad_channel,'UNKNOWN')

			)
			,tmp_total_cost as(
				select c.created_date
				,to_char( to_timestamp(c.created_at),'YYYY-MM-DD') as d_date
				,to_char( to_timestamp(c.created_at),'YYYY-MM') as d_month
				,upper(c.area) as country_code
				,upper(c.ad_channel) as ad_channel
				,sum(c.cost_amount*0.0001) as ad_cost
				-- from public.ad_cost_summary_data_log
				from public.ad_cost_data_log c
				where 1=1
				and c.account_id not in('3851320725139192','1248567319618926')
				and c.created_date>=20240701
				group by c.created_date
				,to_char( to_timestamp(c.created_at),'YYYY-MM-DD')
				,to_char( to_timestamp(c.created_at),'YYYY-MM')
				,upper(c.area)
				,upper(c.ad_channel)

			)
			,tmp_primary as(
				select distinct  d_date,country_code,ad_channel ,d_month
				from(
					select distinct d_date,country_code,ad_channel ,d_month  from new_reg_users union all
					select distinct d_date,country_code,ad_channel ,d_month  from tmp_total_pay union all
					select distinct d_date,country_code,ad_channel ,d_month  from tmp_total_cost
				)t1
				where d_date<=(current_date+interval'-1 d')::date::text
			)

				select t0.d_date,t0.country_code,t0.ad_channel
				,'day' as date_tag ,'' as d_mdate ,'' as p_mdate
				,t1.p_date
				,t1.new_user_cnt
				,t1.new_pay_orderall
				,t1.new_pay_order
				,t1.new_pay_amt
				,sum(t1.new_pay_orderall) over(partition by t0.d_date,t0.country_code,t0.ad_channel )  as new_pay_orderall_total
				,sum(t1.new_pay_order) over(partition by t0.d_date,t0.country_code,t0.ad_channel )  as new_pay_order_total
				,sum(t1.new_pay_amt) over(partition by t0.d_date,t0.country_code,t0.ad_channel )::decimal(20,2)  as new_pay_amt_total
				,row_number() over(partition by t0.d_date,t0.country_code,t0.ad_channel order by p_date) as rn
				,coalesce(tp.pay_amt,0)::decimal(20,2) as app_pay_amt
				,coalesce(tr.refund_amt,0)::decimal(20,2) as app_refund_amt
				,coalesce(tc.ad_cost,0)::decimal(20,2) as app_ad_cost
				from tmp_primary t0
				left join(
					select  t1.d_date
					,t2.d_date as p_date
					,t1.country_code
					,t1.ad_channel
					,t1.new_user_cnt_daily as new_user_cnt
					,sum(t2.pay_orderall) as new_pay_orderall
					,sum(t2.pay_order) as new_pay_order
					,sum(t2.pay_amt)::decimal(20,2) as new_pay_amt
					-- ,1+current_date-(t1.d_date::date) as max_roi
					from new_reg_users t1
					left join tmp_pay t2 on t1.uid=t2.uid  and t1.d_date<=t2.d_date
						and t2.d_date <= (current_date+interval'-1 days')::date::text
					-- where t2.d_date is not null
					group by t1.d_date
					,t2.d_date
					,t1.country_code
					,t1.ad_channel
					,t1.new_user_cnt_daily
				)t1 on t0.d_date=t1.d_date and t0.country_code=t1.country_code and t0.ad_channel=t1.ad_channel
				left join tmp_total_pay tp on t0.d_date=tp.d_date and t0.country_code=tp.country_code and t0.ad_channel=tp.ad_channel
				left join tmp_total_refund tr on t0.d_date=tr.d_date and t0.country_code=tr.country_code and t0.ad_channel=tr.ad_channel
				left join tmp_total_cost tc on t0.d_date=tc.d_date and t0.country_code=tc.country_code and t0.ad_channel=tc.ad_channel


				union all
				select t0.d_date,t0.country_code,t0.ad_channel
				,'month' as date_tag ,concat(t0.d_date,'-01') as d_mdate ,(case when t1.p_date is null then null else concat(t1.p_date,'-01') end)as p_mdate
				,t1.p_date
				,t1.new_user_cnt
				,t1.new_pay_orderall
				,t1.new_pay_order
				,t1.new_pay_amt
				,sum(t1.new_pay_orderall) over(partition by t0.d_date,t0.country_code,t0.ad_channel )  as new_pay_orderall_total
				,sum(t1.new_pay_order) over(partition by t0.d_date,t0.country_code,t0.ad_channel )  as new_pay_order_total
				,sum(t1.new_pay_amt) over(partition by t0.d_date,t0.country_code,t0.ad_channel )::decimal(20,2)  as new_pay_amt_total
				,row_number() over(partition by t0.d_date,t0.country_code,t0.ad_channel order by p_date) as rn
				,coalesce(tp.pay_amt,0)::decimal(20,2) as app_pay_amt
				,coalesce(tr.refund_amt,0)::decimal(20,2) as app_refund_amt
				,coalesce(tc.ad_cost,0)::decimal(20,2) as app_ad_cost
				from(
					select distinct d_month as d_date,country_code,ad_channel  from tmp_primary
				)t0
				left join(
					select  t1.d_month as d_date
					,t2.d_month as p_date
					,t1.country_code
					,t1.ad_channel
					,t1.new_user_cnt_month as new_user_cnt
					,sum(t2.pay_orderall) as new_pay_orderall
					,sum(t2.pay_order) as new_pay_order
					,sum(t2.pay_amt)::decimal(20,2) as new_pay_amt
					-- ,1+EXTRACT(month from age(current_date, concat(t1.d_month,'-01')::date)) as max_roi
					from new_reg_users t1 -- log表可能重复记录新用户
					left join tmp_pay t2 on t1.uid=t2.uid and t1.d_month<=t2.d_month
						and t2.d_date <= (current_date+interval'-1 days')::date::text
					-- where t2.d_date is not null
					group by t1.d_month
					,t2.d_month
					,t1.country_code ,t1.ad_channel
					,t1.new_user_cnt_month
				)t1  on t0.d_date=t1.d_date and t0.country_code=t1.country_code and t0.ad_channel=t1.ad_channel
				left join(
					select d_month as d_date,country_code,ad_channel
					,sum(pay_orderall) as pay_orderall,sum(pay_order) as pay_order,sum(pay_amt) as pay_amt
					from tmp_total_pay
					group by d_month,country_code,ad_channel
				)tp on t0.d_date=tp.d_date and t0.country_code=tp.country_code and t0.ad_channel=tp.ad_channel
				left join(
					select d_month as d_date,country_code,ad_channel
					,sum(refund_amt) as refund_amt
					from tmp_total_refund
					group by d_month,country_code,ad_channel
				)tr on t0.d_date=tr.d_date and t0.country_code=tr.country_code and t0.ad_channel=tr.ad_channel
				left join(
					select d_month as d_date,country_code,ad_channel
					,sum(ad_cost) as ad_cost
					from tmp_total_cost
					group by d_month,country_code,ad_channel
				)tc on t0.d_date=tc.d_date and t0.country_code=tc.country_code and t0.ad_channel=tc.ad_channel
		;















		truncate table analysis.dw_operate_roi_v2_tmp01;
		insert into analysis.dw_operate_roi_v2_tmp01
			select
			 t1.date_tag
			,t1.d_date
			,t1.d_mdate
			,t1.country_code
			,t1.ad_channel
			,t1.new_user_cnt
			,cc.country_name
			,cc.area
			--
			,t1.app_ad_cost::decimal(20,2) as app_ad_cost
			,t1.app_pay_amt::decimal(20,2) as app_pay_amt
			,t1.app_refund_amt::decimal(20,2) as app_refund_amt
			,(t1.app_pay_amt-t1.app_refund_amt)::decimal(20,2) as app_pfpay_amt
			--
			,t1.new_pay_orderall_total as new_pay_orderall_total
			,t1.new_pay_order_total as new_pay_order_total
			,t1.new_pay_amt_total::decimal(20,2) as new_pay_amt_total
			--
			,sum(case when t1.d_date=t1.p_date then new_pay_orderall else 0 end) as new_pay_orderall
			,sum(case when t1.d_date=t1.p_date then new_pay_order else 0 end) as new_pay_order
			,sum(case when t1.d_date=t1.p_date then new_pay_amt else 0 end)::decimal(20,2) as new_pay_amt
			--
			,count(p_date) as date_cnt
			    ,sum(case when d_date::date between (p_date::date+interval'- 0 d')::date and p_date::date  and (d_date::date+interval' 0 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_0
			    ,sum(case when d_date::date between (p_date::date+interval'- 1 d')::date and p_date::date  and (d_date::date+interval' 1 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_1
			    ,sum(case when d_date::date between (p_date::date+interval'- 2 d')::date and p_date::date  and (d_date::date+interval' 2 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_2
			    ,sum(case when d_date::date between (p_date::date+interval'- 3 d')::date and p_date::date  and (d_date::date+interval' 3 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_3
			    ,sum(case when d_date::date between (p_date::date+interval'- 4 d')::date and p_date::date  and (d_date::date+interval' 4 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_4
			    ,sum(case when d_date::date between (p_date::date+interval'- 5 d')::date and p_date::date  and (d_date::date+interval' 5 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_5
			    ,sum(case when d_date::date between (p_date::date+interval'- 6 d')::date and p_date::date  and (d_date::date+interval' 6 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_6
			    ,sum(case when d_date::date between (p_date::date+interval'- 7 d')::date and p_date::date  and (d_date::date+interval' 7 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_7
			    ,sum(case when d_date::date between (p_date::date+interval'- 8 d')::date and p_date::date  and (d_date::date+interval' 8 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_8
			    ,sum(case when d_date::date between (p_date::date+interval'- 9 d')::date and p_date::date  and (d_date::date+interval' 9 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_9
			    ,sum(case when d_date::date between (p_date::date+interval'-10 d')::date and p_date::date  and (d_date::date+interval'10 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_10
			    ,sum(case when d_date::date between (p_date::date+interval'-11 d')::date and p_date::date  and (d_date::date+interval'11 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_11
			    ,sum(case when d_date::date between (p_date::date+interval'-12 d')::date and p_date::date  and (d_date::date+interval'12 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_12
			    ,sum(case when d_date::date between (p_date::date+interval'-13 d')::date and p_date::date  and (d_date::date+interval'13 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_13
			    ,sum(case when d_date::date between (p_date::date+interval'-14 d')::date and p_date::date  and (d_date::date+interval'14 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_14
			    ,sum(case when d_date::date between (p_date::date+interval'-15 d')::date and p_date::date  and (d_date::date+interval'15 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_15
			    ,sum(case when d_date::date between (p_date::date+interval'-16 d')::date and p_date::date  and (d_date::date+interval'16 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_16
			    ,sum(case when d_date::date between (p_date::date+interval'-17 d')::date and p_date::date  and (d_date::date+interval'17 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_17
			    ,sum(case when d_date::date between (p_date::date+interval'-18 d')::date and p_date::date  and (d_date::date+interval'18 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_18
			    ,sum(case when d_date::date between (p_date::date+interval'-19 d')::date and p_date::date  and (d_date::date+interval'19 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_19
			    ,sum(case when d_date::date between (p_date::date+interval'-20 d')::date and p_date::date  and (d_date::date+interval'20 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_20
			    ,sum(case when d_date::date between (p_date::date+interval'-21 d')::date and p_date::date  and (d_date::date+interval'21 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_21
			    ,sum(case when d_date::date between (p_date::date+interval'-22 d')::date and p_date::date  and (d_date::date+interval'22 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_22
			    ,sum(case when d_date::date between (p_date::date+interval'-23 d')::date and p_date::date  and (d_date::date+interval'23 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_23
			    ,sum(case when d_date::date between (p_date::date+interval'-24 d')::date and p_date::date  and (d_date::date+interval'24 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_24
			    ,sum(case when d_date::date between (p_date::date+interval'-25 d')::date and p_date::date  and (d_date::date+interval'25 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_25
			    ,sum(case when d_date::date between (p_date::date+interval'-26 d')::date and p_date::date  and (d_date::date+interval'26 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_26
			    ,sum(case when d_date::date between (p_date::date+interval'-27 d')::date and p_date::date  and (d_date::date+interval'27 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_27
			    ,sum(case when d_date::date between (p_date::date+interval'-28 d')::date and p_date::date  and (d_date::date+interval'28 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_28
			    ,sum(case when d_date::date between (p_date::date+interval'-29 d')::date and p_date::date  and (d_date::date+interval'29 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_29
			    ,sum(case when d_date::date between (p_date::date+interval'-30 d')::date and p_date::date  and (d_date::date+interval'30 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_30
			    ,sum(case when d_date::date between (p_date::date+interval'-31 d')::date and p_date::date  and (d_date::date+interval'31 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_31
			    ,sum(case when d_date::date between (p_date::date+interval'-32 d')::date and p_date::date  and (d_date::date+interval'32 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_32
			    ,sum(case when d_date::date between (p_date::date+interval'-33 d')::date and p_date::date  and (d_date::date+interval'33 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_33
			    ,sum(case when d_date::date between (p_date::date+interval'-34 d')::date and p_date::date  and (d_date::date+interval'34 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_34
			    ,sum(case when d_date::date between (p_date::date+interval'-35 d')::date and p_date::date  and (d_date::date+interval'35 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_35
			    ,sum(case when d_date::date between (p_date::date+interval'-36 d')::date and p_date::date  and (d_date::date+interval'36 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_36
			    ,sum(case when d_date::date between (p_date::date+interval'-37 d')::date and p_date::date  and (d_date::date+interval'37 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_37
			    ,sum(case when d_date::date between (p_date::date+interval'-38 d')::date and p_date::date  and (d_date::date+interval'38 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_38
			    ,sum(case when d_date::date between (p_date::date+interval'-39 d')::date and p_date::date  and (d_date::date+interval'39 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_39
			    ,sum(case when d_date::date between (p_date::date+interval'-40 d')::date and p_date::date  and (d_date::date+interval'40 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_40
			    ,sum(case when d_date::date between (p_date::date+interval'-41 d')::date and p_date::date  and (d_date::date+interval'41 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_41
			    ,sum(case when d_date::date between (p_date::date+interval'-42 d')::date and p_date::date  and (d_date::date+interval'42 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_42
			    ,sum(case when d_date::date between (p_date::date+interval'-43 d')::date and p_date::date  and (d_date::date+interval'43 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_43
			    ,sum(case when d_date::date between (p_date::date+interval'-44 d')::date and p_date::date  and (d_date::date+interval'44 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_44
			    ,sum(case when d_date::date between (p_date::date+interval'-45 d')::date and p_date::date  and (d_date::date+interval'45 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_45
			    ,sum(case when d_date::date between (p_date::date+interval'-46 d')::date and p_date::date  and (d_date::date+interval'46 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_46
			    ,sum(case when d_date::date between (p_date::date+interval'-47 d')::date and p_date::date  and (d_date::date+interval'47 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_47
			    ,sum(case when d_date::date between (p_date::date+interval'-48 d')::date and p_date::date  and (d_date::date+interval'48 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_48
			    ,sum(case when d_date::date between (p_date::date+interval'-49 d')::date and p_date::date  and (d_date::date+interval'49 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_49
			    ,sum(case when d_date::date between (p_date::date+interval'-50 d')::date and p_date::date  and (d_date::date+interval'50 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_50
			    ,sum(case when d_date::date between (p_date::date+interval'-51 d')::date and p_date::date  and (d_date::date+interval'51 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_51
			    ,sum(case when d_date::date between (p_date::date+interval'-52 d')::date and p_date::date  and (d_date::date+interval'52 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_52
			    ,sum(case when d_date::date between (p_date::date+interval'-53 d')::date and p_date::date  and (d_date::date+interval'53 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_53
			    ,sum(case when d_date::date between (p_date::date+interval'-54 d')::date and p_date::date  and (d_date::date+interval'54 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_54
			    ,sum(case when d_date::date between (p_date::date+interval'-55 d')::date and p_date::date  and (d_date::date+interval'55 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_55
			    ,sum(case when d_date::date between (p_date::date+interval'-56 d')::date and p_date::date  and (d_date::date+interval'56 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_56
			    ,sum(case when d_date::date between (p_date::date+interval'-57 d')::date and p_date::date  and (d_date::date+interval'57 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_57
			    ,sum(case when d_date::date between (p_date::date+interval'-58 d')::date and p_date::date  and (d_date::date+interval'58 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_58
			    ,sum(case when d_date::date between (p_date::date+interval'-59 d')::date and p_date::date  and (d_date::date+interval'59 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_59
			    ,sum(case when d_date::date between (p_date::date+interval'-60 d')::date and p_date::date  and (d_date::date+interval'60 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_60
			    ,sum(case when d_date::date between (p_date::date+interval'-61 d')::date and p_date::date  and (d_date::date+interval'61 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_61
			    ,sum(case when d_date::date between (p_date::date+interval'-62 d')::date and p_date::date  and (d_date::date+interval'62 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_62
			    ,sum(case when d_date::date between (p_date::date+interval'-63 d')::date and p_date::date  and (d_date::date+interval'63 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_63
			    ,sum(case when d_date::date between (p_date::date+interval'-64 d')::date and p_date::date  and (d_date::date+interval'64 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_64
			    ,sum(case when d_date::date between (p_date::date+interval'-65 d')::date and p_date::date  and (d_date::date+interval'65 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_65
			    ,sum(case when d_date::date between (p_date::date+interval'-66 d')::date and p_date::date  and (d_date::date+interval'66 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_66
			    ,sum(case when d_date::date between (p_date::date+interval'-67 d')::date and p_date::date  and (d_date::date+interval'67 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_67
			    ,sum(case when d_date::date between (p_date::date+interval'-68 d')::date and p_date::date  and (d_date::date+interval'68 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_68
			    ,sum(case when d_date::date between (p_date::date+interval'-69 d')::date and p_date::date  and (d_date::date+interval'69 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_69
			    ,sum(case when d_date::date between (p_date::date+interval'-70 d')::date and p_date::date  and (d_date::date+interval'70 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_70
			    ,sum(case when d_date::date between (p_date::date+interval'-71 d')::date and p_date::date  and (d_date::date+interval'71 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_71
			    ,sum(case when d_date::date between (p_date::date+interval'-72 d')::date and p_date::date  and (d_date::date+interval'72 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_72
			    ,sum(case when d_date::date between (p_date::date+interval'-73 d')::date and p_date::date  and (d_date::date+interval'73 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_73
			    ,sum(case when d_date::date between (p_date::date+interval'-74 d')::date and p_date::date  and (d_date::date+interval'74 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_74
			    ,sum(case when d_date::date between (p_date::date+interval'-75 d')::date and p_date::date  and (d_date::date+interval'75 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_75
			    ,sum(case when d_date::date between (p_date::date+interval'-76 d')::date and p_date::date  and (d_date::date+interval'76 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_76
			    ,sum(case when d_date::date between (p_date::date+interval'-77 d')::date and p_date::date  and (d_date::date+interval'77 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_77
			    ,sum(case when d_date::date between (p_date::date+interval'-78 d')::date and p_date::date  and (d_date::date+interval'78 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_78
			    ,sum(case when d_date::date between (p_date::date+interval'-79 d')::date and p_date::date  and (d_date::date+interval'79 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_79
			    ,sum(case when d_date::date between (p_date::date+interval'-80 d')::date and p_date::date  and (d_date::date+interval'80 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_80
			    ,sum(case when d_date::date between (p_date::date+interval'-81 d')::date and p_date::date  and (d_date::date+interval'81 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_81
			    ,sum(case when d_date::date between (p_date::date+interval'-82 d')::date and p_date::date  and (d_date::date+interval'82 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_82
			    ,sum(case when d_date::date between (p_date::date+interval'-83 d')::date and p_date::date  and (d_date::date+interval'83 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_83
			    ,sum(case when d_date::date between (p_date::date+interval'-84 d')::date and p_date::date  and (d_date::date+interval'84 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_84
			    ,sum(case when d_date::date between (p_date::date+interval'-85 d')::date and p_date::date  and (d_date::date+interval'85 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_85
			    ,sum(case when d_date::date between (p_date::date+interval'-86 d')::date and p_date::date  and (d_date::date+interval'86 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_86
			    ,sum(case when d_date::date between (p_date::date+interval'-87 d')::date and p_date::date  and (d_date::date+interval'87 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_87
			    ,sum(case when d_date::date between (p_date::date+interval'-88 d')::date and p_date::date  and (d_date::date+interval'88 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_88
			    ,sum(case when d_date::date between (p_date::date+interval'-89 d')::date and p_date::date  and (d_date::date+interval'89 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_89
			    ,sum(case when d_date::date between (p_date::date+interval'-90 d')::date and p_date::date  and (d_date::date+interval'90 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_90
			    ,sum(case when d_date::date between (p_date::date+interval'-91 d')::date and p_date::date  and (d_date::date+interval'91 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_91
			    ,sum(case when d_date::date between (p_date::date+interval'-92 d')::date and p_date::date  and (d_date::date+interval'92 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_92
			    ,sum(case when d_date::date between (p_date::date+interval'-93 d')::date and p_date::date  and (d_date::date+interval'93 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_93
			    ,sum(case when d_date::date between (p_date::date+interval'-94 d')::date and p_date::date  and (d_date::date+interval'94 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_94
			    ,sum(case when d_date::date between (p_date::date+interval'-95 d')::date and p_date::date  and (d_date::date+interval'95 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_95
			    ,sum(case when d_date::date between (p_date::date+interval'-96 d')::date and p_date::date  and (d_date::date+interval'96 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_96
			    ,sum(case when d_date::date between (p_date::date+interval'-97 d')::date and p_date::date  and (d_date::date+interval'97 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_97
			    ,sum(case when d_date::date between (p_date::date+interval'-98 d')::date and p_date::date  and (d_date::date+interval'98 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_98
			    ,sum(case when d_date::date between (p_date::date+interval'-99 d')::date and p_date::date  and (d_date::date+interval'99 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_99
			    ,sum(case when d_date::date between (p_date::date+interval'-100 d')::date and p_date::date  and (d_date::date+interval'100 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_100
				,sum(case when d_date::date between (p_date::date+interval'-101 d')::date and p_date::date  and (d_date::date+interval'101 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_101
				,sum(case when d_date::date between (p_date::date+interval'-102 d')::date and p_date::date  and (d_date::date+interval'102 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_102
				,sum(case when d_date::date between (p_date::date+interval'-103 d')::date and p_date::date  and (d_date::date+interval'103 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_103
				,sum(case when d_date::date between (p_date::date+interval'-104 d')::date and p_date::date  and (d_date::date+interval'104 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_104
				,sum(case when d_date::date between (p_date::date+interval'-105 d')::date and p_date::date  and (d_date::date+interval'105 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_105
				,sum(case when d_date::date between (p_date::date+interval'-106 d')::date and p_date::date  and (d_date::date+interval'106 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_106
				,sum(case when d_date::date between (p_date::date+interval'-107 d')::date and p_date::date  and (d_date::date+interval'107 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_107
				,sum(case when d_date::date between (p_date::date+interval'-108 d')::date and p_date::date  and (d_date::date+interval'108 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_108
				,sum(case when d_date::date between (p_date::date+interval'-109 d')::date and p_date::date  and (d_date::date+interval'109 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_109
				,sum(case when d_date::date between (p_date::date+interval'-110 d')::date and p_date::date  and (d_date::date+interval'110 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_110
				,sum(case when d_date::date between (p_date::date+interval'-111 d')::date and p_date::date  and (d_date::date+interval'111 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_111
				,sum(case when d_date::date between (p_date::date+interval'-112 d')::date and p_date::date  and (d_date::date+interval'112 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_112
				,sum(case when d_date::date between (p_date::date+interval'-113 d')::date and p_date::date  and (d_date::date+interval'113 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_113
				,sum(case when d_date::date between (p_date::date+interval'-114 d')::date and p_date::date  and (d_date::date+interval'114 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_114
				,sum(case when d_date::date between (p_date::date+interval'-115 d')::date and p_date::date  and (d_date::date+interval'115 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_115
				,sum(case when d_date::date between (p_date::date+interval'-116 d')::date and p_date::date  and (d_date::date+interval'116 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_116
				,sum(case when d_date::date between (p_date::date+interval'-117 d')::date and p_date::date  and (d_date::date+interval'117 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_117
				,sum(case when d_date::date between (p_date::date+interval'-118 d')::date and p_date::date  and (d_date::date+interval'118 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_118
				,sum(case when d_date::date between (p_date::date+interval'-119 d')::date and p_date::date  and (d_date::date+interval'119 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_119
				,sum(case when d_date::date between (p_date::date+interval'-120 d')::date and p_date::date  and (d_date::date+interval'120 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_120
				,sum(case when d_date::date between (p_date::date+interval'-121 d')::date and p_date::date  and (d_date::date+interval'121 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_121
				,sum(case when d_date::date between (p_date::date+interval'-122 d')::date and p_date::date  and (d_date::date+interval'122 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_122
				,sum(case when d_date::date between (p_date::date+interval'-123 d')::date and p_date::date  and (d_date::date+interval'123 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_123
				,sum(case when d_date::date between (p_date::date+interval'-124 d')::date and p_date::date  and (d_date::date+interval'124 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_124
				,sum(case when d_date::date between (p_date::date+interval'-125 d')::date and p_date::date  and (d_date::date+interval'125 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_125
				,sum(case when d_date::date between (p_date::date+interval'-126 d')::date and p_date::date  and (d_date::date+interval'126 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_126
				,sum(case when d_date::date between (p_date::date+interval'-127 d')::date and p_date::date  and (d_date::date+interval'127 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_127
				,sum(case when d_date::date between (p_date::date+interval'-128 d')::date and p_date::date  and (d_date::date+interval'128 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_128
				,sum(case when d_date::date between (p_date::date+interval'-129 d')::date and p_date::date  and (d_date::date+interval'129 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_129
				,sum(case when d_date::date between (p_date::date+interval'-130 d')::date and p_date::date  and (d_date::date+interval'130 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_130
				,sum(case when d_date::date between (p_date::date+interval'-131 d')::date and p_date::date  and (d_date::date+interval'131 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_131
				,sum(case when d_date::date between (p_date::date+interval'-132 d')::date and p_date::date  and (d_date::date+interval'132 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_132
				,sum(case when d_date::date between (p_date::date+interval'-133 d')::date and p_date::date  and (d_date::date+interval'133 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_133
				,sum(case when d_date::date between (p_date::date+interval'-134 d')::date and p_date::date  and (d_date::date+interval'134 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_134
				,sum(case when d_date::date between (p_date::date+interval'-135 d')::date and p_date::date  and (d_date::date+interval'135 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_135
				,sum(case when d_date::date between (p_date::date+interval'-136 d')::date and p_date::date  and (d_date::date+interval'136 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_136
				,sum(case when d_date::date between (p_date::date+interval'-137 d')::date and p_date::date  and (d_date::date+interval'137 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_137
				,sum(case when d_date::date between (p_date::date+interval'-138 d')::date and p_date::date  and (d_date::date+interval'138 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_138
				,sum(case when d_date::date between (p_date::date+interval'-139 d')::date and p_date::date  and (d_date::date+interval'139 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_139
				,sum(case when d_date::date between (p_date::date+interval'-140 d')::date and p_date::date  and (d_date::date+interval'140 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_140
				,sum(case when d_date::date between (p_date::date+interval'-141 d')::date and p_date::date  and (d_date::date+interval'141 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_141
				,sum(case when d_date::date between (p_date::date+interval'-142 d')::date and p_date::date  and (d_date::date+interval'142 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_142
				,sum(case when d_date::date between (p_date::date+interval'-143 d')::date and p_date::date  and (d_date::date+interval'143 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_143
				,sum(case when d_date::date between (p_date::date+interval'-144 d')::date and p_date::date  and (d_date::date+interval'144 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_144
				,sum(case when d_date::date between (p_date::date+interval'-145 d')::date and p_date::date  and (d_date::date+interval'145 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_145
				,sum(case when d_date::date between (p_date::date+interval'-146 d')::date and p_date::date  and (d_date::date+interval'146 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_146
				,sum(case when d_date::date between (p_date::date+interval'-147 d')::date and p_date::date  and (d_date::date+interval'147 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_147
				,sum(case when d_date::date between (p_date::date+interval'-148 d')::date and p_date::date  and (d_date::date+interval'148 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_148
				,sum(case when d_date::date between (p_date::date+interval'-149 d')::date and p_date::date  and (d_date::date+interval'149 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_149
				,sum(case when d_date::date between (p_date::date+interval'-150 d')::date and p_date::date  and (d_date::date+interval'150 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_150
				,sum(case when d_date::date between (p_date::date+interval'-151 d')::date and p_date::date  and (d_date::date+interval'151 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_151
				,sum(case when d_date::date between (p_date::date+interval'-152 d')::date and p_date::date  and (d_date::date+interval'152 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_152
				,sum(case when d_date::date between (p_date::date+interval'-153 d')::date and p_date::date  and (d_date::date+interval'153 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_153
				,sum(case when d_date::date between (p_date::date+interval'-154 d')::date and p_date::date  and (d_date::date+interval'154 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_154
				,sum(case when d_date::date between (p_date::date+interval'-155 d')::date and p_date::date  and (d_date::date+interval'155 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_155
				,sum(case when d_date::date between (p_date::date+interval'-156 d')::date and p_date::date  and (d_date::date+interval'156 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_156
				,sum(case when d_date::date between (p_date::date+interval'-157 d')::date and p_date::date  and (d_date::date+interval'157 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_157
				,sum(case when d_date::date between (p_date::date+interval'-158 d')::date and p_date::date  and (d_date::date+interval'158 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_158
				,sum(case when d_date::date between (p_date::date+interval'-159 d')::date and p_date::date  and (d_date::date+interval'159 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_159
				,sum(case when d_date::date between (p_date::date+interval'-160 d')::date and p_date::date  and (d_date::date+interval'160 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_160
				,sum(case when d_date::date between (p_date::date+interval'-161 d')::date and p_date::date  and (d_date::date+interval'161 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_161
				,sum(case when d_date::date between (p_date::date+interval'-162 d')::date and p_date::date  and (d_date::date+interval'162 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_162
				,sum(case when d_date::date between (p_date::date+interval'-163 d')::date and p_date::date  and (d_date::date+interval'163 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_163
				,sum(case when d_date::date between (p_date::date+interval'-164 d')::date and p_date::date  and (d_date::date+interval'164 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_164
				,sum(case when d_date::date between (p_date::date+interval'-165 d')::date and p_date::date  and (d_date::date+interval'165 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_165
				,sum(case when d_date::date between (p_date::date+interval'-166 d')::date and p_date::date  and (d_date::date+interval'166 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_166
				,sum(case when d_date::date between (p_date::date+interval'-167 d')::date and p_date::date  and (d_date::date+interval'167 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_167
				,sum(case when d_date::date between (p_date::date+interval'-168 d')::date and p_date::date  and (d_date::date+interval'168 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_168
				,sum(case when d_date::date between (p_date::date+interval'-169 d')::date and p_date::date  and (d_date::date+interval'169 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_169
				,sum(case when d_date::date between (p_date::date+interval'-170 d')::date and p_date::date  and (d_date::date+interval'170 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_170
				,sum(case when d_date::date between (p_date::date+interval'-171 d')::date and p_date::date  and (d_date::date+interval'171 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_171
				,sum(case when d_date::date between (p_date::date+interval'-172 d')::date and p_date::date  and (d_date::date+interval'172 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_172
				,sum(case when d_date::date between (p_date::date+interval'-173 d')::date and p_date::date  and (d_date::date+interval'173 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_173
				,sum(case when d_date::date between (p_date::date+interval'-174 d')::date and p_date::date  and (d_date::date+interval'174 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_174
				,sum(case when d_date::date between (p_date::date+interval'-175 d')::date and p_date::date  and (d_date::date+interval'175 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_175
				,sum(case when d_date::date between (p_date::date+interval'-176 d')::date and p_date::date  and (d_date::date+interval'176 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_176
				,sum(case when d_date::date between (p_date::date+interval'-177 d')::date and p_date::date  and (d_date::date+interval'177 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_177
				,sum(case when d_date::date between (p_date::date+interval'-178 d')::date and p_date::date  and (d_date::date+interval'178 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_178
				,sum(case when d_date::date between (p_date::date+interval'-179 d')::date and p_date::date  and (d_date::date+interval'179 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_179
				,sum(case when d_date::date between (p_date::date+interval'-180 d')::date and p_date::date  and (d_date::date+interval'180 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_180
				,sum(case when d_date::date between (p_date::date+interval'-185 d')::date and p_date::date  and (d_date::date+interval'185 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_185
				,sum(case when d_date::date between (p_date::date+interval'-195 d')::date and p_date::date  and (d_date::date+interval'195 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_195
				,sum(case when d_date::date between (p_date::date+interval'-205 d')::date and p_date::date  and (d_date::date+interval'205 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_205
				,sum(case when d_date::date between (p_date::date+interval'-215 d')::date and p_date::date  and (d_date::date+interval'215 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_215
				,sum(case when d_date::date between (p_date::date+interval'-225 d')::date and p_date::date  and (d_date::date+interval'225 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_225
				,sum(case when d_date::date between (p_date::date+interval'-235 d')::date and p_date::date  and (d_date::date+interval'235 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_235
				,sum(case when d_date::date between (p_date::date+interval'-245 d')::date and p_date::date  and (d_date::date+interval'245 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_245
				,sum(case when d_date::date between (p_date::date+interval'-255 d')::date and p_date::date  and (d_date::date+interval'255 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_255
				,sum(case when d_date::date between (p_date::date+interval'-265 d')::date and p_date::date  and (d_date::date+interval'265 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_265
				,sum(case when d_date::date between (p_date::date+interval'-275 d')::date and p_date::date  and (d_date::date+interval'275 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_275
				,sum(case when d_date::date between (p_date::date+interval'-285 d')::date and p_date::date  and (d_date::date+interval'285 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_285
				,sum(case when d_date::date between (p_date::date+interval'-295 d')::date and p_date::date  and (d_date::date+interval'295 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_295
				,sum(case when d_date::date between (p_date::date+interval'-305 d')::date and p_date::date  and (d_date::date+interval'305 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_305
				,sum(case when d_date::date between (p_date::date+interval'-315 d')::date and p_date::date  and (d_date::date+interval'315 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_315
				,sum(case when d_date::date between (p_date::date+interval'-325 d')::date and p_date::date  and (d_date::date+interval'325 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_325
				,sum(case when d_date::date between (p_date::date+interval'-335 d')::date and p_date::date  and (d_date::date+interval'335 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_335
				,sum(case when d_date::date between (p_date::date+interval'-345 d')::date and p_date::date  and (d_date::date+interval'345 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_345
				,sum(case when d_date::date between (p_date::date+interval'-355 d')::date and p_date::date  and (d_date::date+interval'355 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_355
				,sum(case when d_date::date between (p_date::date+interval'-365 d')::date and p_date::date  and (d_date::date+interval'365 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_365
			from analysis.dw_operate_roi_v2_calc01 t1
			left join v_dim_country_area cc on t1.country_code=cc.country_code
			where date_tag='day'
			and t1.d_date<=(current_date+interval'1 days')::date::text
			group by
			 t1.date_tag
			,t1.d_date
			,t1.d_mdate
			,t1.country_code
			,t1.ad_channel
			,t1.new_user_cnt
			,cc.country_name
			,cc.area
			,t1.app_ad_cost
			,t1.app_pay_amt
			,t1.app_refund_amt
			,t1.new_pay_orderall_total
			,t1.new_pay_order_total
			,t1.new_pay_amt_total
		; -- 日

		insert into analysis.dw_operate_roi_v2_tmp01
			select
			 t1.date_tag
			,t1.d_date
			,t1.d_mdate
			,t1.country_code
			,t1.ad_channel
			,t1.new_user_cnt
			,cc.country_name
			,cc.area
			--
			,t1.app_ad_cost::decimal(20,2) as app_ad_cost
			,t1.app_pay_amt::decimal(20,2) as app_pay_amt
			,t1.app_refund_amt::decimal(20,2) as app_refund_amt
			,(t1.app_pay_amt-t1.app_refund_amt)::decimal(20,2) as app_pfpay_amt
			--
			,t1.new_pay_orderall_total as new_pay_orderall_total
			,t1.new_pay_order_total as new_pay_order_total
			,t1.new_pay_amt_total::decimal(20,2) as new_pay_amt_total
			--
			,sum(case when t1.d_date=t1.p_date then new_pay_orderall else 0 end) as new_pay_orderall
			,sum(case when t1.d_date=t1.p_date then new_pay_order else 0 end) as new_pay_order
			,sum(case when t1.d_date=t1.p_date then new_pay_amt else 0 end)::decimal(20,2) as new_pay_amt
			--
			,count(p_date) as date_cnt
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'- 0 month')::date and p_mdate::date  and (d_mdate::date+interval' 0 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_0
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'- 1 month')::date and p_mdate::date  and (d_mdate::date+interval' 1 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_1
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'- 2 month')::date and p_mdate::date  and (d_mdate::date+interval' 2 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_2
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'- 3 month')::date and p_mdate::date  and (d_mdate::date+interval' 3 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_3
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'- 4 month')::date and p_mdate::date  and (d_mdate::date+interval' 4 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_4
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'- 5 month')::date and p_mdate::date  and (d_mdate::date+interval' 5 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_5
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'- 6 month')::date and p_mdate::date  and (d_mdate::date+interval' 6 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_6
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'- 7 month')::date and p_mdate::date  and (d_mdate::date+interval' 7 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_7
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'- 8 month')::date and p_mdate::date  and (d_mdate::date+interval' 8 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_8
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'- 9 month')::date and p_mdate::date  and (d_mdate::date+interval' 9 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_9
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-10 month')::date and p_mdate::date  and (d_mdate::date+interval'10 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_10
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-11 month')::date and p_mdate::date  and (d_mdate::date+interval'11 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_11
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-12 month')::date and p_mdate::date  and (d_mdate::date+interval'12 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_12
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-13 month')::date and p_mdate::date  and (d_mdate::date+interval'13 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_13
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-14 month')::date and p_mdate::date  and (d_mdate::date+interval'14 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_14
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-15 month')::date and p_mdate::date  and (d_mdate::date+interval'15 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_15
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-16 month')::date and p_mdate::date  and (d_mdate::date+interval'16 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_16
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-17 month')::date and p_mdate::date  and (d_mdate::date+interval'17 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_17
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-18 month')::date and p_mdate::date  and (d_mdate::date+interval'18 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_18
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-19 month')::date and p_mdate::date  and (d_mdate::date+interval'19 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_19
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-20 month')::date and p_mdate::date  and (d_mdate::date+interval'20 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_20
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-21 month')::date and p_mdate::date  and (d_mdate::date+interval'21 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_21
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-22 month')::date and p_mdate::date  and (d_mdate::date+interval'22 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_22
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-23 month')::date and p_mdate::date  and (d_mdate::date+interval'23 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_23
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-24 month')::date and p_mdate::date  and (d_mdate::date+interval'24 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_24
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-25 month')::date and p_mdate::date  and (d_mdate::date+interval'25 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_25
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-26 month')::date and p_mdate::date  and (d_mdate::date+interval'26 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_26
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-27 month')::date and p_mdate::date  and (d_mdate::date+interval'27 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_27
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-28 month')::date and p_mdate::date  and (d_mdate::date+interval'28 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_28
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-29 month')::date and p_mdate::date  and (d_mdate::date+interval'29 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_29
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-30 month')::date and p_mdate::date  and (d_mdate::date+interval'30 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_30
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-31 month')::date and p_mdate::date  and (d_mdate::date+interval'31 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_31
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-32 month')::date and p_mdate::date  and (d_mdate::date+interval'32 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_32
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-33 month')::date and p_mdate::date  and (d_mdate::date+interval'33 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_33
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-34 month')::date and p_mdate::date  and (d_mdate::date+interval'34 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_34
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-35 month')::date and p_mdate::date  and (d_mdate::date+interval'35 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_35
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-36 month')::date and p_mdate::date  and (d_mdate::date+interval'36 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_36
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-37 month')::date and p_mdate::date  and (d_mdate::date+interval'37 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_37
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-38 month')::date and p_mdate::date  and (d_mdate::date+interval'38 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_38
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-39 month')::date and p_mdate::date  and (d_mdate::date+interval'39 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_39
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-40 month')::date and p_mdate::date  and (d_mdate::date+interval'40 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_40
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-41 month')::date and p_mdate::date  and (d_mdate::date+interval'41 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_41
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-42 month')::date and p_mdate::date  and (d_mdate::date+interval'42 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_42
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-43 month')::date and p_mdate::date  and (d_mdate::date+interval'43 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_43
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-44 month')::date and p_mdate::date  and (d_mdate::date+interval'44 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_44
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-45 month')::date and p_mdate::date  and (d_mdate::date+interval'45 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_45
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-46 month')::date and p_mdate::date  and (d_mdate::date+interval'46 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_46
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-47 month')::date and p_mdate::date  and (d_mdate::date+interval'47 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_47
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-48 month')::date and p_mdate::date  and (d_mdate::date+interval'48 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_48
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-49 month')::date and p_mdate::date  and (d_mdate::date+interval'49 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_49
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-50 month')::date and p_mdate::date  and (d_mdate::date+interval'50 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_50
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-51 month')::date and p_mdate::date  and (d_mdate::date+interval'51 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_51
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-52 month')::date and p_mdate::date  and (d_mdate::date+interval'52 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_52
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-53 month')::date and p_mdate::date  and (d_mdate::date+interval'53 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_53
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-54 month')::date and p_mdate::date  and (d_mdate::date+interval'54 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_54
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-55 month')::date and p_mdate::date  and (d_mdate::date+interval'55 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_55
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-56 month')::date and p_mdate::date  and (d_mdate::date+interval'56 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_56
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-57 month')::date and p_mdate::date  and (d_mdate::date+interval'57 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_57
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-58 month')::date and p_mdate::date  and (d_mdate::date+interval'58 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_58
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-59 month')::date and p_mdate::date  and (d_mdate::date+interval'59 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_59
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-60 month')::date and p_mdate::date  and (d_mdate::date+interval'60 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_60
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-61 month')::date and p_mdate::date  and (d_mdate::date+interval'61 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_61
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-62 month')::date and p_mdate::date  and (d_mdate::date+interval'62 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_62
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-63 month')::date and p_mdate::date  and (d_mdate::date+interval'63 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_63
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-64 month')::date and p_mdate::date  and (d_mdate::date+interval'64 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_64
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-65 month')::date and p_mdate::date  and (d_mdate::date+interval'65 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_65
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-66 month')::date and p_mdate::date  and (d_mdate::date+interval'66 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_66
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-67 month')::date and p_mdate::date  and (d_mdate::date+interval'67 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_67
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-68 month')::date and p_mdate::date  and (d_mdate::date+interval'68 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_68
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-69 month')::date and p_mdate::date  and (d_mdate::date+interval'69 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_69
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-70 month')::date and p_mdate::date  and (d_mdate::date+interval'70 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_70
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-71 month')::date and p_mdate::date  and (d_mdate::date+interval'71 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_71
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-72 month')::date and p_mdate::date  and (d_mdate::date+interval'72 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_72
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-73 month')::date and p_mdate::date  and (d_mdate::date+interval'73 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_73
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-74 month')::date and p_mdate::date  and (d_mdate::date+interval'74 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_74
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-75 month')::date and p_mdate::date  and (d_mdate::date+interval'75 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_75
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-76 month')::date and p_mdate::date  and (d_mdate::date+interval'76 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_76
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-77 month')::date and p_mdate::date  and (d_mdate::date+interval'77 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_77
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-78 month')::date and p_mdate::date  and (d_mdate::date+interval'78 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_78
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-79 month')::date and p_mdate::date  and (d_mdate::date+interval'79 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_79
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-80 month')::date and p_mdate::date  and (d_mdate::date+interval'80 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_80
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-81 month')::date and p_mdate::date  and (d_mdate::date+interval'81 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_81
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-82 month')::date and p_mdate::date  and (d_mdate::date+interval'82 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_82
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-83 month')::date and p_mdate::date  and (d_mdate::date+interval'83 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_83
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-84 month')::date and p_mdate::date  and (d_mdate::date+interval'84 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_84
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-85 month')::date and p_mdate::date  and (d_mdate::date+interval'85 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_85
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-86 month')::date and p_mdate::date  and (d_mdate::date+interval'86 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_86
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-87 month')::date and p_mdate::date  and (d_mdate::date+interval'87 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_87
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-88 month')::date and p_mdate::date  and (d_mdate::date+interval'88 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_88
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-89 month')::date and p_mdate::date  and (d_mdate::date+interval'89 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_89
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-90 month')::date and p_mdate::date  and (d_mdate::date+interval'90 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_90
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-91 month')::date and p_mdate::date  and (d_mdate::date+interval'91 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_91
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-92 month')::date and p_mdate::date  and (d_mdate::date+interval'92 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_92
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-93 month')::date and p_mdate::date  and (d_mdate::date+interval'93 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_93
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-94 month')::date and p_mdate::date  and (d_mdate::date+interval'94 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_94
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-95 month')::date and p_mdate::date  and (d_mdate::date+interval'95 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_95
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-96 month')::date and p_mdate::date  and (d_mdate::date+interval'96 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_96
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-97 month')::date and p_mdate::date  and (d_mdate::date+interval'97 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_97
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-98 month')::date and p_mdate::date  and (d_mdate::date+interval'98 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_98
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-99 month')::date and p_mdate::date  and (d_mdate::date+interval'99 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_99
			    ,sum(case when d_mdate::date between (p_mdate::date+interval'-100 month')::date and p_mdate::date  and (d_mdate::date+interval'100 month')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_100
				,0 as pay_101
				,0 as pay_102
				,0 as pay_103
				,0 as pay_104
				,0 as pay_105
				,0 as pay_106
				,0 as pay_107
				,0 as pay_108
				,0 as pay_109
				,0 as pay_110
				,0 as pay_111
				,0 as pay_112
				,0 as pay_113
				,0 as pay_114
				,0 as pay_115
				,0 as pay_116
				,0 as pay_117
				,0 as pay_118
				,0 as pay_119
				,0 as pay_120
				,0 as pay_121
				,0 as pay_122
				,0 as pay_123
				,0 as pay_124
				,0 as pay_125
				,0 as pay_126
				,0 as pay_127
				,0 as pay_128
				,0 as pay_129
				,0 as pay_130
				,0 as pay_131
				,0 as pay_132
				,0 as pay_133
				,0 as pay_134
				,0 as pay_135
				,0 as pay_136
				,0 as pay_137
				,0 as pay_138
				,0 as pay_139
				,0 as pay_140
				,0 as pay_141
				,0 as pay_142
				,0 as pay_143
				,0 as pay_144
				,0 as pay_145
				,0 as pay_146
				,0 as pay_147
				,0 as pay_148
				,0 as pay_149
				,0 as pay_150
				,0 as pay_151
				,0 as pay_152
				,0 as pay_153
				,0 as pay_154
				,0 as pay_155
				,0 as pay_156
				,0 as pay_157
				,0 as pay_158
				,0 as pay_159
				,0 as pay_160
				,0 as pay_161
				,0 as pay_162
				,0 as pay_163
				,0 as pay_164
				,0 as pay_165
				,0 as pay_166
				,0 as pay_167
				,0 as pay_168
				,0 as pay_169
				,0 as pay_170
				,0 as pay_171
				,0 as pay_172
				,0 as pay_173
				,0 as pay_174
				,0 as pay_175
				,0 as pay_176
				,0 as pay_177
				,0 as pay_178
				,0 as pay_179
				,0 as pay_180
				,0 as pay_185
				,0 as pay_195
				,0 as pay_205
				,0 as pay_215
				,0 as pay_225
				,0 as pay_235
				,0 as pay_245
				,0 as pay_255
				,0 as pay_265
				,0 as pay_275
				,0 as pay_285
				,0 as pay_295
				,0 as pay_305
				,0 as pay_315
				,0 as pay_325
				,0 as pay_335
				,0 as pay_345
				,0 as pay_355
				,0 as pay_365
			from analysis.dw_operate_roi_v2_calc01 t1
			left join v_dim_country_area cc on t1.country_code=cc.country_code
			where date_tag='month'
			group by
			 t1.date_tag
			,t1.d_date
			,t1.d_mdate
			,t1.country_code
			,t1.ad_channel
			,t1.new_user_cnt
			,cc.country_name
			,cc.area
			,t1.app_ad_cost
			,t1.app_pay_amt
			,t1.app_refund_amt
			,t1.new_pay_orderall_total
			,t1.new_pay_order_total
			,t1.new_pay_amt_total
		; -- 月



			truncate table public.dw_operate_roi_v2;
			insert into public.dw_operate_roi_v2  select * from analysis.dw_operate_roi_v2_tmp01;

