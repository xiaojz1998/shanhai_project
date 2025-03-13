set timezone ='UTC-0';
delete from tmp.dw_operate_schedule_tmp01 where d_date>= (current_date+interval '-2 day')::date::text;
insert into tmp.dw_operate_schedule_tmp01
-- 脚本
with new_reg_users as (
	select v_date as created_date
	,d_date::date as d_date
	,uid::int8 as uid
	,country_code
	,lang
	,lang_name
	,is_campaign
	from public.dwd_user_info

)
,tmp_watch as(
	select a.d_date::text d_date
	,coalesce(u0.country_code,'UNKNOWN') as country_code
	,coalesce(u0.lang,'UNKNOWN') as lang
	,sum(a.vid_watch_cnt) as vid_watch_cnt
	,sum(a.eid_watch_cnt) as eid_watch_cnt
	,round(sum(a.watch_duration)/60.0,2) as watch_duration
	,count(distinct case when a.vid_watch_cnt>0 then a.uid else null end) as watch_user
	,sum(a.eidpay_watch_cnt) as eidpay_watch_cnt
	,sum(a.eidfree_watch_cnt) as eidfree_watch_cnt
	,count(distinct case when a.eidpay_watch_cnt>0 then a.uid else null end) as eidpay_watch_user
	,count(distinct case when a.eidfree_watch_cnt>0 then a.uid else null end) as eidfree_watch_user
	from(
		select to_timestamp(a.created_at) :: date as d_date
		,a.country_code
		,a.uid
		,count(distinct a.vid) as vid_watch_cnt -- 每人看短剧数
		,count(distinct a.eid) as eid_watch_cnt -- 每人看剧集数
		,count(distinct case when e.sort >= c.pay_num then a.eid else null end) as eidpay_watch_cnt
		,count(distinct case when e.sort <  c.pay_num then a.eid else null end) as eidfree_watch_cnt
		,sum(case when a.event=2 then watch_time else 0 end) as watch_duration -- "看剧时长(分钟)"
		from public.app_user_track_log a
		left join "oversea-api_osd_videos" c on a.vid = c.id
		left join "oversea-api_osd_video_episodes" e on a.eid = e.id
		where 1=1
		and a.event in (1,2,13,14)
		and a.vid>0 and a.eid>0
		-- and a.watch_time >3
		-- and to_timestamp(a.created_at) :: date>='2024-11-01'
			and to_timestamp(a.created_at) :: date>=(current_date+interval '-2 day')::date
		group by to_timestamp(a.created_at) :: date
		,a.country_code
		,a.uid
	)a
	left join new_reg_users u0 on a.uid=u0.uid
	group by a.d_date::text
	,coalesce(u0.country_code,'UNKNOWN')
	,coalesce(u0.lang,'UNKNOWN')
)
,tmp_operate_01 as(
	select
	t1.d_date
	,t1.country_code
	,t1.country_name
	,t1.area
	,t1.lang
	,t1.lang_name
	,sum(t1.dau) as dau
	,sum(t1.dau_7login) as dau_7login
	,sum(t1.new_dau) as new_dau
	,sum(t1.new_dau_2login) as new_dau_2login
	,sum(t1.old_dau) as old_dau
	,sum(t1.old_dau_2login) as old_dau_2login
	,sum(t1.pay_order) as pay_order
	,sum(t1.pay_user) as pay_user
	,sum(t1.pay_amt) as pay_amt
	,sum(t1.ad_income_amt) as ad_income_amt
	,sum(t1.new_pay_order) as new_pay_order
	,sum(t1.new_pay_user) as new_pay_user
	,sum(t1.new_pay_amt) as new_pay_amt
	,sum(t1.old_pay_order) as old_pay_order
	,sum(t1.old_pay_user) as old_pay_user
	,sum(t1.old_pay_amt) as old_pay_amt
	,sum(t1.ad_cost) as ad_cost
	,sum(t1.ad_cost_tt) as ad_cost_tt
	,sum(t1.ad_cost_fb) as ad_cost_fb
	,sum(t1.ad_cost_asa) as ad_cost_asa
	,sum(t1.ad_cost_other) as ad_cost_other
	,sum(t1.pay_refund_amt) as pay_refund_amt
	,sum(t1.dau_2login) as dau_2login
	,sum(t1.dau_3login) as dau_3login
	,sum(t1.dau_14login) as dau_14login
	,sum(t1.dau_30login) as dau_30login
	from public.dw_operate_view t1
	group by
	t1.d_date
	,t1.country_code
	,t1.country_name
	,t1.area
	,t1.lang
	,t1.lang_name
)
,tmp_operate as(
	select
	t1.d_date
	,t1.country_code
	,t1.country_name
	,t1.area
	,t1.lang
	,t1.lang_name
	,t1.dau
	,t1.dau_7login
	,t1.new_dau
	,t1.new_dau_2login
	,t1.old_dau
	,t1.old_dau_2login
	,t1.pay_order
	,t1.pay_user
	,t1.pay_amt
	,t1.ad_income_amt
	,t1.new_pay_order
	,t1.new_pay_user
	,t1.new_pay_amt
	,t1.old_pay_order
	,t1.old_pay_user
	,t1.old_pay_amt
	,sum(t1.pay_amt+t1.ad_income_amt) over(partition by substr(t1.d_date,1,7),t1.country_code,t1.lang order by t1.d_date asc ) as month_income
	,t1.ad_cost
	,t1.ad_cost_tt
	,t1.ad_cost_fb
	,t1.ad_cost_asa
	,t1.ad_cost_other
	,t1.pay_refund_amt
,t1.dau_2login
,t1.dau_3login
,t1.dau_14login
,t1.dau_30login
,sum(t1.ad_cost) over(partition by substr(t1.d_date,1,7),t1.country_code,t1.lang order by t1.d_date asc ) as month_ad_cost
	from tmp_operate_01 t1
)
select
 t1.d_date
,t1.country_code
,t1.country_name
,t1.area
,t1.lang
,t1.lang_name
,t1.dau
,t1.dau_7login
,t1.new_dau
,t1.new_dau_2login
,t1.old_dau
,t1.old_dau_2login
,t1.pay_order
,t1.pay_user
,t1.pay_amt
,t1.ad_income_amt
,t1.new_pay_order
,t1.new_pay_user
,t1.new_pay_amt
,t1.old_pay_order
,t1.old_pay_user
,t1.old_pay_amt
,t1.month_income
,coalesce(t2.vid_watch_cnt,0) as vid_watch_cnt
,coalesce(t2.eid_watch_cnt,0) as eid_watch_cnt
,coalesce(t2.watch_duration,0) as watch_duration
,coalesce(t2.watch_user,0) as watch_user
,coalesce(t2.eidpay_watch_cnt,0) as eidpay_watch_cnt
,coalesce(t2.eidfree_watch_cnt,0) as eidfree_watch_cnt
,coalesce(t2.eidpay_watch_user,0) as eidpay_watch_user
,coalesce(t2.eidfree_watch_user,0) as eidfree_watch_user
,t1.ad_cost
,t1.ad_cost_tt
,t1.ad_cost_fb
,t1.ad_cost_asa
,t1.ad_cost_other
,t1.pay_refund_amt
,t1.dau_2login
,t1.dau_3login
,t1.dau_14login
,t1.dau_30login
,t1.month_ad_cost
from tmp_operate t1
left join tmp_watch t2 on t1.d_date=t2.d_date and t1.country_code=t2.country_code and t1.lang=t2.lang
where 1=1
and t1.d_date>= (current_date+interval '-2 day')::date::text
;

	delete from public.dw_operate_schedule where d_date>= (current_date+interval '-2 day')::date::text;
	insert into public.dw_operate_schedule select * from tmp.dw_operate_schedule_tmp01 where d_date>= (current_date+interval '-2 day')::date::text;



