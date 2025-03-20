----------------------------------------------------
-- 排除日活表中的自动续订人员
-- 保持活跃用户的金额不变，所以不能直接用where来排除
-- 表：public.duanju_vip_order   判断字段： public.duanju_vip_order.agreement_no
----------------------------------------------------
----------------------------------------------------
--  求每一天的非续订活跃用户
----------------------------------------------------
select
    device_id,
	to_timestamp(pay_time)::date as pay_date
from public.duanju_vip_order
where device_id is not null and device_id<>'' and status>=2 and (agreement_no is null or agreement_no = '')
group by device_id, to_timestamp(pay_time)::date;

----------------------------------------------------
--  修改后sql
----------------------------------------------------
truncate table tmp.dwd_user_active_tmp01 ;
insert into tmp.dwd_user_active_tmp01
-- 脚本
	with tmp_device_act as(
		select
		 device_id
		,created_date as act_date
		,string_agg(distinct app_id,'') as act_app_id
		,string_agg(distinct app_name,'') as act_app_name
		,string_agg(distinct app_version,'' ) as act_app_version
		,string_agg(distinct os,'') as act_os
		,string_agg(distinct brand,'') as act_brand
		,min(link_id) as act_link_id
		,count(1) as pv
		,count(distinct case when event_name='play_drama' and unlock_type='ad' then eid else null end) as unlock_eid_num
		,count(distinct case when event_name='play_drama' then eid else null end) as watch_eid_num
		,count(distinct case when event_name='play_drama' then vid else null end) as watch_vid_num
		,sum(case when event_name='stay_drama' then cast(time as numeric) else 0 end) duration
		from public.user_track_log
		WHERE event_name in ('start_app','stay_app','enter_tab','show_cover','click_cover','enter_player','play_drama' ,'stay_drama')
		and created_at is not null   and created_at!=0
		and device_id is not null
		-- and created_date>=(current_date+interval'-1 d')::date -- 增
		group by
		 device_id
		,created_date
	)
	, tmp_device_order_valid as (
	    select
            device_id,
            to_timestamp(pay_time)::date as pay_date
        from public.duanju_vip_order
        where device_id is not null and device_id<>'' and status>=2 and (agreement_no is null or agreement_no = '')
        group by device_id, to_timestamp(pay_time)::date
    )
	,tmp_device_order as(
		select
		 t1.device_id
		,t1.pay_date
		,count(distinct order_id) as pay_order
		,sum(amount) as pay_amt
		from tmp_device_order_valid t1 left join public.duanju_vip_order t2 on t1.device_id=t2.device_id and t1.pay_date=to_timestamp(pay_time)::date
		where t2.device_id is not null and t2.device_id<>''  and status>=2
		-- and to_timestamp(pay_time)::date>=(current_date+interval'-1 d')::date -- 增
		group by t1.device_id
		,t1.pay_date
	)
	,tmp_primary as(
		select distinct device_id,act_date from(
		select distinct device_id,act_date from tmp_device_act union all
		select distinct device_id,pay_date from tmp_device_order union all
		select distinct device_id,created_date from dw.dwd_user_info
			-- where created_date>=(current_date+interval'-1 d')::date -- 增
		)a
	)

	select
	 md5(concat(t0.device_id,t0.act_date)) as id
	,t0.device_id
	,t0.act_date
	,t1.act_app_id
	,t1.act_app_name
	,t1.act_app_version
	,t1.act_os
	,t1.act_brand
	,t1.act_link_id
	,t1.pv
	,t1.unlock_eid_num
	,t1.watch_eid_num
	,t1.watch_vid_num
	,t1.duration
	,t2.pay_order
	,t2.pay_amt
	,t3.app_id
	,t3.os
	,t3.device_type
	,t3.wechat_open_id
	,t3.oaid
	,t3.mobile
	,t3.created_date
	,t3.created_at
	,t3.mobile_bind_date
	,t3.expiration_date
	,t3.is_deleted
	,t3.guiyin_date
	,t3.ad_channel_id
	,t3.ad_channel
	,t3.link_id
	,CURRENT_TIMESTAMP as etl_time
	from tmp_primary t0
	left join tmp_device_act t1 on t0.device_id=t1.device_id and t0.act_date=t1.act_date
	left join tmp_device_order t2 on t0.device_id=t2.device_id and t0.act_date=t2.pay_date
	left join dw.dwd_user_info t3 on t0.device_id=t3.device_id
	;

	truncate table dw.dwd_user_active ;
	insert into dw.dwd_user_active select * from tmp.dwd_user_active_tmp01;


