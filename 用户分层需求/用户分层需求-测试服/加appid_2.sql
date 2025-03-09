-------------------------------------------------------------
-- 注入dw_video_hot
-- 废掉 total_user_track_log 直接在sql中合并
-------------------------------------------------------------
truncate table dw.dw_video_hot;
INSERT INTO dw.dw_video_hot (vid,app_source_id,play_pv, pay_pv, search_pv, hot, etl_time)
WITH tmp_total_user_track_log as (
    select
    md5(concat(id,'duanju'))  as id               ,
    model            ,
    order_type       ,
    uid              ,
    device_id        ,
    event_name       ,
    sku_id           ,
    system           ,
    order_id         ,
    os               ,
    ad_entrance      ,
    vip_entrance     ,
    network          ,
    percent          ,
    vid              ,
    balance          ,
    event_id         ,
    k_coin           ,
    time             ,
    aid              ,
    brand            ,
    unlock_type      ,
    vid_is_pay       ,
    eid              ,
    money            ,
    is_vip           ,
    eid_is_pay       ,
    ext_body         ,
    app_name         ,
    created_at       ,
    entrance         ,
    oaid             ,
    payment          ,
    sort             ,
    ad_type          ,
    app_id           ,
    created_date     ,
    ad_id            ,
    account_id       ,
    ad_channel       ,
    fail_code        ,
    fail_msg         ,
    link_id          ,
    app_version      ,
    vip_expired_time ,
    rank             ,
    play_speed       ,
    click_content    ,
    task_id          ,
    task_type        ,
    skip_to          ,
    signin_days      ,
    action           ,
    source           ,
    num              ,
    is_auto_unlock   ,
    category_name    ,
    tab_name         ,
    type             ,
    page_name        ,
    popup_name       ,
    '1' as app_source_id ,
    'duanju' as app_source_name
from public.user_track_log
union all
select
    md5(concat(id,'xingju'))  as id             ,
    model            ,
    order_type       ,
    uid              ,
    device_id        ,
    event_name       ,
    sku_id           ,
    system           ,
    order_id         ,
    os               ,
    ad_entrance      ,
    vip_entrance     ,
    network          ,
    percent          ,
    vid              ,
    balance          ,
    event_id         ,
    k_coin           ,
    time             ,
    aid              ,
    brand            ,
    unlock_type      ,
    vid_is_pay       ,
    eid              ,
    money            ,
    is_vip           ,
    eid_is_pay       ,
    ext_body         ,
    app_name         ,
    created_at       ,
    entrance         ,
    oaid             ,
    payment          ,
    sort             ,
    ad_type          ,
    app_id           ,
    created_date     ,
    ad_id            ,
    account_id       ,
    ad_channel       ,
    fail_code        ,
    fail_msg         ,
    link_id          ,
    app_version      ,
    vip_expired_time ,
    rank             ,
    play_speed       ,
    click_content    ,
    task_id          ,
    task_type        ,
    skip_to          ,
    signin_days      ,
    action           ,
    source           ,
    num              ,
    is_auto_unlock   ,
    category_name    ,
    tab_name         ,
    type             ,
    page_name        ,
    popup_name       ,
    '2' as app_source_id,
    'xingju' as app_source_name
from public.xingju_user_track_log
) ,play_stats AS (
    SELECT
        vid,
        app_source_id,
        SUM(pv) AS pv_sum
    FROM
    (
        SELECT
            vid,
            device_id,
            app_source_id,
            COUNT(DISTINCT created_at) AS pv
        FROM tmp_total_user_track_log
        WHERE event_name = 'enter_player'
          AND created_date >= '2024-12-17'
          AND vid > 0
        GROUP BY vid, device_id,app_source_id
    ) t
    GROUP BY vid,app_source_id
),
recharge_stats AS (
    SELECT
        vid,
        app_source_id,
        COUNT(DISTINCT order_id) AS pv
    FROM tmp_total_user_track_log
    WHERE event_name = 'recharge_success'
      AND created_date >= '2024-12-17'
      AND vid > 0
    GROUP BY vid,app_source_id
),
search_stats AS (
    SELECT
        vid,
        app_source_id,
        SUM(pv) AS pv_sum
    FROM
    (
        SELECT
            vid,
            device_id,
            app_source_id,
            COUNT(DISTINCT created_at) AS pv
        FROM tmp_total_user_track_log
        WHERE event_name = 'show_cover'
          AND created_date >= '2024-12-17'
          AND entrance = 'search_result'
        GROUP BY vid, device_id,app_source_id
    ) t
    GROUP BY vid,app_source_id
),
all_vids AS (
    SELECT DISTINCT vid,app_source_id FROM tmp_total_user_track_log WHERE vid > 0
   and created_date >= '2024-12-17'
)
SELECT
    a.vid,
    a.app_source_id,
    COALESCE(p.pv_sum, 0) AS play_pv,
    COALESCE(r.pv, 0) AS pay_pv,
    COALESCE(s.pv_sum, 0) AS search_pv,
    ((COALESCE(p.pv_sum, 0) * 0.5 + COALESCE(r.pv, 0) * 0.35 + COALESCE(s.pv_sum, 0) * 0.15) * 100) AS hot,
    NOW() AS etl_time
FROM
    all_vids a
LEFT JOIN play_stats p ON a.vid = p.vid and a.app_source_id = p.app_source_id
LEFT JOIN recharge_stats r ON a.vid = r.vid and a.app_source_id = r.app_source_id
LEFT JOIN search_stats s ON a.vid = s.vid and a.app_source_id = s.app_source_id
order by  hot desc;
-------------------------------------------------------------
-- 注入用户分层表（参考表已经废弃）
-------------------------------------------------------------
truncate table dw.dw_user_layer_tag ;
	insert into dw.dw_user_layer_tag
-- 设备账号
with tmp_device as(
	select *
	from(
	select device_id ,case when os=1 then 'android' when os=2 then 'ios' end as os
	,to_char(created_at, 'YYYY-MM-DD')::date as first_date
	,row_number() over(partition by device_id order by created_at) as rn
	from public.duanju_accounts
	where is_deleted=0
	and device_id is not null
	)a where rn=1
)
-- 渠道归因
,tmp_device_guiyin as(
	select
     t1.device_id
    ,t1.link_id
    ,t1.guiyin_date
    ,t2.ad_channel_id
    ,coalesce(t2.ad_channel,'归因缺失') as ad_channel
	from(
		select * from(
		select
         device_id
        ,link_id -- 推广链接id
        ,to_date(created_at, 'YYYY-MM-DD HH24:MI:SS')::date AS guiyin_date
        ,row_number() over(partition by device_id order by created_at) as rn
	    from public.mg_duanju_report_active_record
	    )t where rn=1
    )t1
    left join(
    	select distinct
         id as link_id
        ,kind as ad_channel_id
        ,case when kind=1 then '巨量(抖音)'
            when kind=2 then '快手'
            when kind=3 then '腾讯'
            when kind=4 then '百度'
            when kind=5 then '小米'
            when kind=6 then 'vivo'
            when kind=7 then 'oppo'
        	else '其他' end as ad_channel
    	from public.duanju_promotion_links
    )t2 on t1.link_id=t2.link_id
)
-- 广告解锁集数、观看剧集数
,tmp_ad_unlock as(
	select device_id
	,count(distinct case when unlock_type='ad' then eid else null end) as unlock_eid_cnt
	,count(distinct eid) as watch_eid_cnt
	from public.user_track_log
	where event_name ='play_drama'
	and created_at is not null   and created_at!=0
	and device_id is not null
	group by device_id
)
-- 活跃维度：活跃天数、最近活跃日期
,tmp_device_act as(
	select device_id
    ,count(distinct to_timestamp(created_at)::date ) as act_dnt
    ,max(to_timestamp(created_at)::date) as max_actdate
    ,current_date-max(to_timestamp(created_at)::date) as silent_dnt
	from public.user_track_log
	WHERE event_name in ('start_app','stay_app','enter_tab','show_cover','click_cover','enter_player','play_drama')
	and created_at is not null   and created_at!=0
	and device_id is not null
	group by device_id
)
-- 充值维度：金额、次数、类型
,tmp_device_order as(
-- select device_id
-- ,sum(product_pay_order) as pay_order
-- ,sum(product_pay_amt) as pay_amt
-- ,array_agg(concat( product_name,',',product_pay_order,',',product_pay_amt))::text
-- from(
	select device_id
	,product_name
	,case when product_name like '%看点币%' then '看点币'
		when product_name like '%周卡%' then '周卡'
		when product_name like '%月卡%' then '月卡'
		when product_name like '%季卡%' then '季卡'
		when product_name like '%年卡%' then '年卡'
		when product_name like '%包周%' then '连续包周会员'
		when product_name like '%包月%' then '连续包月会员'
		when product_name like '%包季%' then '连续包季会员'
		when product_name like '%包年%' then '连续包年会员'
		else '测试' end as product_category
	,count(distinct order_id) as product_pay_order
	,sum(amount) as product_pay_amt
	from public.duanju_vip_order
	where device_id is not null and device_id<>''  and status>=2
	group by device_id
	,product_name
	order by device_id desc
-- )a
-- group by device_id

),tmp_duanju_account_device as (
    SELECT
        device_id,
        account_id,
        app_id
    from public.duanju_account_device
),tmp_duanju_account_vip as (
    select
        account_id
    	,expire_time
    from public.duanju_account_vip
),tmp_device_appid_vip as (
    select
        t1.device_id,
        t1.app_id,
        max(case when current_timestamp < to_timestamp(coalesce(expire_time,0)) then 2 else 1 end) as subscription_status
    from tmp_duanju_account_device t1
    left join tmp_duanju_account_vip t2 on t1.account_id=t2.account_id
    GROUP BY t1.device_id, t1.app_id
)
select
 md5(concat(t1.device_id,coalesce(t5.product_name,'未知'))) as id
,t1.device_id
,t1.os
,t1.first_date
,t2.guiyin_date
,t2.ad_channel_id
,t2.ad_channel
,t3.unlock_eid_cnt
,t3.watch_eid_cnt
,t4.act_dnt
,t4.silent_dnt
,null as product_id
,t5.product_name
,t5.product_category
,t5.product_pay_order
,t5.product_pay_amt
,CURRENT_TIMESTAMP as etl_time
,t6.subscription_status
,t6.app_id
from tmp_device t1
left join tmp_device_guiyin t2 on t1.device_id=t2.device_id
left join tmp_ad_unlock t3 on t1.device_id=t3.device_id
left join tmp_device_act t4 on t1.device_id=t4.device_id
left join tmp_device_order t5 on t1.device_id=t5.device_id
left join tmp_device_appid_vip t6 on t1.device_id=t6.device_id;
-- where 1=1
-- and t1.device_id='2ff3918ebfeeb3fdbb1f211b31f84028e';

-------------------------------------------------------------
-- 注入 用户分层表
-- public.duanju_accounts
-- public.mg_duanju_report_active_record
-- public.duanju_promotion_links
-- public.user_track_log
-- public.duanju_vip_order
-- public.duanju_account_device
-- public.duanju_account_vip
-------------------------------------------------------------
truncate table dw.dw_user_layer_tag ;
insert into dw.dw_user_layer_tag
    with tmp_total_accounts as (
        select
            *,
            '1' as app_source_id
        from duanju_accounts
        union all
        select
            *,
            '2' as app_source_id
        from xingju_accounts
    ),
    tmp_total_mg_report_active_record as (
        select
            *,
            '1' as app_source_id
        from mg_duanju_report_active_record
    ),
    tmp_total_promotion_links as (
        select
            *
        from duanju_promotion_links
    ),
    tmp_total_user_track_log as (
      select
          *,
          '1' as app_source_id
      from user_track_log
      union all
      select
          *,
          '2' as app_source_id
      from xingju_user_track_log
    ),
    tmp_total_vip_order as (
        select
            *,
            '1' as app_source_id
        from duanju_vip_order
        union all
        select
            *,
            '2' as app_source_id
        from xingju_vip_order
    ),
    tmp_total_account_device as (
        select
            *,
            '1' as app_source_id
        from duanju_account_device
        union all
        select
            *,
            '2' as app_source_id
        from xingju_account_device
    ),
    tmp_total_account_vip as (
        select
            *,
            '1' as app_source_id
        from duanju_account_vip
        union all
        select
            *,
            '2' as app_source_id
        from xingju_account_vip
    ),
    -- 设备账号
	tmp_device as(
		select *
		from(
		select device_id
		,app_source_id
		,case when os=1 then 'android' when os=2 then 'ios' end as os
		,to_char(created_at, 'YYYY-MM-DD')::date as first_date
		,row_number() over(partition by device_id,app_source_id order by created_at) as rn
		from tmp_total_accounts
		where is_deleted=0
		and device_id is not null
		)a where rn=1
	),
	-- 渠道归因
	tmp_device_guiyin as(
		select
	     t1.device_id
		,t1.app_source_id
	    ,t1.link_id
	    ,t1.guiyin_date
	    ,t2.ad_channel_id
	    ,coalesce(t2.ad_channel,'归因缺失') as ad_channel
		from(
			select * from(
			select
	         device_id
			,app_source_id
	        ,link_id -- 推广链接id
	        ,to_date(created_at, 'YYYY-MM-DD HH24:MI:SS')::date AS guiyin_date
	        ,row_number() over(partition by device_id,app_source_id order by created_at) as rn
		    from tmp_total_mg_report_active_record
		    )t where rn=1
	    )t1
	    left join(
	    	select distinct
	         id as link_id
	        ,kind as ad_channel_id
	        ,case when kind=1 then '巨量(抖音)'
	            when kind=2 then '快手'
	            when kind=3 then '腾讯'
	            when kind=4 then '百度'
	            when kind=5 then '小米'
	            when kind=6 then 'vivo'
	            when kind=7 then 'oppo'
	        	else '其他' end as ad_channel
	    	from tmp_total_promotion_links
	    )t2 on t1.link_id=t2.link_id
	)
	-- 广告解锁集数、观看剧集数
	,tmp_ad_unlock as(
		select device_id
		,app_source_id
		,count(distinct case when unlock_type='ad' then eid else null end) as unlock_eid_cnt
		,count(distinct eid) as watch_eid_cnt
		from tmp_total_user_track_log
		where event_name ='play_drama'
		and created_at is not null   and created_at!=0
		and device_id is not null
		group by device_id,app_source_id
	)
	-- 活跃维度：活跃天数、最近活跃日期
	,tmp_device_act as(
		select device_id
		,app_source_id
	    ,count(distinct to_timestamp(created_at)::date ) as act_dnt
	    ,max(to_timestamp(created_at)::date) as max_actdate
	    ,current_date-max(to_timestamp(created_at)::date) as silent_dnt
		from tmp_total_user_track_log
		WHERE event_name in ('start_app','stay_app','enter_tab','show_cover','click_cover','enter_player','play_drama')
		and created_at is not null   and created_at!=0
		and device_id is not null
		group by device_id,app_source_id
	)
	-- 充值维度：金额、次数、类型
	,tmp_device_order as(
	-- select device_id
	-- ,sum(product_pay_order) as pay_order
	-- ,sum(product_pay_amt) as pay_amt
	-- ,array_agg(concat( product_name,',',product_pay_order,',',product_pay_amt))::text
	-- from(
		select device_id
		,product_name
		,app_source_id
		,case when product_name like '%看点币%' then '看点币'
			when product_name like '%周卡%' then '周卡'
			when product_name like '%月卡%' then '月卡'
			when product_name like '%季卡%' then '季卡'
			when product_name like '%年卡%' then '年卡'
			when product_name like '%包周%' then '连续包周会员'
			when product_name like '%包月%' then '连续包月会员'
			when product_name like '%包季%' then '连续包季会员'
			when product_name like '%包年%' then '连续包年会员'
			else '测试' end as product_category
		,count(distinct order_id) as product_pay_order
		,sum(amount) as product_pay_amt
		from tmp_total_vip_order
		where device_id is not null and device_id<>''  and status>=2
		group by device_id
		,product_name
		,app_source_id
		order by device_id desc
	-- )a
	-- group by device_id

	),tmp_duanju_account_device as (
        SELECT
            device_id,
            account_id,
            app_source_id
        from tmp_total_account_device
    ),tmp_duanju_account_vip as (
        select
            account_id
        	,expire_time
            ,app_source_id
        from tmp_total_account_vip
    ),tmp_device_appid_vip as (
        select
            t1.device_id,
            t1.app_source_id,
            max(case when current_timestamp < to_timestamp(coalesce(expire_time,0)) then 2 else 1 end) as subscription_status
        from tmp_duanju_account_device t1
        left join tmp_duanju_account_vip t2 on t1.account_id=t2.account_id and t1.app_source_id = t2.app_source_id
        GROUP BY t1.device_id, t1.app_source_id
    )
	select
	 md5(concat(t1.device_id,coalesce(t5.product_name,'未知'),t1.app_source_id)) as id
	,t1.device_id
	,t1.os
	,t1.first_date
	,t2.guiyin_date
	,t2.ad_channel_id
	,t2.ad_channel
	,t3.unlock_eid_cnt
	,t3.watch_eid_cnt
	,t4.act_dnt
	,t4.silent_dnt
	,null as product_id
	,t5.product_name
	,t5.product_category
	,t5.product_pay_order
	,t5.product_pay_amt
	,CURRENT_TIMESTAMP as etl_time
    ,t6.subscription_status
    ,t6.app_source_id
	from tmp_device t1
	left join tmp_device_guiyin t2 on t1.device_id=t2.device_id and t1.app_source_id = t2.app_source_id
	left join tmp_ad_unlock t3 on t1.device_id=t3.device_id and t1.app_source_id = t3.app_source_id
	left join tmp_device_act t4 on t1.device_id=t4.device_id and t1.app_source_id = t4.app_source_id
	left join tmp_device_order t5 on t1.device_id=t5.device_id and t1.app_source_id = t5.app_source_id
    left join tmp_device_appid_vip t6 on t1.device_id=t6.device_id and t1.app_source_id = t6.app_source_id
	-- where 1=1
	-- and t1.device_id='2ff3918ebfeeb3fdbb1f211b31f84028e'
	;
