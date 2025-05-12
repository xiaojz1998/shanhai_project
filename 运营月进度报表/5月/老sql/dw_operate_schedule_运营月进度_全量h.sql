------------------------------------------
-- file: dw_operate_schedule_运营月进度_全量h.sql
-- author: xiaoj
-- time: 2025/5/10 13:00
-- description:
------------------------------------------
-- show timezone;  --
set timezone ='UTC-0';
truncate table tmp.dw_operate_schedule_tmp02 ;
insert into tmp.dw_operate_schedule_tmp02
-- 脚本
-- 新增用户信息，用于补全国家和语言
with new_reg_users as (
	select v_date as created_date
	,d_date::date as d_date
	,uid::int8 as uid
	,country_code
	,lang
	,lang_name
	,is_campaign
	from public.dwd_user_info   -- 新增用户表
)
-- 每日国家语言观看行为统计
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
		-- 找到每日每个用户的观看剧集行为
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
		and to_timestamp(a.created_at) :: date>='2024-11-01'
		group by to_timestamp(a.created_at) :: date
		,a.country_code
		,a.uid
	)a
	left join new_reg_users u0 on a.uid=u0.uid
	group by a.d_date::text
	,coalesce(u0.country_code,'UNKNOWN')
	,coalesce(u0.lang,'UNKNOWN')
)
-- 求出pay_7
,tmp_pay as (
    select
        d_date,
        country_code,
        lang,
        sum(case when d_date::date between (p_date::date+interval'- 7 d')::date and p_date::date  and (d_date::date+interval' 7 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_7
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
                and created_date>=20240701
                group by
                to_char( to_timestamp(created_at),'YYYY-MM-DD')
                ,uid
            )a
            group by d_date,uid
    ) t2 on t1.uid = t2.uid and t1.d_date <= t2.d_date
    group by t1.d_date,t2.d_date, coalesce(t1.country_code,'UNKNOWN'), coalesce(t1.lang,'UNKNOWN')) t3
    group by d_date,country_code, lang
)
-- 退款金额
, tmp_refund as (
        select
            d_date,
            country_code,
            lang,
            sum(case when d_date::date between (r_date::date+interval'- 7 d')::date and r_date::date  and (d_date::date+interval' 7 d')<=(current_date+interval'-1 d') then new_pay_refund_amt else null end)::decimal(20,2) as refund_7
        from (select
            t1.d_date,
            t2.d_date as r_date,
            coalesce(t1.country_code,'UNKNOWN') as country_code,
            coalesce(t1.lang,'UNKNOWN') as lang,
            sum(t2.pay_refund_amt)::decimal(20,2) as new_pay_refund_amt
        from new_reg_users t1
        left join (
        -- 每日用户退款订单 美分
        select
            (to_char(to_timestamp(created_at),'YYYY-MM-DD'))::date as d_date,
            r.uid,
    		sum(r.total_money*0.01) as pay_refund_amt -- 退款金额
    		from public.all_refund_order_log r
    		where r.environment = 1  and r.os in('android','ios')
    		and r.status = 1
            and r.refund_date>=20240701
            -- and r.refund_date>=20250101
            -- 	and to_char( to_timestamp(r.refund_time),'YYYY-MM-DD') >= (current_date+interval '-2 day')::date::text -- 增
    		group by (to_char(to_timestamp(created_at),'YYYY-MM-DD'))::date,r.uid
    		) t2 on t1.uid = t2.uid and t1.d_date <= t2.d_date
        group by t1.d_date,t2.d_date, coalesce(t1.country_code,'UNKNOWN'), coalesce(t1.lang,'UNKNOWN')) t3
        group by d_date,country_code, lang
)
,tmp_push as (
    select
        push_date,
        lang,
        sum(push_unt) total_push_unt,
        sum(click_unt) total_click_unt,
        case when sum(push_unt)=0 then null
            else 1.0*max(click_unt)/max(push_unt)
            end as push_click_rate
    from (select
        push_id,
        push_time::date as push_date,
        push_unt,
        click_unt,
        case
             when t2.lang_name='英语阿拉伯语' then '阿拉伯语'
             when t2.lang_name=''or t2.lang_name is null then 'UNKNOWN' else t2.lang_name end as lang
    from(
        select
         id::text as push_id
        ,to_timestamp(pushed_at) at time zone 'UTC-8' as push_time  --,to_timestamp(pushed_at) at time zone 'UTC-8'
        ,json_array_elements((REGEXP_MATCH(replace(user_layered_configs, '\"', '"'),'\[.*?\]'))[1]::json) ->> 'id' as layered_id
        ,delivered_count as push_unt
        ,click_count as click_unt
        from public."oversea-api_osd_pushed" x
    )t1
    left join(
        select t1.*,t2."name" as lang_name
        from(
        select
            id ,
            lang_config::json ->> 0 as lang_code
        from public."oversea-api_osd_user_layered_configs"
        where lang_config <>'[]'
        )t1
        left join public."oversea-api_osd_lang" t2 on t1.lang_code=t2.lang_code
    )t2 on t1.layered_id=t2.id::text ) t3
    group by push_date,lang
)
,tmp_operate_01 as(
	-- 分组聚合时为了统计不同os的总和
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
	,case when SUM(dau) = 0 then 0 else SUM(pay_amt) / SUM(dau) end as pay_amt_per_user     -- 客单价
	,CASE WHEN SUM(ad_cost) = 0.00  THEN 0 ELSE SUM(pay_amt+ad_income_amt-pay_refund_amt) / SUM(ad_cost) END as total_ROI   -- 总roi
	,sum(repay_user) as repay_user
	,sum(due_user)   as due_user
	,case when sum(due_user)=0 then null  else 1.0*sum(repay_user)/sum(due_user) end as subscription_rate
	,case when sum(dau)=0 then null else 1.0*sum(dau_7login)/sum(dau) end as total_retention_rate_7d
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
    --
	,t1.pay_amt_per_user
	,t1.total_ROI
	,t1.repay_user
	,t1.due_user
	,t1.subscription_rate
	,t1.total_retention_rate_7d
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
--
,t1.pay_amt_per_user            -- 客单价
,t1.total_ROI                   -- 总roi
,t1.repay_user
,t1.due_user
,t1.subscription_rate           -- 续订率
,t1.total_retention_rate_7d     -- 7日留存率
,pay_7                          -- 7日支付
,refund_7                       -- 7日退款
,total_push_unt                 -- 触及人数
,total_click_unt                -- 点击人数
,push_click_rate                -- 触达率
from tmp_operate t1
left join tmp_watch t2 on t1.d_date=t2.d_date and t1.country_code=t2.country_code and t1.lang=t2.lang
left join tmp_pay t3 on t1.d_date=t3.d_date::text and t1.country_code=t3.country_code and t1.lang=t3.lang
left join tmp_push t4 on t1.d_date = t4.push_date::text and t1.lang_name = t4.lang
left join tmp_refund t5 on t1.d_date = t5.d_date::text and t1.country_code = t5.country_code and t1.lang = t5.lang;
--从临时表导入主表
	truncate table public.dw_operate_schedule ;
	insert into public.dw_operate_schedule select * from tmp.dw_operate_schedule_tmp02;