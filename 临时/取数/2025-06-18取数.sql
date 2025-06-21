---------------------------------------------
-- File: 2025-06-18取数.sql
-- Time: 2025/6/18 10:24
-- User: xiaoj
-- Description:  
---------------------------------------------

------------------------------------------
--  参考表及分析
------------------------------------------
-- dw_operate_schedule
-- 维度：date country_code lang  其他伴随维度: country_name area lang_name

-- dw_core_indicators
-- 维度：week_start week_end country_name lang_name
-- 注意： 报表里过滤掉了最近7天的

-- dw_operate_view
-- 维度： date country_code lang 其他伴随维度：country_name area lang_name

------------------------------------------
--  目标
------------------------------------------
-- 取六月数据
-- 条件：剔除播放时长小于3秒的当日注册新用户后计算以下各个指标数据





set timezone ='UTC-0';
-- 需要排除的数据
with excluded_data as (
    select
        t1.v_date,
        t1.d_date :: date,
        t1.uid ::bigint
    from dwd_user_info t1
    left join  (
    -- 3月以后 每个用户每天观看时间
        SELECT
            to_timestamp(created_at)::date as d_date,
            uid,
            sum(case when event=2 then watch_time else 0 end) as watch_duration_sec
        FROM public.app_user_track_log a
        WHERE a.event = 2 AND a.vid > 0 AND a.eid > 0 AND to_timestamp(a.created_at)::date >= '2025-06-01'
        GROUP BY to_timestamp(a.created_at):: date,a.uid
    ) t2
        on t1.d_date::date = t2.d_date::date and t1.uid::bigint = t2.uid::bigint
    where t1.d_date>= '2025-06-01' and( watch_duration_sec is null or watch_duration_sec < 3)
    group by t1.d_date,t1.uid
),
new_reg_users as (
	select
	     t.v_date as created_date
	    ,t.d_date::date as d_date
	    ,t.uid::int8 as uid
	    ,country_code
	    ,lang
	    ,lang_name
	    ,is_campaign
        ,os
	from public.dwd_user_info t
	left join excluded_data ed on t.uid::bigint = ed.uid and t.d_date::date = ed.d_date
	where ed.uid is null -- 剔除这部分新用户
),
-- 排除后的dau
tmp_dau as(
	select  to_char(t.d_date,'yyyymmdd')::int as created_date ,t.uid::int8 , t.d_date
	from public.dwd_user_active t left join excluded_data t1 on t.d_date = t1.d_date and t.uid = t1.uid
	where t1.uid  is null and t.d_date >= '2025-03-01'::date
),
-- 取续订
-- 不用排除
tmp_subscription as(
		select t1.*
		  ,row_number() over(partition by t1.uid,t1.vip_days,t1.order_type2 order by t1.begin_date ) as rn -- 周卡第n次订阅/第n次续订
		  ,dense_rank() over(partition by t1.uid,t1.vip_days order by t1.order_id  ) as odrn -- 第n次周卡
		  from(
		    select t1.cs_order_id,t1.order_num,t1.vip_days,t1.status,t1.pay_type ,t1.money
		      ,t2.*
		      ,case when row_number() over(partition by t2.uid,t2.order_id order by t2.out_order_id)=1 then 4 else 5 end as order_type2
		      -- ,case when  t2.out_origin_order_id<>t2.out_order_id then 5 else 4 end as order_type
		      -- ,case when t1.vip_days is null then (case when product_id like '%week%' then 7 when product_id like '%month%' then 30 when product_id like '%quarter%' then 90 else 365 end) else t1.vip_days end as vip_days2
		    from(
		      select distinct t1.app_id,t1.cs_order_id,t1.order_num,t1.vip_days ,t1.status,t1.pay_type ,t1.money
		      from public."oversea-api_osd_order" t1
		      where environment=1 and order_type =4 -- and status in(1,3)
		      and to_timestamp(created_at)::date>='2024-07-01'
		      	-- and to_timestamp(created_at)::date >= (current_date+interval '-1 day')::date -- 增 不可用
		    )t1
		    right join(
		      select distinct product_id, uid ,order_id ,out_origin_order_id ,out_order_id  ,status as sub_status,payment_state -- 1正常2到期
		      ,to_timestamp(begin_time)::date::text as begin_date ,to_timestamp(end_time)::date::text as end_date
		      ,to_timestamp(end_time)::date-to_timestamp(begin_time)::date as diff_date
		      from public.middle_subscription m
		      where environment=1 and status>0
		      and to_timestamp(created_at)::date>='2024-07-01'
		      	-- and to_timestamp(created_at)::date >= (current_date+interval '-1 day')::date -- 增 不可用
		      -- order by order_id ,out_order_id
		    )t2 on t1.cs_order_id=t2.order_id
		    where t1.cs_order_id is not null -- 过滤问题数据
		    and t2.diff_date>=7 -- 过滤问题数据
		    and t1.app_id<>'osd13469466' -- 过滤官网数据
		    -- order by t2.uid,t2.order_id,t2.out_order_id
		  )t1
),
-- 取时间
tmp_primary as(
    select v_date::int ,d_date::date as d_date
    from analysis.dim_day where d_date between '2025-06-01' and '2025-06-17'
),
-- 未排除
user_pay_status AS (
    SELECT DISTINCT
        uid
        ,to_timestamp(created_at)::date as d_date
    FROM all_order_log
    WHERE to_timestamp(created_at)::date >= '2025-03-01'
    --   AND order_type IN (1, 4, 5)
      AND environment = 1
      AND status = 1
),
tmp_t0 as (
      select
          d_date
          , count(distinct uid) as excluded_uv
      from excluded_data
      group by d_date
),
tmp_t1 as (
    select
	     u1.created_date
	    ,count(distinct u1.uid) as dau
	    ,count(distinct u2.uid) as dau_2login
	    ,count(distinct u3.uid ) as dau_3login -- 用户第3留
	    ,count(distinct u7.uid ) as dau_7login -- 用户第7留
	    ,count(distinct case when (un.uid is null) then u1.uid else null end ) as old_dau -- 登录老用户数
	    ,count(distinct case when (un.uid is null) then u2.uid else null end ) as old_dau_2login -- 老用户第二日登录
	    ,count(distinct un.uid) as new_dau -- 新用户数
	    ,count(distinct n2.uid) as new_dau_2login -- 新用户第二日登录
	    ,count(distinct case when un.is_campaign=1 then un.uid else null end) as new_dau_campaign -- 新用户【推广量】
	    ,count(distinct case when un.is_campaign=0 then un.uid else null end) as new_dau_natural -- 新用户【自然量】
	    from tmp_dau u1
	    left join new_reg_users un on u1.uid=un.uid and u1.created_date=un.created_date
	    left join tmp_dau as n2 on un.uid = n2.uid  and un.d_date=(n2.d_date +interval '-1 day')::date -- and un.country_code=u2.country_code
	    left join tmp_dau u2 on u1.uid=u2.uid  and u1.d_date=(u2.d_date +interval '-1 day')::date  -- and u1.country_code=u2.country_code
	    left join tmp_dau u3 on u1.uid=u3.uid  and u1.d_date=(u3.d_date +interval '-3 day')::date   -- and u1.country_code=u3.country_code
	    left join tmp_dau u7 on u1.uid=u7.uid  and u1.d_date=(u7.d_date +interval '-7 day')::date    -- and u1.country_code=u7.country_code
	    where 1=1
	    group by u1.created_date
),
-- t3
tmp_t3 as (
     select
	         o.created_date
			,count(distinct o.order_num) as all_pay_order  -- 总订单数(包含失败)
			,count(distinct  o.uid ) as  all_pay_user  -- 总生成订单人数(包含失败)
			,sum(case when o.status = 1 then o.money else 0 end) as  pay_amt  -- 成功充值金额
			,count(distinct case when o.status = 1 then o.order_num else null end) as  pay_order  -- 成功充值订单数
			,count(distinct case when o.status = 1 then o.uid else null end) as  pay_user  -- 成功充值人数
			,count(distinct case when (o.status = 1 and length(coalesce(o.campaign_id,'')) >1) then o.uid else null end) as  pay_user_campaign  -- 成功充值人数【推广量】
			,count(distinct case when (o.status = 1 and length(coalesce(o.campaign_id,''))<=1) then o.uid else null end) as  pay_user_natural  -- 成功充值人数【自然量】
		    ,sum(case when (o.status = 1 and o.order_type = 1) then o.money else 0 end) as pay_k_amt  -- 充值K币金额
		    ,sum(case when (o.status = 1 and o.order_type in (4,5) and vip_days =   7) then o.money else 0 end) as pay_week_amt --- 充值周卡金额
		    ,sum(case when (o.status = 1 and o.order_type in (4,5) and vip_days =  30) then o.money else 0 end) as pay_month_amt --- 充值月卡金额
		    ,sum(case when (o.status = 1 and o.order_type in (4,5) and vip_days =  90) then o.money else 0 end) as pay_quarter_amt  --- 充值季卡金额
		    ,sum(case when (o.status = 1 and o.order_type in (4,5) and vip_days = 365) then o.money else 0 end) as pay_year_amt  -- 充值年卡金额
	        from public.all_order_log o
	        left join excluded_data ed on ed.uid = o.uid and ed.v_date = o.created_date -- 排除掉那部分
	        where 1=1 and ed.uid is null and o.environment = 1
	        and o.created_date>=20250301
	        	and to_char( to_timestamp(o.created_at),'YYYY-MM-DD') >= '2025-03-01'::date::text
	        group by o.created_date
),
-- t4
tmp_t4 as (
    select
        r.refund_date as created_date
		,sum(r.total_money) as pay_refund_amt -- 退款金额
		from public.all_refund_order_log r
		left join excluded_data ed on r.uid = ed.uid and r.refund_date = ed.v_date
		where ed.uid is null and r.environment = 1  and r.os in('android','ios')
		and r.status = 1
	    and r.refund_date>=20250301
	    and to_char( to_timestamp(r.refund_time),'YYYY-MM-DD') >= '2025-03-01'::date::text -- 增
		group by r.refund_date
),
tmp_t6 as (
    select cd.created_date
	        ,sum(case when ad_channel = 'tt' then cost_amount else 0 end) as ad_cost_tt    -- 【tt渠道消耗】
	        ,sum(case when ad_channel = 'fb' then cost_amount else 0 end) as ad_cost_fb    -- 【fb渠道消耗】
	        ,sum(case when ad_channel = 'apple' then cost_amount else 0 end) as ad_cost_asa    -- 【asa渠道消耗】
	        ,sum(case when ad_channel not in('tt','fb','apple') then cost_amount else 0 end) as ad_cost_other    -- 【小渠道消耗】
	        ,sum(cost_amount) as ad_cost -- 总渠道消耗
			from public.ad_cost_data_log cd -- 消耗明细表
			where 1=1
			and cd.account_id not in('3851320725139192','1248567319618926')
			and cd.created_date>=20250301
			group by cd.created_date
),
tmp_t7 as (
    select  a.created_date :: bigint as created_date
  			,sum(adin_amt) as ad_income_amt -- 商业化广告收入
			from public.dwd_adin_media_revenue a
			where 1=1
				and a.created_date::int >= to_char('2025-03-01'::date ,'yyyymmdd')::int  -- 增
			group by a.created_date
),
tmp_t31 as (
    select t1.begin_date ::date
			,count(distinct case when t1.order_type2=5 then t1.uid else null end) as repay_user -- 续订人数
			from tmp_subscription t1
			where 1=1
			and t1.begin_date>='2025-03-01'
			group by t1.begin_date
),
tmp_t32 as (
    select  t1.end_date ::date
			,count(distinct t1.uid) as due_user -- 到期人数
			from tmp_subscription t1
			where 1=1 and t1.end_date>='2025-03-01'
			group by t1.end_date
),
-- 来自运营月报表
tmp_watch as (
    select
         a.d_date::date  as d_date
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
		,a.uid
		,count(distinct a.vid) as vid_watch_cnt -- 每人看短剧数
		,count(distinct a.eid) as eid_watch_cnt -- 每人看剧集数
		,count(distinct case when e.sort >= c.pay_num then a.eid else null end) as eidpay_watch_cnt
		,count(distinct case when e.sort <  c.pay_num then a.eid else null end) as eidfree_watch_cnt
		,sum(case when a.event=2 then watch_time else 0 end) as watch_duration -- "看剧时长(分钟)"
		from public.app_user_track_log a
		left join excluded_data ed on a.uid = ed.uid and to_timestamp(a.created_at) :: date = ed.d_date
		left join "oversea-api_osd_videos" c on a.vid = c.id
		left join "oversea-api_osd_video_episodes" e on a.eid = e.id
		where 1=1
		and ed.uid is null -- 排除掉那部分
		and a.event in (1,2,13,14)
		and a.vid>0 and a.eid>0
		and to_timestamp(a.created_at) :: date>='2025-03-01'
		group by to_timestamp(a.created_at) :: date
		,a.uid
	)a
	group by a.d_date::date
),
-- 取做任务人数
tmp_task as (
    SELECT
        d_date,
        SUM(做任务人数) as task_doers
    FROM public.dw_rewards_view a
    GROUP BY d_date
),
-- 取 pre_pay_watch_uv_complete 和 free_episodes_watch_uv
tmp_daily_watch_summary as (
    SELECT
        TO_TIMESTAMP(a.created_at)::date AS d_date,
        COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) AND b.sort = (v1.pay_num - 1) THEN a.uid ELSE NULL END) AS pre_pay_watch_uv_complete,
        COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) AND b.sort < v1.pay_num THEN a.uid ELSE NULL END) AS free_episodes_watch_uv
    FROM public.app_user_track_log a
    INNER JOIN "oversea-api_osd_video_episodes" b ON a.eid = b.id
    LEFT JOIN public."oversea-api_osd_videos" v1 ON a.vid = v1.id
    left join excluded_data ed on a.uid = ed.uid and TO_TIMESTAMP(a.created_at)::date = ed.d_date
    WHERE
    ed.uid is not null and event IN (1, 2, 13, 14)
    AND a.vid = b.vid
    AND TO_TIMESTAMP(a.created_at)::date >= '2025-03-01'
    GROUP BY TO_TIMESTAMP(a.created_at)::date
),
tmp_unacitive_users as (
    select
        v_date,
        d_date,
        count(distinct case when stay_active is null then uid else null end) as stay_unactive_uv
    from(
        select
            t.v_date,
            t.d_date,
            t.uid,
            max(t0.uid) as stay_active
        from excluded_data t
        left join public.dwd_user_active t0 on t.d_date + 1 <= t0.d_date  and t0.d_date <= t.d_date + 7 and t0.uid = t.uid
        group by t.v_date, t.d_date, t.uid
    ) t
    group by t.v_date, t.d_date
)
select
    t0.d_date as "日期",
    sum(t3.pay_amt)/100 as "充值金额",
    sum(t7.ad_income_amt) as "广告收入",
    (sum(t3.pay_amt))/100 + sum(t7.ad_income_amt) as "收入",
    sum(tmp_t0.excluded_uv) as "排除用户数量",
    sum(tmp_unacitive_users.stay_unactive_uv) as "后7天未活跃人数",
    sum(t1.dau) as dau,
    sum(t1.new_dau) as 新用户数,
    1.0*sum(tmp_t0.excluded_uv)/(sum(t1.new_dau)+sum(tmp_t0.excluded_uv)) as 不活跃新用户数占新用户数占比,
    case when sum(t6.ad_cost) = 0 then 0 else 1.0*(sum(t3.pay_amt) + sum(t7.ad_income_amt) - sum(t4.pay_refund_amt)) / sum(t6.ad_cost) end as "总roi",
    case when sum(t1.dau) = 0 then 0 else 1.0* sum(t3.pay_user)/sum(t1.dau) end as "总付费率",
    case when sum(t3.pay_user) = 0 then 0 else 1.0*sum(t3.pay_order)/sum(t3.pay_user) end as "总人均付费次数",
    case when sum(t1.dau) = 0 then 0 else 1.0*sum(t3.pay_amt)/100/sum(t1.dau) end  as "ARPU",
    case when sum(t3.pay_user) = 0 then 0 else 1.0*sum(t3.pay_amt)/100/sum(t3.pay_user)end  as"总客单价",
    case when sum(t3.pay_amt) + sum(t7.ad_income_amt) = 0 then 0 else 1.0*sum(t7.ad_income_amt) /(sum(t3.pay_amt) + sum(t7.ad_income_amt)) end as "商业化广告收入占比",
    case when sum(t32.due_user) = 0 then 0 else 1.0*sum(t31.repay_user)/sum(t32.due_user) end as "订阅续订率",
    case when sum(t1.new_dau) = 0 then 0 else 1.0*sum(t1.new_dau_2login)/sum(t1.new_dau) end as "新用户次日留存率",
    case when sum(t1.dau) = 0 then 0 else 1.0*sum(t1.dau_2login)/sum(t1.dau) end as "总次留",
    case when sum(t1.dau) = 0 then 0 else 1.0*sum(t1.dau_3login)/sum(t1.dau) end as "总3留",
    case when sum(t1.dau) = 0 then 0 else 1.0*sum(t1.dau_7login)/sum(t1.dau) end as "总7留",
    case when sum(t1.dau) = 0 then 0 else 1.0*sum(tt.task_doers)/sum(t1.dau) end as "任务参与率",
    case when sum(tds.free_episodes_watch_uv)  = 0 then 0 else 1.0*sum(tds.pre_pay_watch_uv_complete)/sum(tds.free_episodes_watch_uv) end as "卡点前完播率",
    case when sum(tw.watch_user) = 0 then 0 else 1.0*sum(tw.eid_watch_cnt) /  sum(tw.watch_user) end as "人均看剧集数",
    case when sum(tw.watch_user) = 0 then 0 else 1.0*sum(tw.eidpay_watch_user) /  sum(tw.watch_user) end as "付费集播放率",
    case when sum(tw.watch_user) = 0 then 0 else 1.0*sum(tw.eidfree_watch_cnt) /  sum(tw.watch_user) end as "付费集播放数"
from tmp_primary t0
left join tmp_t0 on t0.d_date = tmp_t0.d_date
left join tmp_unacitive_users on t0.d_date = tmp_unacitive_users.d_date
left join tmp_t1 t1 on t0.v_date = t1.created_date
left join tmp_t3 t3 on t0.v_date = t3.created_date
left join tmp_t4 t4 on t0.v_date = t4.created_date
left join tmp_t6 t6 on t0.v_date = t6.created_date
left join tmp_t7 t7 on t0.v_date = t7.created_date
left join tmp_t31 t31 on t0.d_date = t31.begin_date::date
left join tmp_t32 t32 on t0.d_date = t32.end_date::date
left join tmp_watch tw on t0.d_date = tw.d_date
left join tmp_task tt on t0.d_date = tt.d_date
left join tmp_daily_watch_summary tds on t0.d_date = tds.d_date
where t0.d_date >= '2025-06-01'::date  -- and t0.d_date <= '2025-04-30'::date
group by t0.d_date;


-- select count(*) from public.dwd_user_active where d_date = '2025-06-10';


with excluded_data as (
    select
        t1.v_date,
        t1.d_date :: date,
        t1.uid ::bigint,
        t1.user_source,
        t1.ad_channel,
        t1.lang_name,
        t1.country_name
    from dwd_user_info t1
    left join  (
    -- 3月以后 每个用户每天观看时间
        SELECT
            to_timestamp(created_at)::date as d_date,
            uid,
            sum(case when event=2 then watch_time else 0 end) as watch_duration_sec
        FROM public.app_user_track_log a
        WHERE a.event = 2 AND a.vid > 0 AND a.eid > 0 AND to_timestamp(a.created_at)::date >= '2025-06-01'
        GROUP BY to_timestamp(a.created_at):: date,a.uid
    ) t2
        on t1.d_date::date = t2.d_date::date and t1.uid::bigint = t2.uid::bigint
    where t1.d_date>= '2025-06-01' and( watch_duration_sec is null or watch_duration_sec < 3)
    group by t1.d_date,t1.uid
),
new_reg_users as (
	select
	     t.v_date as created_date
	    , t.d_date::date as d_date
	    , t.uid::int8 as uid
	    , t.country_name
	    , t.lang_name
	    , t.user_source
	    , t.ad_channel
	from public.dwd_user_info t
	left join excluded_data ed on t.uid::bigint = ed.uid and t.d_date::date = ed.d_date
	where ed.uid is null -- 剔除这部分新用户
),
tmp_excluded_users as (
    select
        d_date,
        user_source,
        ad_channel,
        lang_name,
        country_name,
        count(distinct uid) as excluded_uv
    from excluded_data
    group by d_date,user_source,ad_channel,lang_name,country_name
),
tmp_unacitive_users as (
    select
        v_date,
        d_date,
        country_name,
        lang_name,
        ad_channel,
        user_source,
        count(distinct case when stay_active is null then uid else null end) as stay_unactive_uv
    from(
        select
            t.v_date,
            t.d_date,
            t.uid,
            t.country_name,
            t.lang_name,
            t.ad_channel,
            t.user_source,
            max(t0.uid) as stay_active
        from excluded_data t
        left join public.dwd_user_active t0 on t.d_date + 1 <= t0.d_date  and t0.d_date <= t.d_date + 7 and t0.uid = t.uid
        group by t.v_date, t.d_date, t.uid, t.country_name,t.lang_name,t.ad_channel,t.user_source
    ) t
    group by t.v_date, t.d_date, country_name,lang_name,ad_channel,user_source
)
select
    t.d_date as 日期,
    t.country_name as 国家,
    t.lang_name as 语言,
    t.ad_channel as 渠道,
    t.user_source as 是否是推广流,
    excluded_uv as 排除用户数,
    stay_unactive_uv as 后七天不活跃用户数
from tmp_excluded_users t
left join tmp_unacitive_users t0
    on t.d_date = t0.d_date and t.user_source = t0.user_source and t.country_name = t0.country_name and t.lang_name=t0.lang_name and t.ad_channel = t0.ad_channel