------------------------------------------
-- file: 剧报表_指标概览端内表现_全量_d.sql
-- author: xiaoj
-- time: 2025/5/16 18:18
-- description:
------------------------------------------
set timezone ='UTC-0';



	-- K币消耗
	-- consume AS (
	truncate table tmp.dw_video_view_calc01_consume;
	insert into tmp.dw_video_view_calc01_consume
	with newuser_tb as( ------ 新增用户
		select uid
		,d_date,v_date,country_code,ad_channel
		from public.dwd_user_info
	)
    SELECT
        TO_TIMESTAMP(created_at) :: DATE AS dt,
        goods_id AS vid,
        country_code,
        ad_channel,
        SUM(money) * 1.0::int8 AS "总消耗K币",
        SUM(use_recharge_balance) * 1.0::int8 AS "消耗充值K币",
        SUM(use_balance_give) * 1.0::int8 AS "消耗赠送K币",
        SUM(CASE WHEN is_new_user = 1 THEN money ELSE 0 END) * 1.0::int8 AS "新用户总消耗K币",
        SUM(CASE WHEN is_new_user = 1 THEN use_recharge_balance ELSE 0 END) * 1.0::int8 AS "新用户消耗充值K币",
        SUM(CASE WHEN is_new_user = 1 THEN use_balance_give ELSE 0 END) * 1.0::int8 AS "新用户消耗赠送K币",
        SUM(CASE WHEN is_new_user <> 1 THEN money ELSE 0 END) * 1.0::int8 AS "老用户总消耗K币",
        SUM(CASE WHEN is_new_user <> 1 THEN use_recharge_balance ELSE 0 END) * 1.0::int8 AS "老用户消耗充值K币",
        SUM(CASE WHEN is_new_user <> 1 THEN use_balance_give ELSE 0 END) * 1.0::int8 AS "老用户消耗赠送K币"
    FROM (
        SELECT t.*,
            CASE
                WHEN TO_TIMESTAMP(t.created_at) :: DATE::text = b.d_date THEN 1
                ELSE 0
            END AS is_new_user,
            COALESCE(b.country_code, 'UNKNOWN') AS country_code,
            COALESCE(b.ad_channel, '未知') AS ad_channel
        FROM (
            SELECT * FROM public.middle_user_consume_record_00
            UNION ALL
            SELECT * FROM public.middle_user_consume_record_01
            UNION ALL
            SELECT * FROM public.middle_user_consume_record_02
            UNION ALL
            SELECT * FROM public.middle_user_consume_record_03
            UNION ALL
            SELECT * FROM public.middle_user_consume_record_04
        ) t
        LEFT JOIN newuser_tb b ON t.uid::text = b.uid
        WHERE type = 0
        AND TO_TIMESTAMP(created_at) :: DATE  >= '2024-07-01'
        -- and to_timestamp(created_at)::date >= (current_date+interval'-2 day')::date
    ) t
    GROUP BY TO_TIMESTAMP(created_at) :: DATE, goods_id, country_code, ad_channel
	;


    --   set timezone ='UTC-0';
		-- 用户看剧行为 （与新用户ROI合并，避免歧义不区分新老）
		-- create table tmp.dw_video_view_calc01_vidlog as
		truncate table tmp.dw_video_view_calc01_vidlog;
		insert into tmp.dw_video_view_calc01_vidlog
		with newuser_tb as( ------ 新增用户
			select uid
			,d_date,v_date,country_code,ad_channel
			from public.dwd_user_info
		)
			select
			 n1.d_date
			,coalesce(nn.country_code,'UNKNOWN') as country_code
			,coalesce(nn.ad_channel,'未知') as ad_channel
			,n1.vid::text as vid
			,sum(case when n1.watch_pv>0 then 1 else 0 end)::int8 as watch_uv
			,sum(n1.watch_pv)::int8 as watch_pv
			,round(sum(n1.watch_duration)/60.0,2)::DECIMAL(20,4) as watch_duration
			,sum(n1.eid_watch_cnt)::int8 as eid_watch_cnt
			,sum(case when n1.ad_unlock_pv>0 then 1 else 0 end)::int8 as ad_unlock_uv
			,sum(n1.ad_unlock_pv)::int8 as ad_unlock_pv
			,sum(case when n1.watch_pv>0 and n1.d_date = nn.d_date::date then 1 else 0 end)::int8 as new_watch_uv
            ,sum(case when n1.watch_pv>0 and n1.d_date!= nn.d_date::date then 1 else 0 end)::int8 as old_watch_uv
			,sum(case when n1.eid_2>0 then 1 else 0 end) as eid_2_watch_uv
			,sum(case when n1.pre_pay_watch_uv>0 then 1 else 0 end) as pre_pay_watch_uv
			from(
			    select
			     to_timestamp(a.created_at)::date as d_date
			    ,a.vid
			    ,a.uid
			    ,count(case when event in(1,2,13,14) and eid>0 then uid else null end) as watch_pv
				,count(distinct case when event in(1,2,13,14) and b.sort = 2 then uid else null end) as eid_2
			    ,count(distinct case when event in(1,2,13,14) and b.sort = (v1.pay_num - 1) then uid else null end) as pre_pay_watch_uv
			    ,sum(case when event=2 and eid>0 then watch_time else 0 end) as watch_duration
			    ,count(distinct case when event in(1,2,13,14) and eid>0 then eid else null end) as eid_watch_cnt -- 每人看短剧集数
			    ,count(case when event=6 then uid else null end) as ad_unlock_pv
			    from public.app_user_track_log a inner join "oversea-api_osd_video_episodes" b on a.eid = b.id
				left join public."oversea-api_osd_videos" v1 on a.vid=v1.id
			    where 1=1
			    and event in(1,2,13,14 ,6)
                and a.vid = b.vid
			    and to_timestamp(a.created_at)::date>='2024-07-01'
			    	-- and to_timestamp(a.created_at)::date >= (current_date+interval'-1 day')::date -- 增
			    group by
			     to_timestamp(a.created_at)::date
			    ,a.vid
			    ,a.uid

				-- in (1,2,13,14)
				-- 2 离开看剧页面的时候上报，统计观看时长	 drama_watch_time
				-- 13 向下滑动页面	drama_scroll_down
				-- 14 向上滑动页面	drama_scroll_up
				-- 1 进入短剧观看页面就上报	drama_watch
				-- 6 短剧观看页面完成广告任务点击领取奖励	drama_adpopup_claim
				--
			)n1
			left join newuser_tb nn on  n1.uid::text=nn.uid
			group by
			 n1.d_date
			,coalesce(nn.country_code,'UNKNOWN')
			,coalesce(nn.ad_channel,'未知')
			,n1.vid
		;



-- set timezone ='UTC-0';
	-- 前置脚本（端内表现）1
	truncate table analysis.dw_video_view_calc01;
	insert into analysis.dw_video_view_calc01
		with newuser_tb as( ------ 新增用户
			select uid
			,d_date,v_date,country_code,ad_channel
			,vid
			from public.dwd_user_info

		)
		,tmp_vid_pay as(
			SELECT
            p1.d_date,
            coalesce(nn.country_code,'UNKNOWN') as country_code,
            coalesce(nn.ad_channel,'未知') as ad_channel,
            p1.vid::text as vid,
            count(distinct p1.uid) as pay_uv,
            count(distinct case when nu.uid is not null then p1.uid else null end) as new_pay_uv,
            count(distinct case when nu.uid is null then p1.uid else null end) as old_pay_uv,
            sum(p1.pay_amt) as pay_amt,
            -- 修改部分：使用新的归因逻辑
            sum(case when p1.vid::bigint = b.vid::bigint then p1.pay_amt else null end) as attributed_pay_amt,
            sum(case when nu.uid is not null then p1.pay_amt else 0 end) as new_pay_amt,
            sum(case when nu.uid is null then p1.pay_amt else 0 end) as old_pay_amt,
            sum(p1.pay_k_amt) as pay_k_amt,
            sum(p1.pay_vip_amt) as pay_vip_amt
        FROM (
            SELECT
                to_timestamp(created_at)::date as d_date,
                goods_id as vid,
                uid,
                sum(money)*0.01 as pay_amt,
                sum(case when order_type = 1 then money else 0 end)*0.01 as pay_k_amt,
                sum(case when order_type in(4,5) then money else 0 end)*0.01 as pay_vip_amt
            FROM
                public.all_order_log
            WHERE
                status = 1 and environment = 1
                AND to_timestamp(created_at)::date >= '2024-07-01'
            GROUP BY
                to_timestamp(created_at)::date,
                goods_id,
                uid
        ) p1
        LEFT JOIN newuser_tb nn ON p1.uid::text = nn.uid
        LEFT JOIN newuser_tb nu ON p1.uid::text = nu.uid and p1.d_date = nu.d_date::date
        -- 修改部分：添加新的左连接
        LEFT JOIN (
            SELECT DISTINCT
                CASE
                    WHEN vid ~ '^\d+$' THEN vid
                    ELSE NULL
                END AS vid,
                uid
            FROM
                dwd_user_info
        ) b ON p1.uid = b.uid::bigint
        -- WHERE
            -- 修改部分：添加新的筛选条件
            -- p1.d_date BETWEEN '2025-04-27' AND '2025-04-27'
            -- AND b.d_date >= '2025-04-15'
            -- AND p1.vid = 7260
        GROUP BY
            p1.d_date,
            coalesce(nn.country_code,'UNKNOWN'),
            coalesce(nn.ad_channel,'未知'),
            p1.vid

		)
		,tmp_primary as(
			SELECT DISTINCT d_date, country_code, ad_channel, vid FROM (
				SELECT DISTINCT d_date, country_code, ad_channel, vid FROM tmp.dw_video_view_calc01_vidlog
				UNION ALL
				SELECT DISTINCT d_date, country_code, ad_channel, vid FROM tmp_vid_pay
				UNION ALL
				SELECT DISTINCT dt AS d_date, country_code, ad_channel, cast(vid as varchar) FROM tmp.dw_video_view_calc01_consume
			) tb
			where 1=1
			-- and tb.d_date >= (current_date+interval'-2 day')::date -- 增
		)

SELECT
    t0.vid,
    t0.d_date,
    t0.country_code,
    t0.ad_channel,
    cc.country_name,
    cc.area,
    COALESCE(t1.watch_uv, 0) AS watch_uv,
    COALESCE(t1.watch_pv, 0) AS watch_pv,
    COALESCE(t1.watch_duration, 0) AS watch_duration,
    COALESCE(t1.eid_watch_cnt, 0) AS eid_watch_cnt,
    COALESCE(t1.ad_unlock_uv, 0) AS ad_unlock_uv,
    COALESCE(t1.ad_unlock_pv, 0) AS ad_unlock_pv,
    COALESCE(t2.pay_uv, 0) AS pay_uv,
    COALESCE(t2.pay_amt, 0) AS pay_amt,
    COALESCE(t2.pay_k_amt, 0) AS pay_k_amt,
    COALESCE(t2.pay_vip_amt, 0) AS pay_vip_amt,
    COALESCE(c."总消耗K币", 0) AS "总消耗K币",
    COALESCE(c."消耗充值K币", 0) AS "消耗充值K币",
    COALESCE(c."消耗赠送K币", 0) AS "消耗赠送K币",
    COALESCE(c."新用户总消耗K币", 0) AS "新用户总消耗K币",
    COALESCE(c."新用户消耗充值K币", 0) AS "新用户消耗充值K币",
    COALESCE(c."新用户消耗赠送K币", 0) AS "新用户消耗赠送K币",
    COALESCE(c."老用户总消耗K币", 0) AS "老用户总消耗K币",
    COALESCE(c."老用户消耗充值K币", 0) AS "老用户消耗充值K币",
    COALESCE(c."老用户消耗赠送K币", 0) AS "老用户消耗赠送K币"
	,COALESCE(t1.new_watch_uv, 0) AS new_watch_uv
	,COALESCE(t1.old_watch_uv, 0) AS old_watch_uv
	,COALESCE(t2.new_pay_uv, 0) AS new_pay_uv
	,COALESCE(t2.old_pay_uv, 0) AS old_pay_uv
	,COALESCE(t2.new_pay_amt, 0) AS new_pay_amt
	,COALESCE(t2.old_pay_amt, 0) AS old_pay_amt
	,COALESCE(t1.eid_2_watch_uv, 0) AS eid_2_watch_uv
	,COALESCE(t1.pre_pay_watch_uv, 0) AS pre_pay_watch_uv
	,COALESCE(t2.attributed_pay_amt, 0) AS attributed_pay_amt
FROM tmp_primary t0
LEFT JOIN tmp.dw_video_view_calc01_vidlog  t1 ON t0.d_date = t1.d_date AND t0.country_code = t1.country_code AND t0.ad_channel = t1.ad_channel AND t0.vid = t1.vid
LEFT JOIN tmp_vid_pay t2 ON t0.d_date = t2.d_date AND t0.country_code = t2.country_code AND t0.ad_channel = t2.ad_channel AND t0.vid = t2.vid
LEFT JOIN tmp.dw_video_view_calc01_consume c      ON t0.d_date = c.dt AND t0.country_code = c.country_code AND t0.ad_channel = c.ad_channel AND t0.vid = cast(c.vid as varchar)
--ON t0.d_date = c.dt AND t0.vid = cast(c.vid as varchar)
LEFT JOIN v_dim_country_area cc ON t0.country_code = cc.country_code;


