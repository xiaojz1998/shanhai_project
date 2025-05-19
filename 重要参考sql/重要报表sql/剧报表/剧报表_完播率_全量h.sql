------------------------------------------
-- file: 剧报表_完播率_全量h.sql
-- author: xiaoj
-- time: 2025/5/16 18:15
-- description:
------------------------------------------
set timezone ='UTC-0';



	-- 播放全量
		truncate table analysis.dw_video_play_tmp01;
		insert into analysis.dw_video_play_tmp01
with newuser_tb as(
			select
--          v_date as created_date
            d_date::date as d_date
           ,country_code
           ,ad_channel
           ,upper(put_system) as put_system
           ,ad_format
           ,uid::int8 as uid
           ,split_part(campaign_name,'_',5)::text as vid
           ,campaign_id,campaign_name
            from public.dwd_user_info
	    )
	    ,tmp_vid_watch as(
		    select
		     e0.vid::text as vid
	        ,coalesce(nn.country_code,'UNKNOWN') as country_code
	        ,coalesce(nn.ad_channel,'未知') as ad_channel
	        ,count(distinct e0.uid) as vid_uv,
	        -- 集衰减
		    	count(distinct case when e1.sort = 1 then e0.uid else null end) as eid_1,
		        count(distinct case when e1.sort = 2 then e0.uid else null end) as eid_2,
		        count(distinct case when e1.sort = 3 then e0.uid else null end) as eid_3,
		        count(distinct case when e1.sort = 4 then e0.uid else null end) as eid_4,
		        count(distinct case when e1.sort = 5 then e0.uid else null end) as eid_5,
		        count(distinct case when e1.sort = 6 then e0.uid else null end) as eid_6,
		        count(distinct case when e1.sort = 7 then e0.uid else null end) as eid_7,
		        count(distinct case when e1.sort = 8 then e0.uid else null end) as eid_8,
		        count(distinct case when e1.sort = 9 then e0.uid else null end) as eid_9,
		        count(distinct case when e1.sort = 10 then e0.uid else null end) as eid_10,
		        count(distinct case when e1.sort = 11 then e0.uid else null end) as eid_11,
		        count(distinct case when e1.sort = 12 then e0.uid else null end) as eid_12,
		        count(distinct case when e1.sort = 13 then e0.uid else null end) as eid_13,
		        count(distinct case when e1.sort = 14 then e0.uid else null end) as eid_14,
		        count(distinct case when e1.sort = 15 then e0.uid else null end) as eid_15,
		        count(distinct case when e1.sort = 16 then e0.uid else null end) as eid_16,
		        count(distinct case when e1.sort = 17 then e0.uid else null end) as eid_17,
		        count(distinct case when e1.sort = 18 then e0.uid else null end) as eid_18,
		        count(distinct case when e1.sort = 19 then e0.uid else null end) as eid_19,
		        count(distinct case when e1.sort = 20 then e0.uid else null end) as eid_20,
		        count(distinct case when e1.sort = 21 then e0.uid else null end) as eid_21,
		        count(distinct case when e1.sort = 22 then e0.uid else null end) as eid_22,
		        count(distinct case when e1.sort = 23 then e0.uid else null end) as eid_23,
		        count(distinct case when e1.sort = 24 then e0.uid else null end) as eid_24,
		        count(distinct case when e1.sort = 25 then e0.uid else null end) as eid_25,
		        count(distinct case when e1.sort = 26 then e0.uid else null end) as eid_26,
		        count(distinct case when e1.sort = 27 then e0.uid else null end) as eid_27,
		        count(distinct case when e1.sort = 28 then e0.uid else null end) as eid_28,
		        count(distinct case when e1.sort = 29 then e0.uid else null end) as eid_29,
		        count(distinct case when e1.sort = 30 then e0.uid else null end) as eid_30,
		        count(distinct case when e1.sort = 31 then e0.uid else null end) as eid_31,
		        count(distinct case when e1.sort = 32 then e0.uid else null end) as eid_32,
		        count(distinct case when e1.sort = 33 then e0.uid else null end) as eid_33,
		        count(distinct case when e1.sort = 34 then e0.uid else null end) as eid_34,
		        count(distinct case when e1.sort = 35 then e0.uid else null end) as eid_35,
		        count(distinct case when e1.sort = 36 then e0.uid else null end) as eid_36,
		        count(distinct case when e1.sort = 37 then e0.uid else null end) as eid_37,
		        count(distinct case when e1.sort = 38 then e0.uid else null end) as eid_38,
		        count(distinct case when e1.sort = 39 then e0.uid else null end) as eid_39,
		        count(distinct case when e1.sort = 40 then e0.uid else null end) as eid_40,
		        count(distinct case when e1.sort = 41 then e0.uid else null end) as eid_41,
		        count(distinct case when e1.sort = 42 then e0.uid else null end) as eid_42,
		        count(distinct case when e1.sort = 43 then e0.uid else null end) as eid_43,
		        count(distinct case when e1.sort = 44 then e0.uid else null end) as eid_44,
		        count(distinct case when e1.sort = 45 then e0.uid else null end) as eid_45,
		        count(distinct case when e1.sort = 46 then e0.uid else null end) as eid_46,
		        count(distinct case when e1.sort = 47 then e0.uid else null end) as eid_47,
		        count(distinct case when e1.sort = 48 then e0.uid else null end) as eid_48,
		        count(distinct case when e1.sort = 49 then e0.uid else null end) as eid_49,
		        count(distinct case when e1.sort = 50 then e0.uid else null end) as eid_50,
		        count(distinct case when e1.sort = 51 then e0.uid else null end) as eid_51,
		        count(distinct case when e1.sort = 52 then e0.uid else null end) as eid_52,
		        count(distinct case when e1.sort = 53 then e0.uid else null end) as eid_53,
		        count(distinct case when e1.sort = 54 then e0.uid else null end) as eid_54,
		        count(distinct case when e1.sort = 55 then e0.uid else null end) as eid_55,
		        count(distinct case when e1.sort = 56 then e0.uid else null end) as eid_56,
		        count(distinct case when e1.sort = 57 then e0.uid else null end) as eid_57,
		        count(distinct case when e1.sort = 58 then e0.uid else null end) as eid_58,
		        count(distinct case when e1.sort = 59 then e0.uid else null end) as eid_59,
		        count(distinct case when e1.sort = 60 then e0.uid else null end) as eid_60,
		        count(distinct case when e1.sort = 61 then e0.uid else null end) as eid_61,
		        count(distinct case when e1.sort = 62 then e0.uid else null end) as eid_62,
		        count(distinct case when e1.sort = 63 then e0.uid else null end) as eid_63,
		        count(distinct case when e1.sort = 64 then e0.uid else null end) as eid_64,
		        count(distinct case when e1.sort = 65 then e0.uid else null end) as eid_65,
		        count(distinct case when e1.sort = 66 then e0.uid else null end) as eid_66,
		        count(distinct case when e1.sort = 67 then e0.uid else null end) as eid_67,
		        count(distinct case when e1.sort = 68 then e0.uid else null end) as eid_68,
		        count(distinct case when e1.sort = 69 then e0.uid else null end) as eid_69,
		        count(distinct case when e1.sort = 70 then e0.uid else null end) as eid_70,
		        count(distinct case when e1.sort = 71 then e0.uid else null end) as eid_71,
		        count(distinct case when e1.sort = 72 then e0.uid else null end) as eid_72,
		        count(distinct case when e1.sort = 73 then e0.uid else null end) as eid_73,
		        count(distinct case when e1.sort = 74 then e0.uid else null end) as eid_74,
		        count(distinct case when e1.sort = 75 then e0.uid else null end) as eid_75,
		        count(distinct case when e1.sort = 76 then e0.uid else null end) as eid_76,
		        count(distinct case when e1.sort = 77 then e0.uid else null end) as eid_77,
		        count(distinct case when e1.sort = 78 then e0.uid else null end) as eid_78,
		        count(distinct case when e1.sort = 79 then e0.uid else null end) as eid_79,
		        count(distinct case when e1.sort = 80 then e0.uid else null end) as eid_80,
		        count(distinct case when e1.sort = 81 then e0.uid else null end) as eid_81,
		        count(distinct case when e1.sort = 82 then e0.uid else null end) as eid_82,
		        count(distinct case when e1.sort = 83 then e0.uid else null end) as eid_83,
		        count(distinct case when e1.sort = 84 then e0.uid else null end) as eid_84,
		        count(distinct case when e1.sort = 85 then e0.uid else null end) as eid_85,
		        count(distinct case when e1.sort = 86 then e0.uid else null end) as eid_86,
		        count(distinct case when e1.sort = 87 then e0.uid else null end) as eid_87,
		        count(distinct case when e1.sort = 88 then e0.uid else null end) as eid_88,
		        count(distinct case when e1.sort = 89 then e0.uid else null end) as eid_89,
		        count(distinct case when e1.sort = 90 then e0.uid else null end) as eid_90,
		        count(distinct case when e1.sort = 91 then e0.uid else null end) as eid_91,
		        count(distinct case when e1.sort = 92 then e0.uid else null end) as eid_92,
		        count(distinct case when e1.sort = 93 then e0.uid else null end) as eid_93,
		        count(distinct case when e1.sort = 94 then e0.uid else null end) as eid_94,
		        count(distinct case when e1.sort = 95 then e0.uid else null end) as eid_95,
		        count(distinct case when e1.sort = 96 then e0.uid else null end) as eid_96,
		        count(distinct case when e1.sort = 97 then e0.uid else null end) as eid_97,
		        count(distinct case when e1.sort = 98 then e0.uid else null end) as eid_98,
		        count(distinct case when e1.sort = 99 then e0.uid else null end) as eid_99,
		        count(distinct case when e1.sort = 100 then e0.uid else null end) as eid_100,
		        count(distinct case when e1.sort = 101 then e0.uid else null end) as eid_101,
		        count(distinct case when e1.sort = 102 then e0.uid else null end) as eid_102,
		        count(distinct case when e1.sort = 103 then e0.uid else null end) as eid_103,
		        count(distinct case when e1.sort = 104 then e0.uid else null end) as eid_104,
		        count(distinct case when e1.sort = 105 then e0.uid else null end) as eid_105,
		        count(distinct case when e1.sort = 106 then e0.uid else null end) as eid_106,
		        count(distinct case when e1.sort = 107 then e0.uid else null end) as eid_107,
		        count(distinct case when e1.sort = 108 then e0.uid else null end) as eid_108,
		        count(distinct case when e1.sort = 109 then e0.uid else null end) as eid_109,
		        count(distinct case when e1.sort = 110 then e0.uid else null end) as eid_110,
		        count(distinct case when e1.sort = 111 then e0.uid else null end) as eid_111,
		        count(distinct case when e1.sort = 112 then e0.uid else null end) as eid_112,
		        count(distinct case when e1.sort = 113 then e0.uid else null end) as eid_113,
		        count(distinct case when e1.sort = 114 then e0.uid else null end) as eid_114,
		        count(distinct case when e1.sort = 115 then e0.uid else null end) as eid_115,
		        count(distinct case when e1.sort = 116 then e0.uid else null end) as eid_116,
		        count(distinct case when e1.sort = 117 then e0.uid else null end) as eid_117,
		        count(distinct case when e1.sort = 118 then e0.uid else null end) as eid_118,
		        count(distinct case when e1.sort = 119 then e0.uid else null end) as eid_119,
		        count(distinct case when e1.sort = 120 then e0.uid else null end) as eid_120,
		        count(distinct case when e1.sort = 121 then e0.uid else null end) as eid_121,
		        count(distinct case when e1.sort = 122 then e0.uid else null end) as eid_122,
		        count(distinct case when e1.sort = 123 then e0.uid else null end) as eid_123,
		        count(distinct case when e1.sort = 124 then e0.uid else null end) as eid_124,
		        count(distinct case when e1.sort = 125 then e0.uid else null end) as eid_125,
		        count(distinct case when e1.sort = 126 then e0.uid else null end) as eid_126,
		        count(distinct case when e1.sort = 127 then e0.uid else null end) as eid_127,
		        count(distinct case when e1.sort = 128 then e0.uid else null end) as eid_128,
		        count(distinct case when e1.sort = 129 then e0.uid else null end) as eid_129
		    from(
		        select uid -- 历史所有观看用户，查询压力大
	            ,vid, eid
		        from public.app_user_track_log
		        where 1=1
		        and event in(2,13,14 ,1,87)
		        and vid>0 and eid>0
		        and  created_date>=20241020
		        	-- and to_timestamp(created_at)::date>= (current_date+interval '-1 day')::date -- 增
		        group by uid
	            ,vid, eid

				-- event in (2,13,14)
				-- 2 离开看剧页面的时候上报，统计观看时长	 drama_watch_time
				-- 13 向下滑动页面	drama_scroll_down
				-- 14 向上滑动页面	drama_scroll_up
				-- 1 进入短剧观看页面就上报	drama_watch
				-- 6 短剧观看页面完成广告任务点击领取奖励	drama_adpopup_claim
				--
		    )e0
		    left join newuser_tb nn on e0.uid=nn.uid
		    left join "oversea-api_osd_video_episodes" e1 on e0.eid=e1.id
			where e0.vid=e1.vid
		    group by
		     e0.vid
	        ,coalesce(nn.country_code,'UNKNOWN')
	        ,coalesce(nn.ad_channel,'未知')
		)
		-----剧的首充率、复充率、三充及以上
--		,tmp_vid_pay as(
--		    select
--		     e0.vid::text as vid
--	        ,coalesce(nn.country_code,'UNKNOWN') as country_code
--	        ,coalesce(nn.ad_channel,'未知') as ad_channel
--	        ,sum(e0.pay_amt) as pay_amt
--	        ,sum(e0.pay_order) as pay_order
--	        ,sum(e0.pay_cnt) as pay_cnt -- 充值次数
--	        ,count(distinct e0.uid) as pay_uv
--	        ,count(distinct case when e0.pay_cnt  = 1 then e0.uid else null end) as pay_1cnt_uv
--	        ,count(distinct case when e0.pay_cnt  = 2 then e0.uid else null end) as pay_2cnt_uv
--	        ,count(distinct case when e0.pay_cnt >= 3 then e0.uid else null end) as pay_3cnt_uv
--		    from(
--		        select goods_id as vid
--	            ,uid
--		        ,sum(money)*0.01 as pay_amt
--	            ,count(distinct order_num) as pay_order
--	            ,count(distinct (cast(order_num as varchar) || cast(order_type as varchar) || cast(created_at as varchar))) as pay_cnt -- 支付次数（包含续订）
--		        from public.all_order_log
--		        where status = 1 and environment = 1
--		        and goods_id <> 0
--		        and created_date>=20240701
--		        	-- and to_timestamp(created_at)::date >= (current_date+interval '-1 day')::date -- 增
--					AND to_timestamp(created_at) < date_trunc('hour', now() - interval '1 hour') ---截止上个小时整点之前
--		        group by goods_id
--	            ,uid
--		    )e0
--		    left join newuser_tb nn on e0.uid=nn.uid
--		    group by
--		     e0.vid
--	        ,coalesce(nn.country_code,'UNKNOWN')
--	        ,coalesce(nn.ad_channel,'未知')
--		)
--		,tmp_primary as(
--			select distinct vid,country_code,ad_channel from(
--				select distinct vid,country_code,ad_channel from tmp_vid_watch union all
--				select distinct vid,country_code,ad_channel from tmp_vid_pay
--			)tb
--		)

		select
	     v1.id as vid
		    ,v1.name
		    ,v1.english_name
		    ,to_timestamp(v1.display_time)::date as display_date
		    ,v1.is_pay
		    ,v1.episode_coin_count
		    ,v1.pay_num
		    ,v1.episode_total
		    ,v1.display_status
		    ,v1.ornaments
		    ,v1.update_status
		    ,to_timestamp(v1.updated_at)::date as updated_date
			,v1.source::text as vid_source
			,v1.type::text as vid_type
			-- ,case when v1.source=1 then '国内' when v1.source=2 then '外采' when v1.source=3 then '自制' end as vid_source
			-- ,case when v1.type=1 then '翻译剧' when v1.type=2 then '本土剧' end as vid_type
		    ,v1.complex
		    ,v1.category
		    ,v2.name as lang
	    ,t0.country_code
	    ,t0.ad_channel
	    ,v3.country_name
	    ,v3.area
--	    ,t2.pay_amt
--	    ,t2.pay_order
--	    ,t2.pay_cnt
--	    ,t2.pay_uv
--	    ,t2.pay_1cnt_uv
--	    ,t2.pay_2cnt_uv
--	    ,t2.pay_3cnt_uv
	    ,t0.vid_uv
	    ,eid_1,  eid_2,  eid_3,  eid_4,  eid_5,  eid_6,  eid_7,  eid_8,  eid_9,  eid_10
	    ,eid_11, eid_12, eid_13, eid_14, eid_15, eid_16, eid_17, eid_18, eid_19, eid_20
	    ,eid_21, eid_22, eid_23, eid_24, eid_25, eid_26, eid_27, eid_28, eid_29, eid_30
	    ,eid_31, eid_32, eid_33, eid_34, eid_35, eid_36, eid_37, eid_38, eid_39, eid_40
	    ,eid_41, eid_42, eid_43, eid_44, eid_45, eid_46, eid_47, eid_48, eid_49, eid_50
	    ,eid_51, eid_52, eid_53, eid_54, eid_55, eid_56, eid_57, eid_58, eid_59, eid_60
	    ,eid_61, eid_62, eid_63, eid_64, eid_65, eid_66, eid_67, eid_68, eid_69, eid_70
	    ,eid_71, eid_72, eid_73, eid_74, eid_75, eid_76, eid_77, eid_78, eid_79, eid_80
	    ,eid_81, eid_82, eid_83, eid_84, eid_85, eid_86, eid_87, eid_88, eid_89, eid_90
	    ,eid_91, eid_92, eid_93, eid_94, eid_95, eid_96, eid_97, eid_98, eid_99, eid_100
	    ,eid_101, eid_102, eid_103, eid_104, eid_105, eid_106, eid_107, eid_108, eid_109, eid_110
		,eid_111, eid_112, eid_113, eid_114, eid_115, eid_116, eid_117, eid_118, eid_119, eid_120
	    ,eid_121, eid_122, eid_123, eid_124, eid_125, eid_126, eid_127, eid_128, eid_129
--	    from tmp_primary t0
	    from tmp_vid_watch t0
--		left join tmp_vid_watch t1 on t0.vid=t1.vid and t0.country_code=t1.country_code and t0.ad_channel=t1.ad_channel
--		left join tmp_vid_pay t2 on t0.vid=t2.vid and t0.country_code=t2.country_code and t0.ad_channel=t2.ad_channel
		left join v_dim_country_area v3 on t0.country_code=v3.country_code
	    left join "oversea-api_osd_videos" v1 on t0.vid=v1.id::text
	    left join "oversea-api_osd_categories" v2 on v1.category_id=v2.id
		where v1.id is not null
		;

        truncate table public.dw_video_play  ;
			insert into public.dw_video_play  select * from analysis.dw_video_play_tmp01;




----完播全量
-- set timezone ='UTC-0';


		truncate table analysis.dw_video_completed_tmp01;
		insert into analysis.dw_video_completed_tmp01
with newuser_tb as(
			select
--          v_date as created_date
            d_date::date as d_date
           ,country_code
           ,ad_channel
           ,upper(put_system) as put_system
           ,ad_format
           ,uid::int8 as uid
           ,split_part(campaign_name,'_',5)::text as vid
           ,campaign_id,campaign_name
            from public.dwd_user_info
	    )
	    ,tmp_vid_watch as(
		    select
		     e0.vid::text as vid
	        ,coalesce(nn.country_code,'UNKNOWN') as country_code
	        ,coalesce(nn.ad_channel,'未知') as ad_channel
--	        ,count(distinct e0.uid) as vid_uv,
	        -- 集衰减
		    	,COUNT(DISTINCT CASE WHEN e1.sort = 1 THEN e0.uid END) AS eid_1_complete_uv,
        COUNT(DISTINCT CASE WHEN e1.sort = 2 THEN e0.uid END) AS eid_2_complete_uv,
        COUNT(DISTINCT CASE WHEN e1.sort = 3 THEN e0.uid END) AS eid_3_complete_uv,
        COUNT(DISTINCT CASE WHEN e1.sort = 4 THEN e0.uid END) AS eid_4_complete_uv,
        COUNT(DISTINCT CASE WHEN e1.sort = 5 THEN e0.uid END) AS eid_5_complete_uv,
count(distinct case when e1.sort = 6 then e0.uid else null end) as eid_6_complete_uv,
count(distinct case when e1.sort = 7 then e0.uid else null end) as eid_7_complete_uv,
count(distinct case when e1.sort = 8 then e0.uid else null end) as eid_8_complete_uv,
count(distinct case when e1.sort = 9 then e0.uid else null end) as eid_9_complete_uv,
count(distinct case when e1.sort = 10 then e0.uid else null end) as eid_10_complete_uv,
count(distinct case when e1.sort = 11 then e0.uid else null end) as eid_11_complete_uv,
count(distinct case when e1.sort = 12 then e0.uid else null end) as eid_12_complete_uv,
count(distinct case when e1.sort = 13 then e0.uid else null end) as eid_13_complete_uv,
count(distinct case when e1.sort = 14 then e0.uid else null end) as eid_14_complete_uv,
count(distinct case when e1.sort = 15 then e0.uid else null end) as eid_15_complete_uv,
count(distinct case when e1.sort = 16 then e0.uid else null end) as eid_16_complete_uv,
count(distinct case when e1.sort = 17 then e0.uid else null end) as eid_17_complete_uv,
count(distinct case when e1.sort = 18 then e0.uid else null end) as eid_18_complete_uv,
count(distinct case when e1.sort = 19 then e0.uid else null end) as eid_19_complete_uv,
count(distinct case when e1.sort = 20 then e0.uid else null end) as eid_20_complete_uv,
count(distinct case when e1.sort = 21 then e0.uid else null end) as eid_21_complete_uv,
count(distinct case when e1.sort = 22 then e0.uid else null end) as eid_22_complete_uv,
count(distinct case when e1.sort = 23 then e0.uid else null end) as eid_23_complete_uv,
count(distinct case when e1.sort = 24 then e0.uid else null end) as eid_24_complete_uv,
count(distinct case when e1.sort = 25 then e0.uid else null end) as eid_25_complete_uv,
count(distinct case when e1.sort = 26 then e0.uid else null end) as eid_26_complete_uv,
count(distinct case when e1.sort = 27 then e0.uid else null end) as eid_27_complete_uv,
count(distinct case when e1.sort = 28 then e0.uid else null end) as eid_28_complete_uv,
count(distinct case when e1.sort = 29 then e0.uid else null end) as eid_29_complete_uv,
count(distinct case when e1.sort = 30 then e0.uid else null end) as eid_30_complete_uv,
count(distinct case when e1.sort = 31 then e0.uid else null end) as eid_31_complete_uv,
count(distinct case when e1.sort = 32 then e0.uid else null end) as eid_32_complete_uv,
count(distinct case when e1.sort = 33 then e0.uid else null end) as eid_33_complete_uv,
count(distinct case when e1.sort = 34 then e0.uid else null end) as eid_34_complete_uv,
count(distinct case when e1.sort = 35 then e0.uid else null end) as eid_35_complete_uv,
count(distinct case when e1.sort = 36 then e0.uid else null end) as eid_36_complete_uv,
count(distinct case when e1.sort = 37 then e0.uid else null end) as eid_37_complete_uv,
count(distinct case when e1.sort = 38 then e0.uid else null end) as eid_38_complete_uv,
count(distinct case when e1.sort = 39 then e0.uid else null end) as eid_39_complete_uv,
count(distinct case when e1.sort = 40 then e0.uid else null end) as eid_40_complete_uv,
count(distinct case when e1.sort = 41 then e0.uid else null end) as eid_41_complete_uv,
count(distinct case when e1.sort = 42 then e0.uid else null end) as eid_42_complete_uv,
count(distinct case when e1.sort = 43 then e0.uid else null end) as eid_43_complete_uv,
count(distinct case when e1.sort = 44 then e0.uid else null end) as eid_44_complete_uv,
count(distinct case when e1.sort = 45 then e0.uid else null end) as eid_45_complete_uv,
count(distinct case when e1.sort = 46 then e0.uid else null end) as eid_46_complete_uv,
count(distinct case when e1.sort = 47 then e0.uid else null end) as eid_47_complete_uv,
count(distinct case when e1.sort = 48 then e0.uid else null end) as eid_48_complete_uv,
count(distinct case when e1.sort = 49 then e0.uid else null end) as eid_49_complete_uv,
count(distinct case when e1.sort = 50 then e0.uid else null end) as eid_50_complete_uv,
count(distinct case when e1.sort = 51 then e0.uid else null end) as eid_51_complete_uv,
count(distinct case when e1.sort = 52 then e0.uid else null end) as eid_52_complete_uv,
count(distinct case when e1.sort = 53 then e0.uid else null end) as eid_53_complete_uv,
count(distinct case when e1.sort = 54 then e0.uid else null end) as eid_54_complete_uv,
count(distinct case when e1.sort = 55 then e0.uid else null end) as eid_55_complete_uv,
count(distinct case when e1.sort = 56 then e0.uid else null end) as eid_56_complete_uv,
count(distinct case when e1.sort = 57 then e0.uid else null end) as eid_57_complete_uv,
count(distinct case when e1.sort = 58 then e0.uid else null end) as eid_58_complete_uv,
count(distinct case when e1.sort = 59 then e0.uid else null end) as eid_59_complete_uv,
count(distinct case when e1.sort = 60 then e0.uid else null end) as eid_60_complete_uv,
count(distinct case when e1.sort = 61 then e0.uid else null end) as eid_61_complete_uv,
count(distinct case when e1.sort = 62 then e0.uid else null end) as eid_62_complete_uv,
count(distinct case when e1.sort = 63 then e0.uid else null end) as eid_63_complete_uv,
count(distinct case when e1.sort = 64 then e0.uid else null end) as eid_64_complete_uv,
count(distinct case when e1.sort = 65 then e0.uid else null end) as eid_65_complete_uv,
count(distinct case when e1.sort = 66 then e0.uid else null end) as eid_66_complete_uv,
count(distinct case when e1.sort = 67 then e0.uid else null end) as eid_67_complete_uv,
count(distinct case when e1.sort = 68 then e0.uid else null end) as eid_68_complete_uv,
count(distinct case when e1.sort = 69 then e0.uid else null end) as eid_69_complete_uv,
count(distinct case when e1.sort = 70 then e0.uid else null end) as eid_70_complete_uv,
count(distinct case when e1.sort = 71 then e0.uid else null end) as eid_71_complete_uv,
count(distinct case when e1.sort = 72 then e0.uid else null end) as eid_72_complete_uv,
count(distinct case when e1.sort = 73 then e0.uid else null end) as eid_73_complete_uv,
count(distinct case when e1.sort = 74 then e0.uid else null end) as eid_74_complete_uv,
count(distinct case when e1.sort = 75 then e0.uid else null end) as eid_75_complete_uv,
count(distinct case when e1.sort = 76 then e0.uid else null end) as eid_76_complete_uv,
count(distinct case when e1.sort = 77 then e0.uid else null end) as eid_77_complete_uv,
count(distinct case when e1.sort = 78 then e0.uid else null end) as eid_78_complete_uv,
count(distinct case when e1.sort = 79 then e0.uid else null end) as eid_79_complete_uv,
count(distinct case when e1.sort = 80 then e0.uid else null end) as eid_80_complete_uv,
count(distinct case when e1.sort = 81 then e0.uid else null end) as eid_81_complete_uv,
count(distinct case when e1.sort = 82 then e0.uid else null end) as eid_82_complete_uv,
count(distinct case when e1.sort = 83 then e0.uid else null end) as eid_83_complete_uv,
count(distinct case when e1.sort = 84 then e0.uid else null end) as eid_84_complete_uv,
count(distinct case when e1.sort = 85 then e0.uid else null end) as eid_85_complete_uv,
count(distinct case when e1.sort = 86 then e0.uid else null end) as eid_86_complete_uv,
count(distinct case when e1.sort = 87 then e0.uid else null end) as eid_87_complete_uv,
count(distinct case when e1.sort = 88 then e0.uid else null end) as eid_88_complete_uv,
count(distinct case when e1.sort = 89 then e0.uid else null end) as eid_89_complete_uv,
count(distinct case when e1.sort = 90 then e0.uid else null end) as eid_90_complete_uv,
count(distinct case when e1.sort = 91 then e0.uid else null end) as eid_91_complete_uv,
count(distinct case when e1.sort = 92 then e0.uid else null end) as eid_92_complete_uv,
count(distinct case when e1.sort = 93 then e0.uid else null end) as eid_93_complete_uv,
count(distinct case when e1.sort = 94 then e0.uid else null end) as eid_94_complete_uv,
count(distinct case when e1.sort = 95 then e0.uid else null end) as eid_95_complete_uv,
count(distinct case when e1.sort = 96 then e0.uid else null end) as eid_96_complete_uv,
count(distinct case when e1.sort = 97 then e0.uid else null end) as eid_97_complete_uv,
count(distinct case when e1.sort = 98 then e0.uid else null end) as eid_98_complete_uv,
count(distinct case when e1.sort = 99 then e0.uid else null end) as eid_99_complete_uv,
count(distinct case when e1.sort = 100 then e0.uid else null end) as eid_100_complete_uv,
count(distinct case when e1.sort = 101 then e0.uid else null end) as eid_101_complete_uv,
count(distinct case when e1.sort = 102 then e0.uid else null end) as eid_102_complete_uv,
count(distinct case when e1.sort = 103 then e0.uid else null end) as eid_103_complete_uv,
count(distinct case when e1.sort = 104 then e0.uid else null end) as eid_104_complete_uv,
count(distinct case when e1.sort = 105 then e0.uid else null end) as eid_105_complete_uv,
count(distinct case when e1.sort = 106 then e0.uid else null end) as eid_106_complete_uv,
count(distinct case when e1.sort = 107 then e0.uid else null end) as eid_107_complete_uv,
count(distinct case when e1.sort = 108 then e0.uid else null end) as eid_108_complete_uv,
count(distinct case when e1.sort = 109 then e0.uid else null end) as eid_109_complete_uv,
count(distinct case when e1.sort = 110 then e0.uid else null end) as eid_110_complete_uv,
count(distinct case when e1.sort = 111 then e0.uid else null end) as eid_111_complete_uv,
count(distinct case when e1.sort = 112 then e0.uid else null end) as eid_112_complete_uv,
count(distinct case when e1.sort = 113 then e0.uid else null end) as eid_113_complete_uv,
count(distinct case when e1.sort = 114 then e0.uid else null end) as eid_114_complete_uv,
count(distinct case when e1.sort = 115 then e0.uid else null end) as eid_115_complete_uv,
count(distinct case when e1.sort = 116 then e0.uid else null end) as eid_116_complete_uv,
count(distinct case when e1.sort = 117 then e0.uid else null end) as eid_117_complete_uv,
count(distinct case when e1.sort = 118 then e0.uid else null end) as eid_118_complete_uv,
count(distinct case when e1.sort = 119 then e0.uid else null end) as eid_119_complete_uv,
count(distinct case when e1.sort = 120 then e0.uid else null end) as eid_120_complete_uv,
count(distinct case when e1.sort = 121 then e0.uid else null end) as eid_121_complete_uv,
count(distinct case when e1.sort = 122 then e0.uid else null end) as eid_122_complete_uv,
count(distinct case when e1.sort = 123 then e0.uid else null end) as eid_123_complete_uv,
count(distinct case when e1.sort = 124 then e0.uid else null end) as eid_124_complete_uv,
count(distinct case when e1.sort = 125 then e0.uid else null end) as eid_125_complete_uv,
count(distinct case when e1.sort = 126 then e0.uid else null end) as eid_126_complete_uv,
count(distinct case when e1.sort = 127 then e0.uid else null end) as eid_127_complete_uv,
count(distinct case when e1.sort = 128 then e0.uid else null end) as eid_128_complete_uv,
count(distinct case when e1.sort = 129 then e0.uid else null end) as eid_129_complete_uv
		    from(
		        select uid -- 历史所有观看用户，查询压力大
	            ,vid, eid
		        from public.app_user_track_log
		        where 1=1
		        and event =87
		        and vid>0 and eid>0
		        and  created_date>=20241020
		        	-- and to_timestamp(created_at)::date>= (current_date+interval '-1 day')::date -- 增
		        group by uid
	            ,vid, eid

				-- event in (2,13,14)
				-- 2 离开看剧页面的时候上报，统计观看时长	 drama_watch_time
				-- 13 向下滑动页面	drama_scroll_down
				-- 14 向上滑动页面	drama_scroll_up
				-- 1 进入短剧观看页面就上报	drama_watch
				-- 6 短剧观看页面完成广告任务点击领取奖励	drama_adpopup_claim
				--
		    )e0
		    left join newuser_tb nn on e0.uid=nn.uid
		    left join "oversea-api_osd_video_episodes" e1 on e0.eid=e1.id
			where e0.vid=e1.vid
		    group by
		     e0.vid
	        ,coalesce(nn.country_code,'UNKNOWN')
	        ,coalesce(nn.ad_channel,'未知')
		)
		---剧的首充率、复充率、三充及以上
		,tmp_vid_pay as(
		    select
		     e0.vid::text as vid
	        ,coalesce(nn.country_code,'UNKNOWN') as country_code
	        ,coalesce(nn.ad_channel,'未知') as ad_channel
	        ,sum(e0.pay_amt) as pay_amt
	        ,sum(e0.pay_order) as pay_order
	        ,sum(e0.pay_cnt) as pay_cnt -- 充值次数
	        ,count(distinct e0.uid) as pay_uv
	        ,count(distinct case when e0.pay_cnt  = 1 then e0.uid else null end) as pay_1cnt_uv
	        ,count(distinct case when e0.pay_cnt  = 2 then e0.uid else null end) as pay_2cnt_uv
	        ,count(distinct case when e0.pay_cnt >= 3 then e0.uid else null end) as pay_3cnt_uv
		    from(
		        select goods_id as vid
	            ,uid
		        ,sum(money)*0.01 as pay_amt
	            ,count(distinct order_num) as pay_order
	            ,count(distinct (cast(order_num as varchar) || cast(order_type as varchar) || cast(created_at as varchar))) as pay_cnt -- 支付次数（包含续订）
		        from public.all_order_log
		        where status = 1 and environment = 1
		        and goods_id <> 0
		        and created_date>=20241020
		        	-- and to_timestamp(created_at)::date >= (current_date+interval '-1 day')::date -- 增
					AND to_timestamp(created_at) < date_trunc('hour', now() - interval '1 hour') ---截止上个小时整点之前
		        group by goods_id
	            ,uid
		    )e0
		    left join newuser_tb nn on e0.uid=nn.uid
		    group by
		     e0.vid
	        ,coalesce(nn.country_code,'UNKNOWN')
	        ,coalesce(nn.ad_channel,'未知')
		)
		,tmp_primary as(
			select distinct vid,country_code,ad_channel from(
				select distinct vid,country_code,ad_channel from tmp_vid_watch union all
				select distinct vid,country_code,ad_channel from tmp_vid_pay
			)tb
		)

		select
	     v1.id as vid
		    ,v1.name
		    ,v1.english_name
		    ,to_timestamp(v1.display_time)::date as display_date
		    ,v1.is_pay
		    ,v1.episode_coin_count
		    ,v1.pay_num
		    ,v1.episode_total
		    ,v1.display_status
		    ,v1.ornaments
		    ,v1.update_status
		    ,to_timestamp(v1.updated_at)::date as updated_date
			,v1.source::text as vid_source
			,v1.type::text as vid_type
			-- ,case when v1.source=1 then '国内' when v1.source=2 then '外采' when v1.source=3 then '自制' end as vid_source
			-- ,case when v1.type=1 then '翻译剧' when v1.type=2 then '本土剧' end as vid_type
		    ,v1.complex
		    ,v1.category
		    ,v2.name as lang
	    ,t0.country_code
	    ,t0.ad_channel
	    ,v3.country_name
	    ,v3.area
	    ,t2.pay_amt
	    ,t2.pay_order
	    ,t2.pay_cnt
	    ,t2.pay_uv
	    ,t2.pay_1cnt_uv
	    ,t2.pay_2cnt_uv
	    ,t2.pay_3cnt_uv
--	    ,t0.vid_uv
--	    ,eid_1,  eid_2,  eid_3,  eid_4,  eid_5,  eid_6,  eid_7,  eid_8,  eid_9,  eid_10
--	    ,eid_11, eid_12, eid_13, eid_14, eid_15, eid_16, eid_17, eid_18, eid_19, eid_20
--	    ,eid_21, eid_22, eid_23, eid_24, eid_25, eid_26, eid_27, eid_28, eid_29, eid_30
--	    ,eid_31, eid_32, eid_33, eid_34, eid_35, eid_36, eid_37, eid_38, eid_39, eid_40
--	    ,eid_41, eid_42, eid_43, eid_44, eid_45, eid_46, eid_47, eid_48, eid_49, eid_50
--	    ,eid_51, eid_52, eid_53, eid_54, eid_55, eid_56, eid_57, eid_58, eid_59, eid_60
--	    ,eid_61, eid_62, eid_63, eid_64, eid_65, eid_66, eid_67, eid_68, eid_69, eid_70
--	    ,eid_71, eid_72, eid_73, eid_74, eid_75, eid_76, eid_77, eid_78, eid_79, eid_80
--	    ,eid_81, eid_82, eid_83, eid_84, eid_85, eid_86, eid_87, eid_88, eid_89, eid_90
--	    ,eid_91, eid_92, eid_93, eid_94, eid_95, eid_96, eid_97, eid_98, eid_99, eid_100
--	    ,eid_101, eid_102, eid_103, eid_104, eid_105, eid_106, eid_107, eid_108, eid_109, eid_110
--		,eid_111, eid_112, eid_113, eid_114, eid_115, eid_116, eid_117, eid_118, eid_119, eid_120
--	    ,eid_121, eid_122, eid_123, eid_124, eid_125, eid_126, eid_127, eid_128, eid_129
	    ,t1.eid_1_complete_uv,
    t1.eid_2_complete_uv,
    t1.eid_3_complete_uv,
    t1.eid_4_complete_uv,
    t1.eid_5_complete_uv,
    t1.eid_6_complete_uv,
     t1.eid_7_complete_uv,
     t1.eid_8_complete_uv,
     t1.eid_9_complete_uv,
t1.eid_10_complete_uv,
t1.eid_11_complete_uv,
t1.eid_12_complete_uv,
t1.eid_13_complete_uv,
t1.eid_14_complete_uv,
t1.eid_15_complete_uv,
t1.eid_16_complete_uv,
t1.eid_17_complete_uv,
t1.eid_18_complete_uv,
t1.eid_19_complete_uv,
t1.eid_20_complete_uv,
t1.eid_21_complete_uv,
t1.eid_22_complete_uv,
t1.eid_23_complete_uv,
t1.eid_24_complete_uv,
t1.eid_25_complete_uv,
t1.eid_26_complete_uv,
t1.eid_27_complete_uv,
t1.eid_28_complete_uv,
t1.eid_29_complete_uv,
t1.eid_30_complete_uv,
t1.eid_31_complete_uv,
t1.eid_32_complete_uv,
t1.eid_33_complete_uv,
t1.eid_34_complete_uv,
t1.eid_35_complete_uv,
t1.eid_36_complete_uv,
t1.eid_37_complete_uv,
t1.eid_38_complete_uv,
t1.eid_39_complete_uv,
t1.eid_40_complete_uv,
t1.eid_41_complete_uv,
t1.eid_42_complete_uv,
t1.eid_43_complete_uv,
t1.eid_44_complete_uv,
t1.eid_45_complete_uv,
t1.eid_46_complete_uv,
t1.eid_47_complete_uv,
t1.eid_48_complete_uv,
t1.eid_49_complete_uv,
t1.eid_50_complete_uv,
t1.eid_51_complete_uv,
t1.eid_52_complete_uv,
t1.eid_53_complete_uv,
t1.eid_54_complete_uv,
t1.eid_55_complete_uv,
t1.eid_56_complete_uv,
t1.eid_57_complete_uv,
t1.eid_58_complete_uv,
t1.eid_59_complete_uv,
t1.eid_60_complete_uv,
t1.eid_61_complete_uv,
t1.eid_62_complete_uv,
t1.eid_63_complete_uv,
t1.eid_64_complete_uv,
t1.eid_65_complete_uv,
t1.eid_66_complete_uv,
t1.eid_67_complete_uv,
t1.eid_68_complete_uv,
t1.eid_69_complete_uv,
t1.eid_70_complete_uv,
t1.eid_71_complete_uv,
t1.eid_72_complete_uv,
t1.eid_73_complete_uv,
t1.eid_74_complete_uv,
t1.eid_75_complete_uv,
t1.eid_76_complete_uv,
t1.eid_77_complete_uv,
t1.eid_78_complete_uv,
t1.eid_79_complete_uv,
t1.eid_80_complete_uv,
t1.eid_81_complete_uv,
t1.eid_82_complete_uv,
t1.eid_83_complete_uv,
t1.eid_84_complete_uv,
t1.eid_85_complete_uv,
t1.eid_86_complete_uv,
t1.eid_87_complete_uv,
t1.eid_88_complete_uv,
t1.eid_89_complete_uv,
t1.eid_90_complete_uv,
t1.eid_91_complete_uv,
t1.eid_92_complete_uv,
t1.eid_93_complete_uv,
t1.eid_94_complete_uv,
t1.eid_95_complete_uv,
t1.eid_96_complete_uv,
t1.eid_97_complete_uv,
t1.eid_98_complete_uv,
t1.eid_99_complete_uv,
t1.eid_100_complete_uv,
t1.eid_101_complete_uv,
t1.eid_102_complete_uv,
t1.eid_103_complete_uv,
t1.eid_104_complete_uv,
t1.eid_105_complete_uv,
t1.eid_106_complete_uv,
t1.eid_107_complete_uv,
t1.eid_108_complete_uv,
t1.eid_109_complete_uv,
t1.eid_110_complete_uv,
t1.eid_111_complete_uv,
t1.eid_112_complete_uv,
t1.eid_113_complete_uv,
t1.eid_114_complete_uv,
t1.eid_115_complete_uv,
t1.eid_116_complete_uv,
t1.eid_117_complete_uv,
t1.eid_118_complete_uv,
t1.eid_119_complete_uv,
t1.eid_120_complete_uv,
t1.eid_121_complete_uv,
t1.eid_122_complete_uv,
t1.eid_123_complete_uv,
t1.eid_124_complete_uv,
t1.eid_125_complete_uv,
t1.eid_126_complete_uv,
t1.eid_127_complete_uv,
t1.eid_128_complete_uv,
t1.eid_129_complete_uv
	    from tmp_primary t0
--	    from tmp_vid_watch t0
		left join tmp_vid_watch t1 on t0.vid=t1.vid and t0.country_code=t1.country_code and t0.ad_channel=t1.ad_channel
		left join tmp_vid_pay t2 on t0.vid=t2.vid and t0.country_code=t2.country_code and t0.ad_channel=t2.ad_channel
		left join v_dim_country_area v3 on t0.country_code=v3.country_code
	    left join "oversea-api_osd_videos" v1 on t0.vid=v1.id::text
	    left join "oversea-api_osd_categories" v2 on v1.category_id=v2.id
		where v1.id is not null
		;


        truncate table public.dw_video_completed  ;
			insert into public.dw_video_completed  select * from analysis.dw_video_completed_tmp01;



-----播放加完播

-- set timezone ='UTC-0';


truncate table analysis.dw_video_play_completed_tmp01;
insert into analysis.dw_video_play_completed_tmp01
SELECT
    t1.vid,
    t1.name,
    t1.english_name,
    t1.display_date,
    t1.is_pay,
    t1.episode_coin_count,
    t1.pay_num,
    t1.episode_total,
    t1.display_status,
    t1.ornaments,
    t1.update_status,
    t1.updated_date,
    t1.vid_source,
    t1.vid_type,
    t1.complex,
    t1.category,
    t1.lang,
    t1.country_code,
    t1.ad_channel,
    t1.country_name,
    t1.area,
    t2.pay_amt,
    t2.pay_order,
    t2.pay_cnt,
    t2.pay_uv,
    t2.pay_1cnt_uv,
    t2.pay_2cnt_uv,
    t2.pay_3cnt_uv,
    t1.vid_uv,
    t1.eid_1,
    t1.eid_2,
    t1.eid_3,
    t1.eid_4,
    t1.eid_5,
     t1.eid_6, t1.eid_7, t1.eid_8, t1.eid_9, t1.eid_10, t1.eid_11, t1.eid_12, t1.eid_13, t1.eid_14, t1.eid_15, t1.eid_16, t1.eid_17,
t1.eid_18, t1.eid_19, t1.eid_20, t1.eid_21, t1.eid_22, t1.eid_23, t1.eid_24, t1.eid_25, t1.eid_26, t1.eid_27, t1.eid_28, t1.eid_29, t1.eid_30, t1.eid_31, t1.eid_32, t1.eid_33,
t1.eid_34, t1.eid_35, t1.eid_36, t1.eid_37, t1.eid_38, t1.eid_39, t1.eid_40, t1.eid_41, t1.eid_42, t1.eid_43, t1.eid_44, t1.eid_45, t1.eid_46, t1.eid_47, t1.eid_48, t1.eid_49,
t1.eid_50, t1.eid_51, t1.eid_52, t1.eid_53, t1.eid_54, t1.eid_55, t1.eid_56, t1.eid_57, t1.eid_58, t1.eid_59, t1.eid_60, t1.eid_61, t1.eid_62, t1.eid_63, t1.eid_64, t1.eid_65,
t1.eid_66, t1.eid_67, t1.eid_68, t1.eid_69, t1.eid_70, t1.eid_71, t1.eid_72, t1.eid_73, t1.eid_74, t1.eid_75, t1.eid_76, t1.eid_77, t1.eid_78, t1.eid_79, t1.eid_80, t1.eid_81,
t1.eid_82, t1.eid_83, t1.eid_84, t1.eid_85, t1.eid_86, t1.eid_87, t1.eid_88, t1.eid_89, t1.eid_90, t1.eid_91, t1.eid_92, t1.eid_93, t1.eid_94, t1.eid_95, t1.eid_96, t1.eid_97,
t1.eid_98, t1.eid_99, t1.eid_100, t1.eid_101, t1.eid_102, t1.eid_103, t1.eid_104, t1.eid_105, t1.eid_106, t1.eid_107, t1.eid_108, t1.eid_109, t1.eid_110, t1.eid_111, t1.eid_112,
t1.eid_113, t1.eid_114, t1.eid_115, t1.eid_116, t1.eid_117, t1.eid_118, t1.eid_119, t1.eid_120, t1.eid_121, t1.eid_122, t1.eid_123, t1.eid_124, t1.eid_125, t1.eid_126, t1.eid_127,
t1.eid_128, t1.eid_129,
    t2.eid_1_complete_uv,
    t2.eid_2_complete_uv,
    t2.eid_3_complete_uv,
    t2.eid_4_complete_uv,
    t2.eid_5_complete_uv,
     t2.eid_6_complete_uv,
     t2.eid_7_complete_uv,
     t2.eid_8_complete_uv,
     t2.eid_9_complete_uv,
t2.eid_10_complete_uv,
t2.eid_11_complete_uv,
t2.eid_12_complete_uv,
t2.eid_13_complete_uv,
t2.eid_14_complete_uv,
t2.eid_15_complete_uv,
t2.eid_16_complete_uv,
t2.eid_17_complete_uv,
t2.eid_18_complete_uv,
t2.eid_19_complete_uv,
t2.eid_20_complete_uv,
t2.eid_21_complete_uv,
t2.eid_22_complete_uv,
t2.eid_23_complete_uv,
t2.eid_24_complete_uv,
t2.eid_25_complete_uv,
t2.eid_26_complete_uv,
t2.eid_27_complete_uv,
t2.eid_28_complete_uv,
t2.eid_29_complete_uv,
t2.eid_30_complete_uv,
t2.eid_31_complete_uv,
t2.eid_32_complete_uv,
t2.eid_33_complete_uv,
t2.eid_34_complete_uv,
t2.eid_35_complete_uv,
t2.eid_36_complete_uv,
t2.eid_37_complete_uv,
t2.eid_38_complete_uv,
t2.eid_39_complete_uv,
t2.eid_40_complete_uv,
t2.eid_41_complete_uv,
t2.eid_42_complete_uv,
t2.eid_43_complete_uv,
t2.eid_44_complete_uv,
t2.eid_45_complete_uv,
t2.eid_46_complete_uv,
t2.eid_47_complete_uv,
t2.eid_48_complete_uv,
t2.eid_49_complete_uv,
t2.eid_50_complete_uv,
t2.eid_51_complete_uv,
t2.eid_52_complete_uv,
t2.eid_53_complete_uv,
t2.eid_54_complete_uv,
t2.eid_55_complete_uv,
t2.eid_56_complete_uv,
t2.eid_57_complete_uv,
t2.eid_58_complete_uv,
t2.eid_59_complete_uv,
t2.eid_60_complete_uv,
t2.eid_61_complete_uv,
t2.eid_62_complete_uv,
t2.eid_63_complete_uv,
t2.eid_64_complete_uv,
t2.eid_65_complete_uv,
t2.eid_66_complete_uv,
t2.eid_67_complete_uv,
t2.eid_68_complete_uv,
t2.eid_69_complete_uv,
t2.eid_70_complete_uv,
t2.eid_71_complete_uv,
t2.eid_72_complete_uv,
t2.eid_73_complete_uv,
t2.eid_74_complete_uv,
t2.eid_75_complete_uv,
t2.eid_76_complete_uv,
t2.eid_77_complete_uv,
t2.eid_78_complete_uv,
t2.eid_79_complete_uv,
t2.eid_80_complete_uv,
t2.eid_81_complete_uv,
t2.eid_82_complete_uv,
t2.eid_83_complete_uv,
t2.eid_84_complete_uv,
t2.eid_85_complete_uv,
t2.eid_86_complete_uv,
t2.eid_87_complete_uv,
t2.eid_88_complete_uv,
t2.eid_89_complete_uv,
t2.eid_90_complete_uv,
t2.eid_91_complete_uv,
t2.eid_92_complete_uv,
t2.eid_93_complete_uv,
t2.eid_94_complete_uv,
t2.eid_95_complete_uv,
t2.eid_96_complete_uv,
t2.eid_97_complete_uv,
t2.eid_98_complete_uv,
t2.eid_99_complete_uv,
t2.eid_100_complete_uv,
t2.eid_101_complete_uv,
t2.eid_102_complete_uv,
t2.eid_103_complete_uv,
t2.eid_104_complete_uv,
t2.eid_105_complete_uv,
t2.eid_106_complete_uv,
t2.eid_107_complete_uv,
t2.eid_108_complete_uv,
t2.eid_109_complete_uv,
t2.eid_110_complete_uv,
t2.eid_111_complete_uv,
t2.eid_112_complete_uv,
t2.eid_113_complete_uv,
t2.eid_114_complete_uv,
t2.eid_115_complete_uv,
t2.eid_116_complete_uv,
t2.eid_117_complete_uv,
t2.eid_118_complete_uv,
t2.eid_119_complete_uv,
t2.eid_120_complete_uv,
t2.eid_121_complete_uv,
t2.eid_122_complete_uv,
t2.eid_123_complete_uv,
t2.eid_124_complete_uv,
t2.eid_125_complete_uv,
t2.eid_126_complete_uv,
t2.eid_127_complete_uv,
t2.eid_128_complete_uv
,t2.eid_129_complete_uv
,v1.translate_type
,v1.source_lang
FROM public.dw_video_play t1
JOIN public.dw_video_completed t2 ON
    t1.vid = t2.vid AND
    t1.country_code = t2.country_code AND
    t1.ad_channel = t2.ad_channel
left join public."oversea-api_osd_videos" v1 on t1.vid=v1.id::text
    ;



    truncate table public.dw_video_play_completed;
            insert into public.dw_video_play_completed  select * from analysis.dw_video_play_completed_tmp01;