------------------------------------------
-- file: 剧报表_指标概览日ROI_全量_d.sql
-- author: xiaoj
-- time: 2025/5/16 18:20
-- description:
------------------------------------------

	set timezone ='UTC-0';
	truncate table analysis.dw_video_roi_calc0210;
	insert into analysis.dw_video_roi_calc0210
		with newuser_tb as(
			select v_date as created_date
			,d_date::date as d_date
			,uid::int8 as uid
			,country_code ,ad_channel
			,lang
			,lang_name
			,campaign_id
			from public.dwd_user_info

		)
		,tmp_user_pay as(
			select p_date,uid
			,goods_id
			,sum(pay_amt) as pay_amt
			from(
		        select to_timestamp(o.created_at)::date as p_date
		        ,o.uid
				,o.goods_id
		        ,sum(o.money)*0.01 as pay_amt
		        from public.all_order_log o
		        where environment = 1  and status = 1
				and o.created_date >= '20240701'
		        group by to_timestamp(o.created_at)::date
		        ,o.uid
				,o.goods_id

		        union all
		        select to_timestamp(r.refund_time)::date as p_date
		        ,r.uid
				,r.goods_id
		        ,-sum(r.money)*0.01 as refund_amt
		        from public.all_refund_order_log r
		        where r.environment = 1 and r.status = 1
				and r.refund_date >= '20240701'
		        group by to_timestamp(r.refund_time)::date
		        ,r.uid
				,r.goods_id

		    )a
		    group by p_date,uid
			,goods_id
		)
		,tmp_camp_roi as(
		    select
		     n1.d_date
		    ,n1.country_code
		    ,n1.ad_channel
		    ,n1.campaign_id
		    ,count(distinct n1.uid) as new_uv
		    ,count(distinct case when d_date=p_date then p1.uid else null end) as new_pay_uv
		    ,count(distinct p1.uid) as new_payuv_total
		    ,sum(p1.pay_amt) as new_payamt_total
		    	,sum(case when d_date=p_date and d_date<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_0
				,sum(case when d_date=p_date and d_date<=(current_date+interval'0 d')::date and p1.goods_id::text = tc.vid then pay_amt else null end) as attributed_pay_0
		    	,sum(case when d_date between (p_date+interval'- 1 d') and p_date  and (d_date+interval' 1 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_1
		    	,sum(case when d_date between (p_date+interval'- 2 d') and p_date  and (d_date+interval' 2 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_2
		    	,sum(case when d_date between (p_date+interval'- 3 d') and p_date  and (d_date+interval' 3 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_3
		    	,sum(case when d_date between (p_date+interval'- 4 d') and p_date  and (d_date+interval' 4 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_4
		    	,sum(case when d_date between (p_date+interval'- 5 d') and p_date  and (d_date+interval' 5 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_5
		    	,sum(case when d_date between (p_date+interval'- 6 d') and p_date  and (d_date+interval' 6 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_6
		    	,sum(case when d_date between (p_date+interval'- 7 d') and p_date  and (d_date+interval' 7 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_7
		    	,sum(case when d_date between (p_date+interval'- 8 d') and p_date  and (d_date+interval' 8 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_8
		    	,sum(case when d_date between (p_date+interval'- 9 d') and p_date  and (d_date+interval' 9 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_9
		    	,sum(case when d_date between (p_date+interval'-10 d') and p_date  and (d_date+interval'10 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_10
		    	,sum(case when d_date between (p_date+interval'-11 d') and p_date  and (d_date+interval'11 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_11
		    	,sum(case when d_date between (p_date+interval'-12 d') and p_date  and (d_date+interval'12 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_12
		    	,sum(case when d_date between (p_date+interval'-13 d') and p_date  and (d_date+interval'13 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_13
		    	,sum(case when d_date between (p_date+interval'-14 d') and p_date  and (d_date+interval'14 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_14
		    	,sum(case when d_date between (p_date+interval'-15 d') and p_date  and (d_date+interval'15 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_15
		    	,sum(case when d_date between (p_date+interval'-16 d') and p_date  and (d_date+interval'16 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_16
		    	,sum(case when d_date between (p_date+interval'-17 d') and p_date  and (d_date+interval'17 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_17
		    	,sum(case when d_date between (p_date+interval'-18 d') and p_date  and (d_date+interval'18 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_18
		    	,sum(case when d_date between (p_date+interval'-19 d') and p_date  and (d_date+interval'19 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_19
		    	,sum(case when d_date between (p_date+interval'-20 d') and p_date  and (d_date+interval'20 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_20
		    	,sum(case when d_date between (p_date+interval'-21 d') and p_date  and (d_date+interval'21 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_21
		    	,sum(case when d_date between (p_date+interval'-22 d') and p_date  and (d_date+interval'22 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_22
		    	,sum(case when d_date between (p_date+interval'-23 d') and p_date  and (d_date+interval'23 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_23
		    	,sum(case when d_date between (p_date+interval'-24 d') and p_date  and (d_date+interval'24 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_24
		    	,sum(case when d_date between (p_date+interval'-25 d') and p_date  and (d_date+interval'25 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_25
		    	,sum(case when d_date between (p_date+interval'-26 d') and p_date  and (d_date+interval'26 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_26
		    	,sum(case when d_date between (p_date+interval'-27 d') and p_date  and (d_date+interval'27 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_27
		    	,sum(case when d_date between (p_date+interval'-28 d') and p_date  and (d_date+interval'28 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_28
		    	,sum(case when d_date between (p_date+interval'-29 d') and p_date  and (d_date+interval'29 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_29
		    	,sum(case when d_date between (p_date+interval'-30 d') and p_date  and (d_date+interval'30 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_30
		    	,sum(case when d_date between (p_date+interval'-31 d') and p_date  and (d_date+interval'31 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_31
		    	,sum(case when d_date between (p_date+interval'-32 d') and p_date  and (d_date+interval'32 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_32
		    	,sum(case when d_date between (p_date+interval'-33 d') and p_date  and (d_date+interval'33 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_33
		    	,sum(case when d_date between (p_date+interval'-34 d') and p_date  and (d_date+interval'34 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_34
		    	,sum(case when d_date between (p_date+interval'-35 d') and p_date  and (d_date+interval'35 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_35
		    	,sum(case when d_date between (p_date+interval'-36 d') and p_date  and (d_date+interval'36 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_36
		    	,sum(case when d_date between (p_date+interval'-37 d') and p_date  and (d_date+interval'37 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_37
		    	,sum(case when d_date between (p_date+interval'-38 d') and p_date  and (d_date+interval'38 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_38
		    	,sum(case when d_date between (p_date+interval'-39 d') and p_date  and (d_date+interval'39 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_39
		    	,sum(case when d_date between (p_date+interval'-40 d') and p_date  and (d_date+interval'40 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_40
		    	,sum(case when d_date between (p_date+interval'-41 d') and p_date  and (d_date+interval'41 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_41
		    	,sum(case when d_date between (p_date+interval'-42 d') and p_date  and (d_date+interval'42 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_42
		    	,sum(case when d_date between (p_date+interval'-43 d') and p_date  and (d_date+interval'43 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_43
		    	,sum(case when d_date between (p_date+interval'-44 d') and p_date  and (d_date+interval'44 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_44
		    	,sum(case when d_date between (p_date+interval'-45 d') and p_date  and (d_date+interval'45 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_45
		    	,sum(case when d_date between (p_date+interval'-46 d') and p_date  and (d_date+interval'46 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_46
		    	,sum(case when d_date between (p_date+interval'-47 d') and p_date  and (d_date+interval'47 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_47
		    	,sum(case when d_date between (p_date+interval'-48 d') and p_date  and (d_date+interval'48 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_48
		    	,sum(case when d_date between (p_date+interval'-49 d') and p_date  and (d_date+interval'49 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_49
		    	,sum(case when d_date between (p_date+interval'-50 d') and p_date  and (d_date+interval'50 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_50
		    	,sum(case when d_date between (p_date+interval'-51 d') and p_date  and (d_date+interval'51 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_51
		    	,sum(case when d_date between (p_date+interval'-52 d') and p_date  and (d_date+interval'52 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_52
		    	,sum(case when d_date between (p_date+interval'-53 d') and p_date  and (d_date+interval'53 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_53
		    	,sum(case when d_date between (p_date+interval'-54 d') and p_date  and (d_date+interval'54 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_54
		    	,sum(case when d_date between (p_date+interval'-55 d') and p_date  and (d_date+interval'55 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_55
		    	,sum(case when d_date between (p_date+interval'-56 d') and p_date  and (d_date+interval'56 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_56
		    	,sum(case when d_date between (p_date+interval'-57 d') and p_date  and (d_date+interval'57 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_57
		    	,sum(case when d_date between (p_date+interval'-58 d') and p_date  and (d_date+interval'58 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_58
		    	,sum(case when d_date between (p_date+interval'-59 d') and p_date  and (d_date+interval'59 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_59
			    	,sum(case when d_date between (p_date+interval'-60 d') and p_date  and (d_date+interval'60 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_60
			    	,sum(case when d_date between (p_date+interval'-61 d') and p_date  and (d_date+interval'61 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_61
			    	,sum(case when d_date between (p_date+interval'-62 d') and p_date  and (d_date+interval'62 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_62
			    	,sum(case when d_date between (p_date+interval'-63 d') and p_date  and (d_date+interval'63 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_63
			    	,sum(case when d_date between (p_date+interval'-64 d') and p_date  and (d_date+interval'64 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_64
			    	,sum(case when d_date between (p_date+interval'-65 d') and p_date  and (d_date+interval'65 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_65
			    	,sum(case when d_date between (p_date+interval'-66 d') and p_date  and (d_date+interval'66 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_66
			    	,sum(case when d_date between (p_date+interval'-67 d') and p_date  and (d_date+interval'67 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_67
			    	,sum(case when d_date between (p_date+interval'-68 d') and p_date  and (d_date+interval'68 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_68
			    	,sum(case when d_date between (p_date+interval'-69 d') and p_date  and (d_date+interval'69 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_69
			    	,sum(case when d_date between (p_date+interval'-70 d') and p_date  and (d_date+interval'70 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_70
			    	,sum(case when d_date between (p_date+interval'-71 d') and p_date  and (d_date+interval'71 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_71
			    	,sum(case when d_date between (p_date+interval'-72 d') and p_date  and (d_date+interval'72 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_72
			    	,sum(case when d_date between (p_date+interval'-73 d') and p_date  and (d_date+interval'73 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_73
			    	,sum(case when d_date between (p_date+interval'-74 d') and p_date  and (d_date+interval'74 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_74
			    	,sum(case when d_date between (p_date+interval'-75 d') and p_date  and (d_date+interval'75 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_75
			    	,sum(case when d_date between (p_date+interval'-76 d') and p_date  and (d_date+interval'76 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_76
			    	,sum(case when d_date between (p_date+interval'-77 d') and p_date  and (d_date+interval'77 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_77
			    	,sum(case when d_date between (p_date+interval'-78 d') and p_date  and (d_date+interval'78 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_78
			    	,sum(case when d_date between (p_date+interval'-79 d') and p_date  and (d_date+interval'79 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_79
			    	,sum(case when d_date between (p_date+interval'-80 d') and p_date  and (d_date+interval'80 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_80
			    	,sum(case when d_date between (p_date+interval'-81 d') and p_date  and (d_date+interval'81 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_81
			    	,sum(case when d_date between (p_date+interval'-82 d') and p_date  and (d_date+interval'82 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_82
			    	,sum(case when d_date between (p_date+interval'-83 d') and p_date  and (d_date+interval'83 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_83
			    	,sum(case when d_date between (p_date+interval'-84 d') and p_date  and (d_date+interval'84 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_84
			    	,sum(case when d_date between (p_date+interval'-85 d') and p_date  and (d_date+interval'85 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_85
			    	,sum(case when d_date between (p_date+interval'-86 d') and p_date  and (d_date+interval'86 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_86
			    	,sum(case when d_date between (p_date+interval'-87 d') and p_date  and (d_date+interval'87 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_87
			    	,sum(case when d_date between (p_date+interval'-88 d') and p_date  and (d_date+interval'88 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_88
			    	,sum(case when d_date between (p_date+interval'-89 d') and p_date  and (d_date+interval'89 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_89
			    	,sum(case when d_date between (p_date+interval'-90 d') and p_date  and (d_date+interval'90 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_90
			    	,sum(case when d_date between (p_date+interval'-91 d') and p_date  and (d_date+interval'91 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_91
			    	,sum(case when d_date between (p_date+interval'-92 d') and p_date  and (d_date+interval'92 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_92
			    	,sum(case when d_date between (p_date+interval'-93 d') and p_date  and (d_date+interval'93 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_93
			    	,sum(case when d_date between (p_date+interval'-94 d') and p_date  and (d_date+interval'94 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_94
			    	,sum(case when d_date between (p_date+interval'-95 d') and p_date  and (d_date+interval'95 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_95
			    	,sum(case when d_date between (p_date+interval'-96 d') and p_date  and (d_date+interval'96 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_96
			    	,sum(case when d_date between (p_date+interval'-97 d') and p_date  and (d_date+interval'97 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_97
			    	,sum(case when d_date between (p_date+interval'-98 d') and p_date  and (d_date+interval'98 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_98
			    	,sum(case when d_date between (p_date+interval'-99 d') and p_date  and (d_date+interval'99 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_99
			    	,sum(case when d_date between (p_date+interval'-100 d') and p_date  and (d_date+interval'100 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_100
			    	,sum(case when d_date between (p_date+interval'-101 d') and p_date  and (d_date+interval'101 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_101
			    	,sum(case when d_date between (p_date+interval'-102 d') and p_date  and (d_date+interval'102 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_102
			    	,sum(case when d_date between (p_date+interval'-103 d') and p_date  and (d_date+interval'103 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_103
			    	,sum(case when d_date between (p_date+interval'-104 d') and p_date  and (d_date+interval'104 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_104
			    	,sum(case when d_date between (p_date+interval'-105 d') and p_date  and (d_date+interval'105 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_105
			    	,sum(case when d_date between (p_date+interval'-106 d') and p_date  and (d_date+interval'106 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_106
			    	,sum(case when d_date between (p_date+interval'-107 d') and p_date  and (d_date+interval'107 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_107
			    	,sum(case when d_date between (p_date+interval'-108 d') and p_date  and (d_date+interval'108 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_108
			    	,sum(case when d_date between (p_date+interval'-109 d') and p_date  and (d_date+interval'109 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_109
			    	,sum(case when d_date between (p_date+interval'-110 d') and p_date  and (d_date+interval'110 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_110
			    	,sum(case when d_date between (p_date+interval'-111 d') and p_date  and (d_date+interval'111 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_111
			    	,sum(case when d_date between (p_date+interval'-112 d') and p_date  and (d_date+interval'112 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_112
			    	,sum(case when d_date between (p_date+interval'-113 d') and p_date  and (d_date+interval'113 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_113
			    	,sum(case when d_date between (p_date+interval'-114 d') and p_date  and (d_date+interval'114 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_114
			    	,sum(case when d_date between (p_date+interval'-115 d') and p_date  and (d_date+interval'115 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_115
			    	,sum(case when d_date between (p_date+interval'-116 d') and p_date  and (d_date+interval'116 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_116
			    	,sum(case when d_date between (p_date+interval'-117 d') and p_date  and (d_date+interval'117 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_117
			    	,sum(case when d_date between (p_date+interval'-118 d') and p_date  and (d_date+interval'118 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_118
			    	,sum(case when d_date between (p_date+interval'-119 d') and p_date  and (d_date+interval'119 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_119
			    	,sum(case when d_date between (p_date+interval'-120 d') and p_date  and (d_date+interval'120 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_120
			    	,sum(case when d_date between (p_date+interval'-121 d') and p_date  and (d_date+interval'121 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_121
			    	,sum(case when d_date between (p_date+interval'-122 d') and p_date  and (d_date+interval'122 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_122
			    	,sum(case when d_date between (p_date+interval'-123 d') and p_date  and (d_date+interval'123 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_123
			    	,sum(case when d_date between (p_date+interval'-124 d') and p_date  and (d_date+interval'124 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_124
			    	,sum(case when d_date between (p_date+interval'-125 d') and p_date  and (d_date+interval'125 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_125
			    	,sum(case when d_date between (p_date+interval'-126 d') and p_date  and (d_date+interval'126 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_126
			    	,sum(case when d_date between (p_date+interval'-127 d') and p_date  and (d_date+interval'127 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_127
			    	,sum(case when d_date between (p_date+interval'-128 d') and p_date  and (d_date+interval'128 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_128
			    	,sum(case when d_date between (p_date+interval'-129 d') and p_date  and (d_date+interval'129 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_129
			    	,sum(case when d_date between (p_date+interval'-130 d') and p_date  and (d_date+interval'130 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_130
			    	,sum(case when d_date between (p_date+interval'-131 d') and p_date  and (d_date+interval'131 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_131
			    	,sum(case when d_date between (p_date+interval'-132 d') and p_date  and (d_date+interval'132 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_132
			    	,sum(case when d_date between (p_date+interval'-133 d') and p_date  and (d_date+interval'133 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_133
			    	,sum(case when d_date between (p_date+interval'-134 d') and p_date  and (d_date+interval'134 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_134
			    	,sum(case when d_date between (p_date+interval'-135 d') and p_date  and (d_date+interval'135 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_135
			    	,sum(case when d_date between (p_date+interval'-136 d') and p_date  and (d_date+interval'136 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_136
			    	,sum(case when d_date between (p_date+interval'-137 d') and p_date  and (d_date+interval'137 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_137
			    	,sum(case when d_date between (p_date+interval'-138 d') and p_date  and (d_date+interval'138 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_138
			    	,sum(case when d_date between (p_date+interval'-139 d') and p_date  and (d_date+interval'139 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_139
			    	,sum(case when d_date between (p_date+interval'-140 d') and p_date  and (d_date+interval'140 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_140
			    	,sum(case when d_date between (p_date+interval'-141 d') and p_date  and (d_date+interval'141 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_141
			    	,sum(case when d_date between (p_date+interval'-142 d') and p_date  and (d_date+interval'142 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_142
			    	,sum(case when d_date between (p_date+interval'-143 d') and p_date  and (d_date+interval'143 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_143
			    	,sum(case when d_date between (p_date+interval'-144 d') and p_date  and (d_date+interval'144 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_144
			    	,sum(case when d_date between (p_date+interval'-145 d') and p_date  and (d_date+interval'145 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_145
			    	,sum(case when d_date between (p_date+interval'-146 d') and p_date  and (d_date+interval'146 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_146
			    	,sum(case when d_date between (p_date+interval'-147 d') and p_date  and (d_date+interval'147 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_147
			    	,sum(case when d_date between (p_date+interval'-148 d') and p_date  and (d_date+interval'148 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_148
			    	,sum(case when d_date between (p_date+interval'-149 d') and p_date  and (d_date+interval'149 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_149
			    	,sum(case when d_date between (p_date+interval'-150 d') and p_date  and (d_date+interval'150 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_150
			    	,sum(case when d_date between (p_date+interval'-151 d') and p_date  and (d_date+interval'151 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_151
			    	,sum(case when d_date between (p_date+interval'-152 d') and p_date  and (d_date+interval'152 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_152
			    	,sum(case when d_date between (p_date+interval'-153 d') and p_date  and (d_date+interval'153 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_153
			    	,sum(case when d_date between (p_date+interval'-154 d') and p_date  and (d_date+interval'154 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_154
			    	,sum(case when d_date between (p_date+interval'-155 d') and p_date  and (d_date+interval'155 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_155
			    	,sum(case when d_date between (p_date+interval'-156 d') and p_date  and (d_date+interval'156 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_156
			    	,sum(case when d_date between (p_date+interval'-157 d') and p_date  and (d_date+interval'157 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_157
			    	,sum(case when d_date between (p_date+interval'-158 d') and p_date  and (d_date+interval'158 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_158
			    	,sum(case when d_date between (p_date+interval'-159 d') and p_date  and (d_date+interval'159 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_159
			    	,sum(case when d_date between (p_date+interval'-160 d') and p_date  and (d_date+interval'160 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_160
			    	,sum(case when d_date between (p_date+interval'-161 d') and p_date  and (d_date+interval'161 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_161
			    	,sum(case when d_date between (p_date+interval'-162 d') and p_date  and (d_date+interval'162 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_162
			    	,sum(case when d_date between (p_date+interval'-163 d') and p_date  and (d_date+interval'163 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_163
			    	,sum(case when d_date between (p_date+interval'-164 d') and p_date  and (d_date+interval'164 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_164
			    	,sum(case when d_date between (p_date+interval'-165 d') and p_date  and (d_date+interval'165 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_165
			    	,sum(case when d_date between (p_date+interval'-166 d') and p_date  and (d_date+interval'166 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_166
			    	,sum(case when d_date between (p_date+interval'-167 d') and p_date  and (d_date+interval'167 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_167
			    	,sum(case when d_date between (p_date+interval'-168 d') and p_date  and (d_date+interval'168 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_168
			    	,sum(case when d_date between (p_date+interval'-169 d') and p_date  and (d_date+interval'169 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_169
			    	,sum(case when d_date between (p_date+interval'-170 d') and p_date  and (d_date+interval'170 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_170
			    	,sum(case when d_date between (p_date+interval'-171 d') and p_date  and (d_date+interval'171 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_171
			    	,sum(case when d_date between (p_date+interval'-172 d') and p_date  and (d_date+interval'172 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_172
			    	,sum(case when d_date between (p_date+interval'-173 d') and p_date  and (d_date+interval'173 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_173
			    	,sum(case when d_date between (p_date+interval'-174 d') and p_date  and (d_date+interval'174 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_174
			    	,sum(case when d_date between (p_date+interval'-175 d') and p_date  and (d_date+interval'175 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_175
			    	,sum(case when d_date between (p_date+interval'-176 d') and p_date  and (d_date+interval'176 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_176
			    	,sum(case when d_date between (p_date+interval'-177 d') and p_date  and (d_date+interval'177 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_177
			    	,sum(case when d_date between (p_date+interval'-178 d') and p_date  and (d_date+interval'178 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_178
			    	,sum(case when d_date between (p_date+interval'-179 d') and p_date  and (d_date+interval'179 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_179
			    	,sum(case when d_date between (p_date+interval'-180 d') and p_date  and (d_date+interval'180 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_180
			    	,sum(case when d_date between (p_date+interval'-181 d') and p_date  and (d_date+interval'181 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_181
			    	,sum(case when d_date between (p_date+interval'-182 d') and p_date  and (d_date+interval'182 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_182
			    	,sum(case when d_date between (p_date+interval'-183 d') and p_date  and (d_date+interval'183 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_183
			    	,sum(case when d_date between (p_date+interval'-184 d') and p_date  and (d_date+interval'184 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_184
			    	,sum(case when d_date between (p_date+interval'-185 d') and p_date  and (d_date+interval'185 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_185
			    	,sum(case when d_date between (p_date+interval'-186 d') and p_date  and (d_date+interval'186 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_186
			    	,sum(case when d_date between (p_date+interval'-187 d') and p_date  and (d_date+interval'187 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_187
			    	,sum(case when d_date between (p_date+interval'-188 d') and p_date  and (d_date+interval'188 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_188
			    	,sum(case when d_date between (p_date+interval'-189 d') and p_date  and (d_date+interval'189 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_189
			    	,sum(case when d_date between (p_date+interval'-190 d') and p_date  and (d_date+interval'190 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_190
			    	,sum(case when d_date between (p_date+interval'-191 d') and p_date  and (d_date+interval'191 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_191
			    	,sum(case when d_date between (p_date+interval'-192 d') and p_date  and (d_date+interval'192 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_192
			    	,sum(case when d_date between (p_date+interval'-193 d') and p_date  and (d_date+interval'193 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_193
			    	,sum(case when d_date between (p_date+interval'-194 d') and p_date  and (d_date+interval'194 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_194
			    	,sum(case when d_date between (p_date+interval'-195 d') and p_date  and (d_date+interval'195 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_195
			    	,sum(case when d_date between (p_date+interval'-196 d') and p_date  and (d_date+interval'196 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_196
			    	,sum(case when d_date between (p_date+interval'-197 d') and p_date  and (d_date+interval'197 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_197
			    	,sum(case when d_date between (p_date+interval'-198 d') and p_date  and (d_date+interval'198 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_198
			    	,sum(case when d_date between (p_date+interval'-199 d') and p_date  and (d_date+interval'199 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_199
			    	,sum(case when d_date between (p_date+interval'-200 d') and p_date  and (d_date+interval'200 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_200
			    	,sum(case when d_date between (p_date+interval'-201 d') and p_date  and (d_date+interval'201 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_201
			    	,sum(case when d_date between (p_date+interval'-202 d') and p_date  and (d_date+interval'202 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_202
			    	,sum(case when d_date between (p_date+interval'-203 d') and p_date  and (d_date+interval'203 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_203
			    	,sum(case when d_date between (p_date+interval'-204 d') and p_date  and (d_date+interval'204 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_204
			    	,sum(case when d_date between (p_date+interval'-205 d') and p_date  and (d_date+interval'205 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_205
			    	,sum(case when d_date between (p_date+interval'-206 d') and p_date  and (d_date+interval'206 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_206
			    	,sum(case when d_date between (p_date+interval'-207 d') and p_date  and (d_date+interval'207 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_207
			    	,sum(case when d_date between (p_date+interval'-208 d') and p_date  and (d_date+interval'208 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_208
			    	,sum(case when d_date between (p_date+interval'-209 d') and p_date  and (d_date+interval'209 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_209
			    	,sum(case when d_date between (p_date+interval'-210 d') and p_date  and (d_date+interval'210 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_210
			    	,sum(case when d_date between (p_date+interval'-211 d') and p_date  and (d_date+interval'211 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_211
			    	,sum(case when d_date between (p_date+interval'-212 d') and p_date  and (d_date+interval'212 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_212
			    	,sum(case when d_date between (p_date+interval'-213 d') and p_date  and (d_date+interval'213 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_213
			    	,sum(case when d_date between (p_date+interval'-214 d') and p_date  and (d_date+interval'214 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_214
			    	,sum(case when d_date between (p_date+interval'-215 d') and p_date  and (d_date+interval'215 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_215
			    	,sum(case when d_date between (p_date+interval'-216 d') and p_date  and (d_date+interval'216 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_216
			    	,sum(case when d_date between (p_date+interval'-217 d') and p_date  and (d_date+interval'217 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_217
			    	,sum(case when d_date between (p_date+interval'-218 d') and p_date  and (d_date+interval'218 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_218
			    	,sum(case when d_date between (p_date+interval'-219 d') and p_date  and (d_date+interval'219 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_219
			    	,sum(case when d_date between (p_date+interval'-220 d') and p_date  and (d_date+interval'220 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_220
			    	,sum(case when d_date between (p_date+interval'-221 d') and p_date  and (d_date+interval'221 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_221
			    	,sum(case when d_date between (p_date+interval'-222 d') and p_date  and (d_date+interval'222 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_222
			    	,sum(case when d_date between (p_date+interval'-223 d') and p_date  and (d_date+interval'223 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_223
			    	,sum(case when d_date between (p_date+interval'-224 d') and p_date  and (d_date+interval'224 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_224
			    	,sum(case when d_date between (p_date+interval'-225 d') and p_date  and (d_date+interval'225 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_225
			    	,sum(case when d_date between (p_date+interval'-226 d') and p_date  and (d_date+interval'226 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_226
			    	,sum(case when d_date between (p_date+interval'-227 d') and p_date  and (d_date+interval'227 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_227
			    	,sum(case when d_date between (p_date+interval'-228 d') and p_date  and (d_date+interval'228 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_228
			    	,sum(case when d_date between (p_date+interval'-229 d') and p_date  and (d_date+interval'229 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_229
			    	,sum(case when d_date between (p_date+interval'-230 d') and p_date  and (d_date+interval'230 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_230
			    	,sum(case when d_date between (p_date+interval'-231 d') and p_date  and (d_date+interval'231 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_231
			    	,sum(case when d_date between (p_date+interval'-232 d') and p_date  and (d_date+interval'232 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_232
			    	,sum(case when d_date between (p_date+interval'-233 d') and p_date  and (d_date+interval'233 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_233
			    	,sum(case when d_date between (p_date+interval'-234 d') and p_date  and (d_date+interval'234 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_234
			    	,sum(case when d_date between (p_date+interval'-235 d') and p_date  and (d_date+interval'235 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_235
			    	,sum(case when d_date between (p_date+interval'-236 d') and p_date  and (d_date+interval'236 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_236
			    	,sum(case when d_date between (p_date+interval'-237 d') and p_date  and (d_date+interval'237 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_237
			    	,sum(case when d_date between (p_date+interval'-238 d') and p_date  and (d_date+interval'238 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_238
			    	,sum(case when d_date between (p_date+interval'-239 d') and p_date  and (d_date+interval'239 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_239
			    	,sum(case when d_date between (p_date+interval'-240 d') and p_date  and (d_date+interval'240 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_240
			    	,sum(case when d_date between (p_date+interval'-241 d') and p_date  and (d_date+interval'241 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_241
			    	,sum(case when d_date between (p_date+interval'-242 d') and p_date  and (d_date+interval'242 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_242
			    	,sum(case when d_date between (p_date+interval'-243 d') and p_date  and (d_date+interval'243 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_243
			    	,sum(case when d_date between (p_date+interval'-244 d') and p_date  and (d_date+interval'244 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_244
			    	,sum(case when d_date between (p_date+interval'-245 d') and p_date  and (d_date+interval'245 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_245
			    	,sum(case when d_date between (p_date+interval'-246 d') and p_date  and (d_date+interval'246 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_246
			    	,sum(case when d_date between (p_date+interval'-247 d') and p_date  and (d_date+interval'247 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_247
			    	,sum(case when d_date between (p_date+interval'-248 d') and p_date  and (d_date+interval'248 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_248
			    	,sum(case when d_date between (p_date+interval'-249 d') and p_date  and (d_date+interval'249 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_249
			    	,sum(case when d_date between (p_date+interval'-250 d') and p_date  and (d_date+interval'250 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_250
			    	,sum(case when d_date between (p_date+interval'-251 d') and p_date  and (d_date+interval'251 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_251
			    	,sum(case when d_date between (p_date+interval'-252 d') and p_date  and (d_date+interval'252 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_252
			    	,sum(case when d_date between (p_date+interval'-253 d') and p_date  and (d_date+interval'253 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_253
			    	,sum(case when d_date between (p_date+interval'-254 d') and p_date  and (d_date+interval'254 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_254
			    	,sum(case when d_date between (p_date+interval'-255 d') and p_date  and (d_date+interval'255 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_255
			    	,sum(case when d_date between (p_date+interval'-256 d') and p_date  and (d_date+interval'256 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_256
			    	,sum(case when d_date between (p_date+interval'-257 d') and p_date  and (d_date+interval'257 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_257
			    	,sum(case when d_date between (p_date+interval'-258 d') and p_date  and (d_date+interval'258 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_258
			    	,sum(case when d_date between (p_date+interval'-259 d') and p_date  and (d_date+interval'259 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_259
			    	,sum(case when d_date between (p_date+interval'-260 d') and p_date  and (d_date+interval'260 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_260
			    	,sum(case when d_date between (p_date+interval'-261 d') and p_date  and (d_date+interval'261 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_261
			    	,sum(case when d_date between (p_date+interval'-262 d') and p_date  and (d_date+interval'262 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_262
			    	,sum(case when d_date between (p_date+interval'-263 d') and p_date  and (d_date+interval'263 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_263
			    	,sum(case when d_date between (p_date+interval'-264 d') and p_date  and (d_date+interval'264 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_264
			    	,sum(case when d_date between (p_date+interval'-265 d') and p_date  and (d_date+interval'265 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_265
			    	,sum(case when d_date between (p_date+interval'-266 d') and p_date  and (d_date+interval'266 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_266
			    	,sum(case when d_date between (p_date+interval'-267 d') and p_date  and (d_date+interval'267 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_267
			    	,sum(case when d_date between (p_date+interval'-268 d') and p_date  and (d_date+interval'268 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_268
			    	,sum(case when d_date between (p_date+interval'-269 d') and p_date  and (d_date+interval'269 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_269
			    	,sum(case when d_date between (p_date+interval'-270 d') and p_date  and (d_date+interval'270 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_270
			    	,sum(case when d_date between (p_date+interval'-271 d') and p_date  and (d_date+interval'271 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_271
			    	,sum(case when d_date between (p_date+interval'-272 d') and p_date  and (d_date+interval'272 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_272
			    	,sum(case when d_date between (p_date+interval'-273 d') and p_date  and (d_date+interval'273 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_273
			    	,sum(case when d_date between (p_date+interval'-274 d') and p_date  and (d_date+interval'274 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_274
			    	,sum(case when d_date between (p_date+interval'-275 d') and p_date  and (d_date+interval'275 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_275
			    	,sum(case when d_date between (p_date+interval'-276 d') and p_date  and (d_date+interval'276 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_276
			    	,sum(case when d_date between (p_date+interval'-277 d') and p_date  and (d_date+interval'277 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_277
			    	,sum(case when d_date between (p_date+interval'-278 d') and p_date  and (d_date+interval'278 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_278
			    	,sum(case when d_date between (p_date+interval'-279 d') and p_date  and (d_date+interval'279 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_279
			    	,sum(case when d_date between (p_date+interval'-280 d') and p_date  and (d_date+interval'280 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_280
			    	,sum(case when d_date between (p_date+interval'-281 d') and p_date  and (d_date+interval'281 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_281
			    	,sum(case when d_date between (p_date+interval'-282 d') and p_date  and (d_date+interval'282 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_282
			    	,sum(case when d_date between (p_date+interval'-283 d') and p_date  and (d_date+interval'283 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_283
			    	,sum(case when d_date between (p_date+interval'-284 d') and p_date  and (d_date+interval'284 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_284
			    	,sum(case when d_date between (p_date+interval'-285 d') and p_date  and (d_date+interval'285 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_285
			    	,sum(case when d_date between (p_date+interval'-286 d') and p_date  and (d_date+interval'286 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_286
			    	,sum(case when d_date between (p_date+interval'-287 d') and p_date  and (d_date+interval'287 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_287
			    	,sum(case when d_date between (p_date+interval'-288 d') and p_date  and (d_date+interval'288 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_288
			    	,sum(case when d_date between (p_date+interval'-289 d') and p_date  and (d_date+interval'289 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_289
			    	,sum(case when d_date between (p_date+interval'-290 d') and p_date  and (d_date+interval'290 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_290
			    	,sum(case when d_date between (p_date+interval'-291 d') and p_date  and (d_date+interval'291 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_291
			    	,sum(case when d_date between (p_date+interval'-292 d') and p_date  and (d_date+interval'292 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_292
			    	,sum(case when d_date between (p_date+interval'-293 d') and p_date  and (d_date+interval'293 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_293
			    	,sum(case when d_date between (p_date+interval'-294 d') and p_date  and (d_date+interval'294 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_294
			    	,sum(case when d_date between (p_date+interval'-295 d') and p_date  and (d_date+interval'295 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_295
			    	,sum(case when d_date between (p_date+interval'-296 d') and p_date  and (d_date+interval'296 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_296
			    	,sum(case when d_date between (p_date+interval'-297 d') and p_date  and (d_date+interval'297 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_297
			    	,sum(case when d_date between (p_date+interval'-298 d') and p_date  and (d_date+interval'298 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_298
			    	,sum(case when d_date between (p_date+interval'-299 d') and p_date  and (d_date+interval'299 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_299
			    	,sum(case when d_date between (p_date+interval'-300 d') and p_date  and (d_date+interval'300 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_300
			    	,sum(case when d_date between (p_date+interval'-301 d') and p_date  and (d_date+interval'301 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_301
			    	,sum(case when d_date between (p_date+interval'-302 d') and p_date  and (d_date+interval'302 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_302
			    	,sum(case when d_date between (p_date+interval'-303 d') and p_date  and (d_date+interval'303 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_303
			    	,sum(case when d_date between (p_date+interval'-304 d') and p_date  and (d_date+interval'304 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_304
			    	,sum(case when d_date between (p_date+interval'-305 d') and p_date  and (d_date+interval'305 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_305
			    	,sum(case when d_date between (p_date+interval'-306 d') and p_date  and (d_date+interval'306 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_306
			    	,sum(case when d_date between (p_date+interval'-307 d') and p_date  and (d_date+interval'307 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_307
			    	,sum(case when d_date between (p_date+interval'-308 d') and p_date  and (d_date+interval'308 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_308
			    	,sum(case when d_date between (p_date+interval'-309 d') and p_date  and (d_date+interval'309 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_309
			    	,sum(case when d_date between (p_date+interval'-310 d') and p_date  and (d_date+interval'310 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_310
			    	,sum(case when d_date between (p_date+interval'-311 d') and p_date  and (d_date+interval'311 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_311
			    	,sum(case when d_date between (p_date+interval'-312 d') and p_date  and (d_date+interval'312 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_312
			    	,sum(case when d_date between (p_date+interval'-313 d') and p_date  and (d_date+interval'313 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_313
			    	,sum(case when d_date between (p_date+interval'-314 d') and p_date  and (d_date+interval'314 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_314
			    	,sum(case when d_date between (p_date+interval'-315 d') and p_date  and (d_date+interval'315 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_315
			    	,sum(case when d_date between (p_date+interval'-316 d') and p_date  and (d_date+interval'316 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_316
			    	,sum(case when d_date between (p_date+interval'-317 d') and p_date  and (d_date+interval'317 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_317
			    	,sum(case when d_date between (p_date+interval'-318 d') and p_date  and (d_date+interval'318 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_318
			    	,sum(case when d_date between (p_date+interval'-319 d') and p_date  and (d_date+interval'319 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_319
			    	,sum(case when d_date between (p_date+interval'-320 d') and p_date  and (d_date+interval'320 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_320
			    	,sum(case when d_date between (p_date+interval'-321 d') and p_date  and (d_date+interval'321 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_321
			    	,sum(case when d_date between (p_date+interval'-322 d') and p_date  and (d_date+interval'322 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_322
			    	,sum(case when d_date between (p_date+interval'-323 d') and p_date  and (d_date+interval'323 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_323
			    	,sum(case when d_date between (p_date+interval'-324 d') and p_date  and (d_date+interval'324 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_324
			    	,sum(case when d_date between (p_date+interval'-325 d') and p_date  and (d_date+interval'325 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_325
			    	,sum(case when d_date between (p_date+interval'-326 d') and p_date  and (d_date+interval'326 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_326
			    	,sum(case when d_date between (p_date+interval'-327 d') and p_date  and (d_date+interval'327 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_327
			    	,sum(case when d_date between (p_date+interval'-328 d') and p_date  and (d_date+interval'328 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_328
			    	,sum(case when d_date between (p_date+interval'-329 d') and p_date  and (d_date+interval'329 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_329
			    	,sum(case when d_date between (p_date+interval'-330 d') and p_date  and (d_date+interval'330 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_330
			    	,sum(case when d_date between (p_date+interval'-331 d') and p_date  and (d_date+interval'331 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_331
			    	,sum(case when d_date between (p_date+interval'-332 d') and p_date  and (d_date+interval'332 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_332
			    	,sum(case when d_date between (p_date+interval'-333 d') and p_date  and (d_date+interval'333 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_333
			    	,sum(case when d_date between (p_date+interval'-334 d') and p_date  and (d_date+interval'334 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_334
			    	,sum(case when d_date between (p_date+interval'-335 d') and p_date  and (d_date+interval'335 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_335
			    	,sum(case when d_date between (p_date+interval'-336 d') and p_date  and (d_date+interval'336 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_336
			    	,sum(case when d_date between (p_date+interval'-337 d') and p_date  and (d_date+interval'337 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_337
			    	,sum(case when d_date between (p_date+interval'-338 d') and p_date  and (d_date+interval'338 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_338
			    	,sum(case when d_date between (p_date+interval'-339 d') and p_date  and (d_date+interval'339 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_339
			    	,sum(case when d_date between (p_date+interval'-340 d') and p_date  and (d_date+interval'340 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_340
			    	,sum(case when d_date between (p_date+interval'-341 d') and p_date  and (d_date+interval'341 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_341
			    	,sum(case when d_date between (p_date+interval'-342 d') and p_date  and (d_date+interval'342 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_342
			    	,sum(case when d_date between (p_date+interval'-343 d') and p_date  and (d_date+interval'343 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_343
			    	,sum(case when d_date between (p_date+interval'-344 d') and p_date  and (d_date+interval'344 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_344
			    	,sum(case when d_date between (p_date+interval'-345 d') and p_date  and (d_date+interval'345 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_345
			    	,sum(case when d_date between (p_date+interval'-346 d') and p_date  and (d_date+interval'346 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_346
			    	,sum(case when d_date between (p_date+interval'-347 d') and p_date  and (d_date+interval'347 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_347
			    	,sum(case when d_date between (p_date+interval'-348 d') and p_date  and (d_date+interval'348 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_348
			    	,sum(case when d_date between (p_date+interval'-349 d') and p_date  and (d_date+interval'349 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_349
			    	,sum(case when d_date between (p_date+interval'-350 d') and p_date  and (d_date+interval'350 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_350
			    	,sum(case when d_date between (p_date+interval'-351 d') and p_date  and (d_date+interval'351 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_351
			    	,sum(case when d_date between (p_date+interval'-352 d') and p_date  and (d_date+interval'352 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_352
			    	,sum(case when d_date between (p_date+interval'-353 d') and p_date  and (d_date+interval'353 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_353
			    	,sum(case when d_date between (p_date+interval'-354 d') and p_date  and (d_date+interval'354 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_354
			    	,sum(case when d_date between (p_date+interval'-355 d') and p_date  and (d_date+interval'355 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_355
			    	,sum(case when d_date between (p_date+interval'-356 d') and p_date  and (d_date+interval'356 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_356
			    	,sum(case when d_date between (p_date+interval'-357 d') and p_date  and (d_date+interval'357 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_357
			    	,sum(case when d_date between (p_date+interval'-358 d') and p_date  and (d_date+interval'358 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_358
			    	,sum(case when d_date between (p_date+interval'-359 d') and p_date  and (d_date+interval'359 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_359
			    	,sum(case when d_date between (p_date+interval'-360 d') and p_date  and (d_date+interval'360 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_360
			    	,sum(case when d_date between (p_date+interval'-361 d') and p_date  and (d_date+interval'361 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_361
			    	,sum(case when d_date between (p_date+interval'-362 d') and p_date  and (d_date+interval'362 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_362
			    	,sum(case when d_date between (p_date+interval'-363 d') and p_date  and (d_date+interval'363 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_363
			    	,sum(case when d_date between (p_date+interval'-364 d') and p_date  and (d_date+interval'364 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_364
			    	,sum(case when d_date between (p_date+interval'-365 d') and p_date  and (d_date+interval'365 d')<=(current_date+interval'0 d')::date then pay_amt else null end) as pay_365
		    from newuser_tb n1
		    left join tmp_user_pay p1 on n1.uid=p1.uid and n1.d_date<=p1.p_date
				and p1.p_date <= (current_date+interval'0 days')::date
			left join (
        select campaign_id, max(vid) as vid
        from public.v_dim_ad_campaign_info
        group by campaign_id
    ) tc on n1.campaign_id = tc.campaign_id
		    group by
		     n1.d_date
		    ,n1.country_code
		    ,n1.ad_channel
		    ,n1.campaign_id
		)
		,tmp_camp_cost as(
			select to_timestamp(created_at)::date as d_date
			,upper(area) as country_code
			,upper(ad_channel) as ad_channel
			,campaign_id
			,split_part(max(campaign_name) ,'_',5) as vid
			,split_part(max(campaign_name) ,'_',9) as youhuashi
			,sum(impression) as impression
			,sum(click) as click
			,sum(cost_amount)*0.0001 as ad_cost
			from public.ad_cost_data_log
			where 1=1
			and created_date>='20240701'
			group by
			to_timestamp(created_at)::date,
			upper(area),
			upper(ad_channel),
			campaign_id
		)
		,tmp_primary as(
			select distinct tb.d_date,tb.country_code,tb.ad_channel,tb.campaign_id  ,coalesce(tc.vid,'UNKNOWN') as vid
			from(
			select distinct d_date,country_code,ad_channel,campaign_id from newuser_tb union all
			select distinct d_date,country_code,ad_channel,campaign_id from tmp_camp_cost
			)tb
			-- left join( select distinct campaign_id,vid from tmp_camp_cost )tc on tb.campaign_id=tc.campaign_id
			left join(
				select campaign_id,max(vid) as vid from public.v_dim_ad_campaign_info group by campaign_id
				)tc on tb.campaign_id=tc.campaign_id
		)

		select
		 t0.vid
		,t0.d_date
		,t0.country_code
		,t0.ad_channel
	    ,cc.country_name
	    ,cc.area
	    ,t0.ad_campaign_cnt
	    ,t0.adcost_campaign_cnt
		,t0.impression
		,t0.click
		,t0.ad_cost
		,t0.new_uv
		,t0.new_pay_uv
		,t0.new_payuv_total
		,t0.new_payamt_total
		,t0.current_diff
			,t0.pay_0
			,t0.pay_1
			,t0.pay_2
			,t0.pay_3
			,t0.pay_4
			,t0.pay_5
			,t0.pay_6
			,t0.pay_7
			,t0.pay_8
			,t0.pay_9
			,t0.pay_10
			,t0.pay_11
			,t0.pay_12
			,t0.pay_13
			,t0.pay_14
			,t0.pay_15
			,t0.pay_16
			,t0.pay_17
			,t0.pay_18
			,t0.pay_19
			,t0.pay_20
			,t0.pay_21
			,t0.pay_22
			,t0.pay_23
			,t0.pay_24
			,t0.pay_25
			,t0.pay_26
			,t0.pay_27
			,t0.pay_28
			,t0.pay_29
			,t0.pay_30
			,t0.pay_31
			,t0.pay_32
			,t0.pay_33
			,t0.pay_34
			,t0.pay_35
			,t0.pay_36
			,t0.pay_37
			,t0.pay_38
			,t0.pay_39
			,t0.pay_40
			,t0.pay_41
			,t0.pay_42
			,t0.pay_43
			,t0.pay_44
			,t0.pay_45
			,t0.pay_46
			,t0.pay_47
			,t0.pay_48
			,t0.pay_49
			,t0.pay_50
			,t0.pay_51
			,t0.pay_52
			,t0.pay_53
			,t0.pay_54
			,t0.pay_55
			,t0.pay_56
			,t0.pay_57
			,t0.pay_58
			,t0.pay_59
				,t0.pay_60
				,t0.pay_61
				,t0.pay_62
				,t0.pay_63
				,t0.pay_64
				,t0.pay_65
				,t0.pay_66
				,t0.pay_67
				,t0.pay_68
				,t0.pay_69
				,t0.pay_70
				,t0.pay_71
				,t0.pay_72
				,t0.pay_73
				,t0.pay_74
				,t0.pay_75
				,t0.pay_76
				,t0.pay_77
				,t0.pay_78
				,t0.pay_79
				,t0.pay_80
				,t0.pay_81
				,t0.pay_82
				,t0.pay_83
				,t0.pay_84
				,t0.pay_85
				,t0.pay_86
				,t0.pay_87
				,t0.pay_88
				,t0.pay_89
				,t0.pay_90
				,t0.pay_91
				,t0.pay_92
				,t0.pay_93
				,t0.pay_94
				,t0.pay_95
				,t0.pay_96
				,t0.pay_97
				,t0.pay_98
				,t0.pay_99
				,t0.pay_100
				,t0.pay_101
				,t0.pay_102
				,t0.pay_103
				,t0.pay_104
				,t0.pay_105
				,t0.pay_106
				,t0.pay_107
				,t0.pay_108
				,t0.pay_109
				,t0.pay_110
				,t0.pay_111
				,t0.pay_112
				,t0.pay_113
				,t0.pay_114
				,t0.pay_115
				,t0.pay_116
				,t0.pay_117
				,t0.pay_118
				,t0.pay_119
				,t0.pay_120
				,t0.pay_121
				,t0.pay_122
				,t0.pay_123
				,t0.pay_124
				,t0.pay_125
				,t0.pay_126
				,t0.pay_127
				,t0.pay_128
				,t0.pay_129
				,t0.pay_130
				,t0.pay_131
				,t0.pay_132
				,t0.pay_133
				,t0.pay_134
				,t0.pay_135
				,t0.pay_136
				,t0.pay_137
				,t0.pay_138
				,t0.pay_139
				,t0.pay_140
				,t0.pay_141
				,t0.pay_142
				,t0.pay_143
				,t0.pay_144
				,t0.pay_145
				,t0.pay_146
				,t0.pay_147
				,t0.pay_148
				,t0.pay_149
				,t0.pay_150
				,t0.pay_151
				,t0.pay_152
				,t0.pay_153
				,t0.pay_154
				,t0.pay_155
				,t0.pay_156
				,t0.pay_157
				,t0.pay_158
				,t0.pay_159
				,t0.pay_160
				,t0.pay_161
				,t0.pay_162
				,t0.pay_163
				,t0.pay_164
				,t0.pay_165
				,t0.pay_166
				,t0.pay_167
				,t0.pay_168
				,t0.pay_169
				,t0.pay_170
				,t0.pay_171
				,t0.pay_172
				,t0.pay_173
				,t0.pay_174
				,t0.pay_175
				,t0.pay_176
				,t0.pay_177
				,t0.pay_178
				,t0.pay_179
				,t0.pay_180
				,t0.pay_181
				,t0.pay_182
				,t0.pay_183
				,t0.pay_184
				,t0.pay_185
				,t0.pay_186
				,t0.pay_187
				,t0.pay_188
				,t0.pay_189
				,t0.pay_190
				,t0.pay_191
				,t0.pay_192
				,t0.pay_193
				,t0.pay_194
				,t0.pay_195
				,t0.pay_196
				,t0.pay_197
				,t0.pay_198
				,t0.pay_199
				,t0.pay_200
				,t0.pay_201
				,t0.pay_202
				,t0.pay_203
				,t0.pay_204
				,t0.pay_205
				,t0.pay_206
				,t0.pay_207
				,t0.pay_208
				,t0.pay_209
				,t0.pay_210
				,t0.pay_211
				,t0.pay_212
				,t0.pay_213
				,t0.pay_214
				,t0.pay_215
				,t0.pay_216
				,t0.pay_217
				,t0.pay_218
				,t0.pay_219
				,t0.pay_220
				,t0.pay_221
				,t0.pay_222
				,t0.pay_223
				,t0.pay_224
				,t0.pay_225
				,t0.pay_226
				,t0.pay_227
				,t0.pay_228
				,t0.pay_229
				,t0.pay_230
				,t0.pay_231
				,t0.pay_232
				,t0.pay_233
				,t0.pay_234
				,t0.pay_235
				,t0.pay_236
				,t0.pay_237
				,t0.pay_238
				,t0.pay_239
				,t0.pay_240
				,t0.pay_241
				,t0.pay_242
				,t0.pay_243
				,t0.pay_244
				,t0.pay_245
				,t0.pay_246
				,t0.pay_247
				,t0.pay_248
				,t0.pay_249
				,t0.pay_250
				,t0.pay_251
				,t0.pay_252
				,t0.pay_253
				,t0.pay_254
				,t0.pay_255
				,t0.pay_256
				,t0.pay_257
				,t0.pay_258
				,t0.pay_259
				,t0.pay_260
				,t0.pay_261
				,t0.pay_262
				,t0.pay_263
				,t0.pay_264
				,t0.pay_265
				,t0.pay_266
				,t0.pay_267
				,t0.pay_268
				,t0.pay_269
				,t0.pay_270
				,t0.pay_271
				,t0.pay_272
				,t0.pay_273
				,t0.pay_274
				,t0.pay_275
				,t0.pay_276
				,t0.pay_277
				,t0.pay_278
				,t0.pay_279
				,t0.pay_280
				,t0.pay_281
				,t0.pay_282
				,t0.pay_283
				,t0.pay_284
				,t0.pay_285
				,t0.pay_286
				,t0.pay_287
				,t0.pay_288
				,t0.pay_289
				,t0.pay_290
				,t0.pay_291
				,t0.pay_292
				,t0.pay_293
				,t0.pay_294
				,t0.pay_295
				,t0.pay_296
				,t0.pay_297
				,t0.pay_298
				,t0.pay_299
				,t0.pay_300
				,t0.pay_301
				,t0.pay_302
				,t0.pay_303
				,t0.pay_304
				,t0.pay_305
				,t0.pay_306
				,t0.pay_307
				,t0.pay_308
				,t0.pay_309
				,t0.pay_310
				,t0.pay_311
				,t0.pay_312
				,t0.pay_313
				,t0.pay_314
				,t0.pay_315
				,t0.pay_316
				,t0.pay_317
				,t0.pay_318
				,t0.pay_319
				,t0.pay_320
				,t0.pay_321
				,t0.pay_322
				,t0.pay_323
				,t0.pay_324
				,t0.pay_325
				,t0.pay_326
				,t0.pay_327
				,t0.pay_328
				,t0.pay_329
				,t0.pay_330
				,t0.pay_331
				,t0.pay_332
				,t0.pay_333
				,t0.pay_334
				,t0.pay_335
				,t0.pay_336
				,t0.pay_337
				,t0.pay_338
				,t0.pay_339
				,t0.pay_340
				,t0.pay_341
				,t0.pay_342
				,t0.pay_343
				,t0.pay_344
				,t0.pay_345
				,t0.pay_346
				,t0.pay_347
				,t0.pay_348
				,t0.pay_349
				,t0.pay_350
				,t0.pay_351
				,t0.pay_352
				,t0.pay_353
				,t0.pay_354
				,t0.pay_355
				,t0.pay_356
				,t0.pay_357
				,t0.pay_358
				,t0.pay_359
				,t0.pay_360
				,t0.pay_361
				,t0.pay_362
				,t0.pay_363
				,t0.pay_364
				,t0.pay_365
				,t0.attributed_pay_0
		from(
			select t0.d_date,t0.country_code,t0.ad_channel,t0.vid
			,count(distinct tc.campaign_id) as ad_campaign_cnt
			,count(distinct case when tc.ad_cost>0 then tc.campaign_id else null end) as adcost_campaign_cnt
			,sum(tc.impression) as impression
			,sum(tc.click) as click
			,sum(tc.ad_cost) as ad_cost
			,sum(tp.new_uv) as new_uv
			,sum(tp.new_pay_uv) as new_pay_uv
			,sum(tp.new_payuv_total) as new_payuv_total
			,sum(tp.new_payamt_total) as new_payamt_total
	    	,(current_date+interval'0 d')::date-t0.d_date as current_diff
				,sum(pay_0) as pay_0
				,sum(pay_1) as pay_1
				,sum(pay_2) as pay_2
				,sum(pay_3) as pay_3
				,sum(pay_4) as pay_4
				,sum(pay_5) as pay_5
				,sum(pay_6) as pay_6
				,sum(pay_7) as pay_7
				,sum(pay_8) as pay_8
				,sum(pay_9) as pay_9
				,sum(pay_10) as pay_10
				,sum(pay_11) as pay_11
				,sum(pay_12) as pay_12
				,sum(pay_13) as pay_13
				,sum(pay_14) as pay_14
				,sum(pay_15) as pay_15
				,sum(pay_16) as pay_16
				,sum(pay_17) as pay_17
				,sum(pay_18) as pay_18
				,sum(pay_19) as pay_19
				,sum(pay_20) as pay_20
				,sum(pay_21) as pay_21
				,sum(pay_22) as pay_22
				,sum(pay_23) as pay_23
				,sum(pay_24) as pay_24
				,sum(pay_25) as pay_25
				,sum(pay_26) as pay_26
				,sum(pay_27) as pay_27
				,sum(pay_28) as pay_28
				,sum(pay_29) as pay_29
				,sum(pay_30) as pay_30
				,sum(pay_31) as pay_31
				,sum(pay_32) as pay_32
				,sum(pay_33) as pay_33
				,sum(pay_34) as pay_34
				,sum(pay_35) as pay_35
				,sum(pay_36) as pay_36
				,sum(pay_37) as pay_37
				,sum(pay_38) as pay_38
				,sum(pay_39) as pay_39
				,sum(pay_40) as pay_40
				,sum(pay_41) as pay_41
				,sum(pay_42) as pay_42
				,sum(pay_43) as pay_43
				,sum(pay_44) as pay_44
				,sum(pay_45) as pay_45
				,sum(pay_46) as pay_46
				,sum(pay_47) as pay_47
				,sum(pay_48) as pay_48
				,sum(pay_49) as pay_49
				,sum(pay_50) as pay_50
				,sum(pay_51) as pay_51
				,sum(pay_52) as pay_52
				,sum(pay_53) as pay_53
				,sum(pay_54) as pay_54
				,sum(pay_55) as pay_55
				,sum(pay_56) as pay_56
				,sum(pay_57) as pay_57
				,sum(pay_58) as pay_58
				,sum(pay_59) as pay_59
					,sum(pay_60) as pay_60
					,sum(pay_61) as pay_61
					,sum(pay_62) as pay_62
					,sum(pay_63) as pay_63
					,sum(pay_64) as pay_64
					,sum(pay_65) as pay_65
					,sum(pay_66) as pay_66
					,sum(pay_67) as pay_67
					,sum(pay_68) as pay_68
					,sum(pay_69) as pay_69
					,sum(pay_70) as pay_70
					,sum(pay_71) as pay_71
					,sum(pay_72) as pay_72
					,sum(pay_73) as pay_73
					,sum(pay_74) as pay_74
					,sum(pay_75) as pay_75
					,sum(pay_76) as pay_76
					,sum(pay_77) as pay_77
					,sum(pay_78) as pay_78
					,sum(pay_79) as pay_79
					,sum(pay_80) as pay_80
					,sum(pay_81) as pay_81
					,sum(pay_82) as pay_82
					,sum(pay_83) as pay_83
					,sum(pay_84) as pay_84
					,sum(pay_85) as pay_85
					,sum(pay_86) as pay_86
					,sum(pay_87) as pay_87
					,sum(pay_88) as pay_88
					,sum(pay_89) as pay_89
					,sum(pay_90) as pay_90
					,sum(pay_91) as pay_91
					,sum(pay_92) as pay_92
					,sum(pay_93) as pay_93
					,sum(pay_94) as pay_94
					,sum(pay_95) as pay_95
					,sum(pay_96) as pay_96
					,sum(pay_97) as pay_97
					,sum(pay_98) as pay_98
					,sum(pay_99) as pay_99
					,sum(pay_100) as pay_100
					,sum(pay_101) as pay_101
					,sum(pay_102) as pay_102
					,sum(pay_103) as pay_103
					,sum(pay_104) as pay_104
					,sum(pay_105) as pay_105
					,sum(pay_106) as pay_106
					,sum(pay_107) as pay_107
					,sum(pay_108) as pay_108
					,sum(pay_109) as pay_109
					,sum(pay_110) as pay_110
					,sum(pay_111) as pay_111
					,sum(pay_112) as pay_112
					,sum(pay_113) as pay_113
					,sum(pay_114) as pay_114
					,sum(pay_115) as pay_115
					,sum(pay_116) as pay_116
					,sum(pay_117) as pay_117
					,sum(pay_118) as pay_118
					,sum(pay_119) as pay_119
					,sum(pay_120) as pay_120
					,sum(pay_121) as pay_121
					,sum(pay_122) as pay_122
					,sum(pay_123) as pay_123
					,sum(pay_124) as pay_124
					,sum(pay_125) as pay_125
					,sum(pay_126) as pay_126
					,sum(pay_127) as pay_127
					,sum(pay_128) as pay_128
					,sum(pay_129) as pay_129
					,sum(pay_130) as pay_130
					,sum(pay_131) as pay_131
					,sum(pay_132) as pay_132
					,sum(pay_133) as pay_133
					,sum(pay_134) as pay_134
					,sum(pay_135) as pay_135
					,sum(pay_136) as pay_136
					,sum(pay_137) as pay_137
					,sum(pay_138) as pay_138
					,sum(pay_139) as pay_139
					,sum(pay_140) as pay_140
					,sum(pay_141) as pay_141
					,sum(pay_142) as pay_142
					,sum(pay_143) as pay_143
					,sum(pay_144) as pay_144
					,sum(pay_145) as pay_145
					,sum(pay_146) as pay_146
					,sum(pay_147) as pay_147
					,sum(pay_148) as pay_148
					,sum(pay_149) as pay_149
					,sum(pay_150) as pay_150
					,sum(pay_151) as pay_151
					,sum(pay_152) as pay_152
					,sum(pay_153) as pay_153
					,sum(pay_154) as pay_154
					,sum(pay_155) as pay_155
					,sum(pay_156) as pay_156
					,sum(pay_157) as pay_157
					,sum(pay_158) as pay_158
					,sum(pay_159) as pay_159
					,sum(pay_160) as pay_160
					,sum(pay_161) as pay_161
					,sum(pay_162) as pay_162
					,sum(pay_163) as pay_163
					,sum(pay_164) as pay_164
					,sum(pay_165) as pay_165
					,sum(pay_166) as pay_166
					,sum(pay_167) as pay_167
					,sum(pay_168) as pay_168
					,sum(pay_169) as pay_169
					,sum(pay_170) as pay_170
					,sum(pay_171) as pay_171
					,sum(pay_172) as pay_172
					,sum(pay_173) as pay_173
					,sum(pay_174) as pay_174
					,sum(pay_175) as pay_175
					,sum(pay_176) as pay_176
					,sum(pay_177) as pay_177
					,sum(pay_178) as pay_178
					,sum(pay_179) as pay_179
					,sum(pay_180) as pay_180
					,sum(pay_181) as pay_181
					,sum(pay_182) as pay_182
					,sum(pay_183) as pay_183
					,sum(pay_184) as pay_184
					,sum(pay_185) as pay_185
					,sum(pay_186) as pay_186
					,sum(pay_187) as pay_187
					,sum(pay_188) as pay_188
					,sum(pay_189) as pay_189
					,sum(pay_190) as pay_190
					,sum(pay_191) as pay_191
					,sum(pay_192) as pay_192
					,sum(pay_193) as pay_193
					,sum(pay_194) as pay_194
					,sum(pay_195) as pay_195
					,sum(pay_196) as pay_196
					,sum(pay_197) as pay_197
					,sum(pay_198) as pay_198
					,sum(pay_199) as pay_199
					,sum(pay_200) as pay_200
					,sum(pay_201) as pay_201
					,sum(pay_202) as pay_202
					,sum(pay_203) as pay_203
					,sum(pay_204) as pay_204
					,sum(pay_205) as pay_205
					,sum(pay_206) as pay_206
					,sum(pay_207) as pay_207
					,sum(pay_208) as pay_208
					,sum(pay_209) as pay_209
					,sum(pay_210) as pay_210
					,sum(pay_211) as pay_211
					,sum(pay_212) as pay_212
					,sum(pay_213) as pay_213
					,sum(pay_214) as pay_214
					,sum(pay_215) as pay_215
					,sum(pay_216) as pay_216
					,sum(pay_217) as pay_217
					,sum(pay_218) as pay_218
					,sum(pay_219) as pay_219
					,sum(pay_220) as pay_220
					,sum(pay_221) as pay_221
					,sum(pay_222) as pay_222
					,sum(pay_223) as pay_223
					,sum(pay_224) as pay_224
					,sum(pay_225) as pay_225
					,sum(pay_226) as pay_226
					,sum(pay_227) as pay_227
					,sum(pay_228) as pay_228
					,sum(pay_229) as pay_229
					,sum(pay_230) as pay_230
					,sum(pay_231) as pay_231
					,sum(pay_232) as pay_232
					,sum(pay_233) as pay_233
					,sum(pay_234) as pay_234
					,sum(pay_235) as pay_235
					,sum(pay_236) as pay_236
					,sum(pay_237) as pay_237
					,sum(pay_238) as pay_238
					,sum(pay_239) as pay_239
					,sum(pay_240) as pay_240
					,sum(pay_241) as pay_241
					,sum(pay_242) as pay_242
					,sum(pay_243) as pay_243
					,sum(pay_244) as pay_244
					,sum(pay_245) as pay_245
					,sum(pay_246) as pay_246
					,sum(pay_247) as pay_247
					,sum(pay_248) as pay_248
					,sum(pay_249) as pay_249
					,sum(pay_250) as pay_250
					,sum(pay_251) as pay_251
					,sum(pay_252) as pay_252
					,sum(pay_253) as pay_253
					,sum(pay_254) as pay_254
					,sum(pay_255) as pay_255
					,sum(pay_256) as pay_256
					,sum(pay_257) as pay_257
					,sum(pay_258) as pay_258
					,sum(pay_259) as pay_259
					,sum(pay_260) as pay_260
					,sum(pay_261) as pay_261
					,sum(pay_262) as pay_262
					,sum(pay_263) as pay_263
					,sum(pay_264) as pay_264
					,sum(pay_265) as pay_265
					,sum(pay_266) as pay_266
					,sum(pay_267) as pay_267
					,sum(pay_268) as pay_268
					,sum(pay_269) as pay_269
					,sum(pay_270) as pay_270
					,sum(pay_271) as pay_271
					,sum(pay_272) as pay_272
					,sum(pay_273) as pay_273
					,sum(pay_274) as pay_274
					,sum(pay_275) as pay_275
					,sum(pay_276) as pay_276
					,sum(pay_277) as pay_277
					,sum(pay_278) as pay_278
					,sum(pay_279) as pay_279
					,sum(pay_280) as pay_280
					,sum(pay_281) as pay_281
					,sum(pay_282) as pay_282
					,sum(pay_283) as pay_283
					,sum(pay_284) as pay_284
					,sum(pay_285) as pay_285
					,sum(pay_286) as pay_286
					,sum(pay_287) as pay_287
					,sum(pay_288) as pay_288
					,sum(pay_289) as pay_289
					,sum(pay_290) as pay_290
					,sum(pay_291) as pay_291
					,sum(pay_292) as pay_292
					,sum(pay_293) as pay_293
					,sum(pay_294) as pay_294
					,sum(pay_295) as pay_295
					,sum(pay_296) as pay_296
					,sum(pay_297) as pay_297
					,sum(pay_298) as pay_298
					,sum(pay_299) as pay_299
					,sum(pay_300) as pay_300
					,sum(pay_301) as pay_301
					,sum(pay_302) as pay_302
					,sum(pay_303) as pay_303
					,sum(pay_304) as pay_304
					,sum(pay_305) as pay_305
					,sum(pay_306) as pay_306
					,sum(pay_307) as pay_307
					,sum(pay_308) as pay_308
					,sum(pay_309) as pay_309
					,sum(pay_310) as pay_310
					,sum(pay_311) as pay_311
					,sum(pay_312) as pay_312
					,sum(pay_313) as pay_313
					,sum(pay_314) as pay_314
					,sum(pay_315) as pay_315
					,sum(pay_316) as pay_316
					,sum(pay_317) as pay_317
					,sum(pay_318) as pay_318
					,sum(pay_319) as pay_319
					,sum(pay_320) as pay_320
					,sum(pay_321) as pay_321
					,sum(pay_322) as pay_322
					,sum(pay_323) as pay_323
					,sum(pay_324) as pay_324
					,sum(pay_325) as pay_325
					,sum(pay_326) as pay_326
					,sum(pay_327) as pay_327
					,sum(pay_328) as pay_328
					,sum(pay_329) as pay_329
					,sum(pay_330) as pay_330
					,sum(pay_331) as pay_331
					,sum(pay_332) as pay_332
					,sum(pay_333) as pay_333
					,sum(pay_334) as pay_334
					,sum(pay_335) as pay_335
					,sum(pay_336) as pay_336
					,sum(pay_337) as pay_337
					,sum(pay_338) as pay_338
					,sum(pay_339) as pay_339
					,sum(pay_340) as pay_340
					,sum(pay_341) as pay_341
					,sum(pay_342) as pay_342
					,sum(pay_343) as pay_343
					,sum(pay_344) as pay_344
					,sum(pay_345) as pay_345
					,sum(pay_346) as pay_346
					,sum(pay_347) as pay_347
					,sum(pay_348) as pay_348
					,sum(pay_349) as pay_349
					,sum(pay_350) as pay_350
					,sum(pay_351) as pay_351
					,sum(pay_352) as pay_352
					,sum(pay_353) as pay_353
					,sum(pay_354) as pay_354
					,sum(pay_355) as pay_355
					,sum(pay_356) as pay_356
					,sum(pay_357) as pay_357
					,sum(pay_358) as pay_358
					,sum(pay_359) as pay_359
					,sum(pay_360) as pay_360
					,sum(pay_361) as pay_361
					,sum(pay_362) as pay_362
					,sum(pay_363) as pay_363
					,sum(pay_364) as pay_364
					,sum(pay_365) as pay_365
					,sum(attributed_pay_0) as attributed_pay_0
	    	from tmp_primary t0
			left join tmp_camp_roi tp on t0.d_date=tp.d_date and t0.country_code=tp.country_code and t0.ad_channel=tp.ad_channel and t0.campaign_id=tp.campaign_id
			left join tmp_camp_cost tc on t0.d_date=tc.d_date and t0.country_code=tc.country_code and t0.ad_channel=tc.ad_channel and t0.campaign_id=tc.campaign_id
			group by t0.d_date,t0.country_code,t0.ad_channel,t0.vid
		)t0
		left join v_dim_country_area cc on t0.country_code=cc.country_code
		-- left join public."oversea-api_osd_videos" v1 on t0.vid=v1.id::text
		-- left join public."oversea-api_osd_categories" v2 on v1.category_id=v2.id
		-- where v1.id is not null
	;
