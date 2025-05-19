------------------------------------------
-- file: 剧报表_指标概览_全量_d.sql
-- author: xiaoj
-- time: 2025/5/16 18:18
-- description:
------------------------------------------
	set timezone ='UTC-0';

	truncate table analysis.dw_video_overview_tmp01;
	insert into analysis.dw_video_overview_tmp01
		with tmp_primary as(
			select distinct vid,d_date,country_code,ad_channel,country_name,area from(
				select distinct vid,d_date,country_code,ad_channel,country_name,area from analysis.dw_video_view_calc01 union all
				select distinct vid,d_date,country_code,ad_channel,country_name,area from analysis.dw_video_roi_calc0210
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
			,v1.complex
			,v1.category
			,v2.name as lang
		--
		,t0.d_date
		,t0.country_code
		,t0.ad_channel
		,t0.country_name
		,t0.area
		,t1.watch_uv
		,t1.watch_pv
		,t1.watch_duration
		,t1.eid_watch_cnt
		,t1.ad_unlock_uv
		,t1.ad_unlock_pv
		,t1.pay_uv
		,t1.pay_amt
		,t1.pay_k_amt
		,t1.pay_vip_amt
		--
	    ,t2.ad_campaign_cnt
	    ,t2.adcost_campaign_cnt
		,t2.impression
		,t2.click
		,t2.ad_cost
		,t2.new_uv
		,t2.new_pay_uv
		-- ,t2.current_diff
	    ,(current_date+interval'0 d')::date-t0.d_date as current_diff -- 距今天数差（绝对值）
		-- roi
			,t2.pay_0,t2.pay_1,t2.pay_2,t2.pay_3,t2.pay_4,t2.pay_5,t2.pay_6,t2.pay_7,t2.pay_8,t2.pay_9,t2.pay_10,t2.pay_11,t2.pay_12,t2.pay_13,t2.pay_14,t2.pay_15,t2.pay_16,t2.pay_17,t2.pay_18,t2.pay_19,t2.pay_20,t2.pay_21,t2.pay_22,t2.pay_23,t2.pay_24,t2.pay_25,t2.pay_26,t2.pay_27,t2.pay_28,t2.pay_29,t2.pay_30,t2.pay_31,t2.pay_32,t2.pay_33,t2.pay_34,t2.pay_35,t2.pay_36,t2.pay_37,t2.pay_38,t2.pay_39,t2.pay_40,t2.pay_41,t2.pay_42,t2.pay_43,t2.pay_44,t2.pay_45,t2.pay_46,t2.pay_47,t2.pay_48,t2.pay_49,t2.pay_50,t2.pay_51,t2.pay_52,t2.pay_53,t2.pay_54,t2.pay_55,t2.pay_56,t2.pay_57,t2.pay_58,t2.pay_59
	,t1."总消耗K币"
    ,t1."消耗充值K币"
    ,t1."消耗赠送K币"
    ,t1."新用户总消耗K币"
    ,t1."新用户消耗充值K币"
    ,t1."新用户消耗赠送K币"
    ,t1."老用户总消耗K币"
    ,t1."老用户消耗充值K币"
    ,t1."老用户消耗赠送K币"
	,drama_duration
	,t1.new_watch_uv
	,t1.old_watch_uv
	-- ,t1.new_pay_uv
	,t1.old_pay_uv
	,t1.new_pay_amt
	,t1.old_pay_amt
		-- roi
			,t2.pay_60,t2.pay_61,t2.pay_62,t2.pay_63,t2.pay_64,t2.pay_65,t2.pay_66,t2.pay_67,t2.pay_68,t2.pay_69,t2.pay_70,t2.pay_71,t2.pay_72,t2.pay_73,t2.pay_74,t2.pay_75,t2.pay_76,t2.pay_77,t2.pay_78,t2.pay_79,t2.pay_80,t2.pay_81,t2.pay_82,t2.pay_83,t2.pay_84,t2.pay_85,t2.pay_86,t2.pay_87,t2.pay_88,t2.pay_89,t2.pay_90,t2.pay_91,t2.pay_92,t2.pay_93,t2.pay_94,t2.pay_95,t2.pay_96,t2.pay_97,t2.pay_98,t2.pay_99,t2.pay_100,t2.pay_101,t2.pay_102,t2.pay_103,t2.pay_104,t2.pay_105,t2.pay_106,t2.pay_107,t2.pay_108,t2.pay_109,t2.pay_110,t2.pay_111,t2.pay_112,t2.pay_113,t2.pay_114,t2.pay_115,t2.pay_116,t2.pay_117,t2.pay_118,t2.pay_119,t2.pay_120,t2.pay_121,t2.pay_122,t2.pay_123,t2.pay_124,t2.pay_125,t2.pay_126,t2.pay_127,t2.pay_128,t2.pay_129,t2.pay_130,t2.pay_131,t2.pay_132,t2.pay_133,t2.pay_134,t2.pay_135,t2.pay_136,t2.pay_137,t2.pay_138,t2.pay_139,t2.pay_140,t2.pay_141,t2.pay_142,t2.pay_143,t2.pay_144,t2.pay_145,t2.pay_146,t2.pay_147,t2.pay_148,t2.pay_149,t2.pay_150,t2.pay_151,t2.pay_152,t2.pay_153,t2.pay_154,t2.pay_155,t2.pay_156,t2.pay_157,t2.pay_158,t2.pay_159,t2.pay_160,t2.pay_161,t2.pay_162,t2.pay_163,t2.pay_164,t2.pay_165,t2.pay_166,t2.pay_167,t2.pay_168,t2.pay_169,t2.pay_170,t2.pay_171,t2.pay_172,t2.pay_173,t2.pay_174,t2.pay_175,t2.pay_176,t2.pay_177,t2.pay_178,t2.pay_179,t2.pay_180,t2.pay_181,t2.pay_182,t2.pay_183,t2.pay_184,t2.pay_185,t2.pay_186,t2.pay_187,t2.pay_188,t2.pay_189,t2.pay_190,t2.pay_191,t2.pay_192,t2.pay_193,t2.pay_194,t2.pay_195,t2.pay_196,t2.pay_197,t2.pay_198,t2.pay_199,t2.pay_200,t2.pay_201,t2.pay_202,t2.pay_203,t2.pay_204,t2.pay_205,t2.pay_206,t2.pay_207,t2.pay_208,t2.pay_209,t2.pay_210,t2.pay_211,t2.pay_212,t2.pay_213,t2.pay_214,t2.pay_215,t2.pay_216,t2.pay_217,t2.pay_218,t2.pay_219,t2.pay_220,t2.pay_221,t2.pay_222,t2.pay_223,t2.pay_224,t2.pay_225,t2.pay_226,t2.pay_227,t2.pay_228,t2.pay_229,t2.pay_230,t2.pay_231,t2.pay_232,t2.pay_233,t2.pay_234,t2.pay_235,t2.pay_236,t2.pay_237,t2.pay_238,t2.pay_239,t2.pay_240,t2.pay_241,t2.pay_242,t2.pay_243,t2.pay_244,t2.pay_245,t2.pay_246,t2.pay_247,t2.pay_248,t2.pay_249,t2.pay_250,t2.pay_251,t2.pay_252,t2.pay_253,t2.pay_254,t2.pay_255,t2.pay_256,t2.pay_257,t2.pay_258,t2.pay_259,t2.pay_260,t2.pay_261,t2.pay_262,t2.pay_263,t2.pay_264,t2.pay_265,t2.pay_266,t2.pay_267,t2.pay_268,t2.pay_269,t2.pay_270,t2.pay_271,t2.pay_272,t2.pay_273,t2.pay_274,t2.pay_275,t2.pay_276,t2.pay_277,t2.pay_278,t2.pay_279,t2.pay_280,t2.pay_281,t2.pay_282,t2.pay_283,t2.pay_284,t2.pay_285,t2.pay_286,t2.pay_287,t2.pay_288,t2.pay_289,t2.pay_290,t2.pay_291,t2.pay_292,t2.pay_293,t2.pay_294,t2.pay_295,t2.pay_296,t2.pay_297,t2.pay_298,t2.pay_299,t2.pay_300,t2.pay_301,t2.pay_302,t2.pay_303,t2.pay_304,t2.pay_305,t2.pay_306,t2.pay_307,t2.pay_308,t2.pay_309,t2.pay_310,t2.pay_311,t2.pay_312,t2.pay_313,t2.pay_314,t2.pay_315,t2.pay_316,t2.pay_317,t2.pay_318,t2.pay_319,t2.pay_320,t2.pay_321,t2.pay_322,t2.pay_323,t2.pay_324,t2.pay_325,t2.pay_326,t2.pay_327,t2.pay_328,t2.pay_329,t2.pay_330,t2.pay_331,t2.pay_332,t2.pay_333,t2.pay_334,t2.pay_335,t2.pay_336,t2.pay_337,t2.pay_338,t2.pay_339,t2.pay_340,t2.pay_341,t2.pay_342,t2.pay_343,t2.pay_344,t2.pay_345,t2.pay_346,t2.pay_347,t2.pay_348,t2.pay_349,t2.pay_350,t2.pay_351,t2.pay_352,t2.pay_353,t2.pay_354,t2.pay_355,t2.pay_356,t2.pay_357,t2.pay_358,t2.pay_359,t2.pay_360,t2.pay_361,t2.pay_362,t2.pay_363,t2.pay_364,t2.pay_365
	,t1.eid_2_watch_uv
	,t1.pre_pay_watch_uv
	,v1.translate_type
	,v1.source_lang
	,t2.new_payamt_total
	,t2.attributed_pay_0
	,t1.attributed_pay_amt
		from tmp_primary t0
		left join analysis.dw_video_view_calc01 t1 on t0.vid=t1.vid::text and t0.d_date=t1.d_date and t0.country_code=t1.country_code and t0.ad_channel=t1.ad_channel
		left join analysis.dw_video_roi_calc0210 t2  on t0.vid=t2.vid and t0.d_date=t2.d_date and t0.country_code=t2.country_code and t0.ad_channel=t2.ad_channel
		left join public."oversea-api_osd_videos" v1 on t0.vid=v1.id::text
		left join (SELECT vid, round(sum(duration)/60.0,2) as drama_duration
         FROM "oversea-api_osd_video_episodes"
         group by vid) v on v1.id = v.vid
		left join public."oversea-api_osd_categories" v2 on v1.category_id=v2.id
		where v1.id is not null
		;





		truncate table public.dw_video_overview;
		insert into public.dw_video_overview select * from analysis.dw_video_overview_tmp01;
