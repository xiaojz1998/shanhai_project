------------------------------------------
-- file: 剧报表2.0bi.sql
-- author: xiaoj
-- time: 2025/5/16 18:40
-- description:
------------------------------------------
select t2.vid                 as "剧ID"
     , t2.name                as "剧名称"
     , t2.english_name        as "剧英文名称"
     , t2.display_date        as "上架日期"
     , t2.is_pay              as "是否收费"
     , t2.episode_coin_count  as "每章币数"
     , t2.pay_num             as "起始收费集数"
     , t2.episode_total       as "总集数"
     , t2.display_status      as "上架状态"                 -- 1上架2下架3入库4测试
     , t2.ornaments           as "剧标签"                   -- 1无2new3hot
     , t2.update_status       as "更新状态"                 -- 1更新2完结
     , t2.updated_date        as "更新日期"
     , case
           when t2.vid_source = '1' then '国内'
           when t2.vid_source = '2' then '外采'
           when t2.vid_source = '3' then '对投'
           when t2.vid_source = '4' then '自制'
           else '其他' end    as "剧目来源"
     , case
           when t2.vid_type = '1' then '翻译剧_人工'
           when t2.vid_type = '2' then '本土剧'
           when t2.vid_type = '3' then '中文剧'
           when t2.vid_type = '4' then '翻译剧_AI'
           when t2.vid_type = '5' then '配音剧'
           when t2.vid_type = '6' then '中文翻译剧'
           when t2.vid_type = '7' then '本土翻译剧'
           else '其他' end    as "剧目类别"
     , case
           when t2.complex = 1 then '男频'
           when t2.complex = 2 then '女频'
           else '其他' end    as "综合"
     , case
           when t2.category = 1 then '现代言情'
           when t2.category = 2 then '奇幻言情'
           when t2.category = 3 then '青春校园'
           when t2.category = 4 then '玄幻/仙侠'
           when t2.category = 5 then '悬疑/惊悚'
           when t2.category = 6 then '科幻'
           when t2.category = 7 then 'LGBT+'
           when t2.category = 8 then '萌宝/奶爸'
           when t2.category = 9 then '古代言情'
           when t2.category = 10 then '年代言情'
           when t2.category = 11 then '家庭伦理'
           when t2.category = 12 then '情色'
           when t2.category = 13 then '其他'
           when t2.category = 14 then '现代都市'
           when t2.category = 15 then '古风权谋'
           else '未知' end    as "作品分类"
     , t2.lang                as "剧语言"
     --
     , t2.d_date              as "数据日期"
     , t2.country_code        as "国家编码"
     , t2.ad_channel          as "渠道"
     , t2.country_name        as "国家"
     , t2.area                as "区域"
     , t2.drama_duration      as "剧时长（分钟）"
     , t2.watch_uv            as "播放人数"
     , t2.new_watch_uv        as "新用户播放人数"
     , t2.old_watch_uv        as "老用户播放"
     , t2.watch_pv            as "播放次数"
     , t2.watch_duration      as "播放时长"
     , t2.eid_watch_cnt       as "播放集数"
     , t2.ad_unlock_uv        as "广告解锁人数"
     , t2.ad_unlock_pv        as "广告解锁次数"
     , t2.pay_uv              as "充值人数"
     , t2.pay_amt             as "充值金额"
     , t2.pay_k_amt           as "K币充值金额"
     , t2.pay_vip_amt         as "订阅充值金额"
     --
     , t2.ad_campaign_cnt     as "在投campaign量"
     , t2.adcost_campaign_cnt as "消耗campaign量"
     , t2.impression          as "广告展现量"
     , t2.click               as "广告点击量"
     , t2.ad_cost             as "广告消耗金额"
     , t2.new_uv              as "剧campaign拉新用户数"
     , t2.new_pay_uv          as "新用户充值人数1"          -- 首日
     , t2.old_pay_uv          as "老用户充值人数"
     , t2.pay_0               as "剧campaign新用户充值金额" -- 首日
     -- ,t2.new_pay_amt as "新用户充值金额"
     , t2.old_pay_amt         as "老用户充值金额"
     , t2.current_diff        as "距今天数"
     -- 累计支付
     , t2.pay_0
     , t2.pay_1
     , t2.pay_2
     , t2.pay_3
     , t2.pay_4
     , t2.pay_5
     , t2.pay_6
     , t2.pay_7
     , t2.pay_8
     , t2.pay_9
     , t2.pay_10
     , t2.pay_11
     , t2.pay_12
     , t2.pay_13
     , t2.pay_14
     , t2.pay_15
     , t2.pay_16
     , t2.pay_17
     , t2.pay_18
     , t2.pay_19
     , t2.pay_20
     , t2.pay_21
     , t2.pay_22
     , t2.pay_23
     , t2.pay_24
     , t2.pay_25
     , t2.pay_26
     , t2.pay_27
     , t2.pay_28
     , t2.pay_29
     , t2.pay_30
     , t2.pay_31
     , t2.pay_32
     , t2.pay_33
     , t2.pay_34
     , t2.pay_35
     , t2.pay_36
     , t2.pay_37
     , t2.pay_38
     , t2.pay_39
     , t2.pay_40
     , t2.pay_41
     , t2.pay_42
     , t2.pay_43
     , t2.pay_44
     , t2.pay_45
     , t2.pay_46
     , t2.pay_47
     , t2.pay_48
     , t2.pay_49
     , t2.pay_50
     , t2.pay_51
     , t2.pay_52
     , t2.pay_53
     , t2.pay_54
     , t2.pay_55
     , t2.pay_56
     , t2.pay_57
     , t2.pay_58
     , t2.pay_59
     , t2."总消耗K币"
     , t2."消耗充值K币"
     , t2."消耗赠送K币"
     , t2."新用户总消耗K币"
     , t2."新用户消耗充值K币"
     , t2."新用户消耗赠送K币"
     , t2."老用户总消耗K币"
     , t2."老用户消耗充值K币"
     , t2."老用户消耗赠送K币"
     , t2.eid_2_watch_uv      as "第2集播放人数"
     , t2.pre_pay_watch_uv    as "付费卡点前播放人数"
     , t2.pay_60
     , t2.pay_61
     , t2.pay_62
     , t2.pay_63
     , t2.pay_64
     , t2.pay_65
     , t2.pay_66
     , t2.pay_67
     , t2.pay_68
     , t2.pay_69
     , t2.pay_70
     , t2.pay_71
     , t2.pay_72
     , t2.pay_73
     , t2.pay_74
     , t2.pay_75
     , t2.pay_76
     , t2.pay_77
     , t2.pay_78
     , t2.pay_79
     , t2.pay_80
     , t2.pay_81
     , t2.pay_82
     , t2.pay_83
     , t2.pay_84
     , t2.pay_85
     , t2.pay_86
     , t2.pay_87
     , t2.pay_88
     , t2.pay_89
     , t2.pay_90
     , t2.pay_91
     , t2.pay_92
     , t2.pay_93
     , t2.pay_94
     , t2.pay_95
     , t2.pay_96
     , t2.pay_97
     , t2.pay_98
     , t2.pay_99
     , t2.pay_100
     , t2.pay_101
     , t2.pay_102
     , t2.pay_103
     , t2.pay_104
     , t2.pay_105
     , t2.pay_106
     , t2.pay_107
     , t2.pay_108
     , t2.pay_109
     , t2.pay_110
     , t2.pay_111
     , t2.pay_112
     , t2.pay_113
     , t2.pay_114
     , t2.pay_115
     , t2.pay_116
     , t2.pay_117
     , t2.pay_118
     , t2.pay_119
     , t2.pay_120
     , t2.pay_121
     , t2.pay_122
     , t2.pay_123
     , t2.pay_124
     , t2.pay_125
     , t2.pay_126
     , t2.pay_127
     , t2.pay_128
     , t2.pay_129
     , t2.pay_130
     , t2.pay_131
     , t2.pay_132
     , t2.pay_133
     , t2.pay_134
     , t2.pay_135
     , t2.pay_136
     , t2.pay_137
     , t2.pay_138
     , t2.pay_139
     , t2.pay_140
     , t2.pay_141
     , t2.pay_142
     , t2.pay_143
     , t2.pay_144
     , t2.pay_145
     , t2.pay_146
     , t2.pay_147
     , t2.pay_148
     , t2.pay_149
     , t2.pay_150
     , t2.pay_151
     , t2.pay_152
     , t2.pay_153
     , t2.pay_154
     , t2.pay_155
     , t2.pay_156
     , t2.pay_157
     , t2.pay_158
     , t2.pay_159
     , t2.pay_160
     , t2.pay_161
     , t2.pay_162
     , t2.pay_163
     , t2.pay_164
     , t2.pay_165
     , t2.pay_166
     , t2.pay_167
     , t2.pay_168
     , t2.pay_169
     , t2.pay_170
     , t2.pay_171
     , t2.pay_172
     , t2.pay_173
     , t2.pay_174
     , t2.pay_175
     , t2.pay_176
     , t2.pay_177
     , t2.pay_178
     , t2.pay_179
     , t2.pay_180
     , t2.pay_181
     , t2.pay_182
     , t2.pay_183
     , t2.pay_184
     , t2.pay_185
     , t2.pay_186
     , t2.pay_187
     , t2.pay_188
     , t2.pay_189
     , t2.pay_190
     , t2.pay_191
     , t2.pay_192
     , t2.pay_193
     , t2.pay_194
     , t2.pay_195
     , t2.pay_196
     , t2.pay_197
     , t2.pay_198
     , t2.pay_199
     , t2.pay_200
     , t2.pay_201
     , t2.pay_202
     , t2.pay_203
     , t2.pay_204
     , t2.pay_205
     , t2.pay_206
     , t2.pay_207
     , t2.pay_208
     , t2.pay_209
     , t2.pay_210
     , t2.pay_211
     , t2.pay_212
     , t2.pay_213
     , t2.pay_214
     , t2.pay_215
     , t2.pay_216
     , t2.pay_217
     , t2.pay_218
     , t2.pay_219
     , t2.pay_220
     , t2.pay_221
     , t2.pay_222
     , t2.pay_223
     , t2.pay_224
     , t2.pay_225
     , t2.pay_226
     , t2.pay_227
     , t2.pay_228
     , t2.pay_229
     , t2.pay_230
     , t2.pay_231
     , t2.pay_232
     , t2.pay_233
     , t2.pay_234
     , t2.pay_235
     , t2.pay_236
     , t2.pay_237
     , t2.pay_238
     , t2.pay_239
     , t2.pay_240
     , t2.pay_241
     , t2.pay_242
     , t2.pay_243
     , t2.pay_244
     , t2.pay_245
     , t2.pay_246
     , t2.pay_247
     , t2.pay_248
     , t2.pay_249
     , t2.pay_250
     , t2.pay_251
     , t2.pay_252
     , t2.pay_253
     , t2.pay_254
     , t2.pay_255
     , t2.pay_256
     , t2.pay_257
     , t2.pay_258
     , t2.pay_259
     , t2.pay_260
     , t2.pay_261
     , t2.pay_262
     , t2.pay_263
     , t2.pay_264
     , t2.pay_265
     , t2.pay_266
     , t2.pay_267
     , t2.pay_268
     , t2.pay_269
     , t2.pay_270
     , t2.pay_271
     , t2.pay_272
     , t2.pay_273
     , t2.pay_274
     , t2.pay_275
     , t2.pay_276
     , t2.pay_277
     , t2.pay_278
     , t2.pay_279
     , t2.pay_280
     , t2.pay_281
     , t2.pay_282
     , t2.pay_283
     , t2.pay_284
     , t2.pay_285
     , t2.pay_286
     , t2.pay_287
     , t2.pay_288
     , t2.pay_289
     , t2.pay_290
     , t2.pay_291
     , t2.pay_292
     , t2.pay_293
     , t2.pay_294
     , t2.pay_295
     , t2.pay_296
     , t2.pay_297
     , t2.pay_298
     , t2.pay_299
     , t2.pay_300
     , t2.pay_301
     , t2.pay_302
     , t2.pay_303
     , t2.pay_304
     , t2.pay_305
     , t2.pay_306
     , t2.pay_307
     , t2.pay_308
     , t2.pay_309
     , t2.pay_310
     , t2.pay_311
     , t2.pay_312
     , t2.pay_313
     , t2.pay_314
     , t2.pay_315
     , t2.pay_316
     , t2.pay_317
     , t2.pay_318
     , t2.pay_319
     , t2.pay_320
     , t2.pay_321
     , t2.pay_322
     , t2.pay_323
     , t2.pay_324
     , t2.pay_325
     , t2.pay_326
     , t2.pay_327
     , t2.pay_328
     , t2.pay_329
     , t2.pay_330
     , t2.pay_331
     , t2.pay_332
     , t2.pay_333
     , t2.pay_334
     , t2.pay_335
     , t2.pay_336
     , t2.pay_337
     , t2.pay_338
     , t2.pay_339
     , t2.pay_340
     , t2.pay_341
     , t2.pay_342
     , t2.pay_343
     , t2.pay_344
     , t2.pay_345
     , t2.pay_346
     , t2.pay_347
     , t2.pay_348
     , t2.pay_349
     , t2.pay_350
     , t2.pay_351
     , t2.pay_352
     , t2.pay_353
     , t2.pay_354
     , t2.pay_355
     , t2.pay_356
     , t2.pay_357
     , t2.pay_358
     , t2.pay_359
     , t2.pay_360
     , t2.pay_361
     , t2.pay_362
     , t2.pay_363
     , t2.pay_364
     , t2.pay_365
     , case
           when t2.source_lang = 13 then '日语'
           when t2.source_lang = 14 then '韩语'
           when t2.source_lang = 20 then '英语'
           when t2.source_lang = 22 then '西班牙语'
           when t2.source_lang = 31 then '印尼语'
           when t2.source_lang = 34 then '中文简体'
           when t2.source_lang = 35 then '法语'
           when t2.source_lang = 36 then '葡萄牙语'
           when t2.source_lang = 37 then '德语'
           when t2.source_lang = 38 then '泰语'
           when t2.source_lang = 39 then '繁体中文'
           when t2.source_lang = 40 then '阿拉伯语'
           when t2.source_lang = 41 then '越南语'
           when t2.source_lang = 42 then '土耳其语'

           else '未知' end    as "来源语言"
     , case
           when t2.translate_type = 1 then '人工翻译'
           when t2.translate_type = 2 then 'AI翻译'
           else '其他' end    as "翻译类型"
,coalesce(t2.new_payamt_total,0) as "累计金额"
,coalesce(t2.attributed_pay_0,0) as "剧campaign新用户充值金额(归因剧)"
from public.dw_video_overview t2