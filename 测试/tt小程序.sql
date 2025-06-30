---------------------------------------------
-- File: tt小程序.sql
-- Time: 2025/6/23 18:39
-- User: xiaoj
-- Description:  
---------------------------------------------
SET odps.sql.timezone = UTC;
CREATE TABLE IF NOT EXISTS tmp_tt_applet_osd_user
(
    id BIGINT COMMENT '自增id',
	 uid BIGINT COMMENT '用户',
	 `type` BIGINT COMMENT '用户类型[0:游客,1:Apple,2:Google,3:Facebook]',
	 app_id STRING COMMENT 'appID',
	 area_id BIGINT COMMENT '区域id',
	 device_id STRING COMMENT '设备ID',
	 open_id STRING COMMENT 'open_id',
	 email STRING COMMENT '邮箱地址',
	 country_code STRING COMMENT '国家代码',
	 lang STRING COMMENT '用户语言代码',
	 nickname STRING COMMENT '用户昵称',
	 gender BIGINT COMMENT '性别 0.保密 1.男 2.女',
	 avatar_url STRING COMMENT '用户头像地址',
	 last_login_time BIGINT COMMENT '最后登录时间',
	 last_login_ip STRING COMMENT '最后登录IP',
	 last_login_mobile STRING COMMENT '最后登录设备',
	 `status` BIGINT COMMENT '状态: 1:正常 0：禁用',
	 sid STRING COMMENT '推广链接id',
	 balance BIGINT COMMENT 'K币余额',
	 is_vip BIGINT COMMENT '是否vip[1:是，0否]',
	 vip_type BIGINT COMMENT 'VIP会员类型[0:普通用户，1:免播放广告会员，2:含广告会员]',
	 vip_expires_time BIGINT COMMENT '会员到期时间戳',
	 binding_time BIGINT COMMENT '绑定时间',
	 recharge_num BIGINT COMMENT '充值次数',
	 recharge_amount BIGINT COMMENT '总充值金额（单位：分）',
	 ua_info STRING COMMENT 'ua信息',
	 bind_email STRING COMMENT '绑定的邮箱地址',
	 phone_area STRING COMMENT '手机号地区',
	 phone_num STRING COMMENT '手机号',
	 mobile_brand STRING COMMENT '手机品牌',
	 mobile_system STRING COMMENT '手机系统',
	 mobile_ram STRING COMMENT '手机RAM（单位：MB）',
	 campaign_name STRING COMMENT '推广活动名称',
	 campaign_id STRING COMMENT '推广活动ID',
	 creative STRING COMMENT '素材',
	 adgroup STRING COMMENT '渠道组',
	 ad_bind_time BIGINT COMMENT '广告平台归因时间',
	 created_at BIGINT COMMENT '创建时间',
	 updated_at BIGINT COMMENT '更新时间',
	 deleted_at BIGINT COMMENT '删除时间',
	 ext_1 BIGINT COMMENT '扩展字段1',
	 ext_2 BIGINT COMMENT '扩展字段2',
	 ext_3 STRING COMMENT '扩展字段3',
	 ext_4 STRING COMMENT '扩展字段4',
	 recharge_coin BIGINT COMMENT '总充值K币',
	 source_channel STRING COMMENT '来源渠道',
	 balance_give_expired BIGINT COMMENT '赠送金币过期数量',
	 total_balance_give BIGINT COMMENT '累计赠送K币总额',
	 balance_give BIGINT COMMENT '赠送金币余额',
	 ad_vid BIGINT COMMENT '站外投放广告剧ID, 自然流用户或无法获取剧ID时，ad_vid 为0',
	 ad_source_type BIGINT COMMENT '归因来源类型：1AF，2自归因',
	 self_attribute_id STRING COMMENT '自归因 id'
) comment 'tt小程序测试user临时表'
;

insert into table tmp_tt_applet_osd_user
VALUES (161191,63016050063127567,5,'osd59750458',1,'17504079493772321463','-000NF1ft_GQcV17S8Pxms8syBfnuYmaM439',null,'US','en_US','chuan_trump',0,
'https://p19-common-sign-va.tiktokcdn-us.com/musically-maliva-obj/1594805258216454~tplv-tiktokx-cropcenter:168:168.jpeg?dr=9638&refresh_token=f35455a0&x-expires=1750579200&x-signature=JC3YGK4gu3Hd4R48U7dWQaXpnog%3D&t=4d5b0474&ps=13740610&shp=a5d48078&shcp=8aecc5ac&idc=useast5',1750407955,
    '38.143.16.91',1,null,30,0,0,0,0,4,2795,'{}',null,null,null,null,null,null,null,null,null,null,0,1749452749,1750407955,null,null,null,null,null,0,'APP分享-63016050063106299',0,30,30,0,1,null)