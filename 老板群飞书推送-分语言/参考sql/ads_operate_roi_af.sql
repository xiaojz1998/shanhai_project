------------------------------------------
-- file: ads_operate_roi_af.sql
-- author: xiaoj
-- time: 2025/5/8 15:17
-- description:
------------------------------------------
set timezone ='UTC-0';
insert into public.ads_operate_roi_af
with new_reg_users as(
	select
	d_date
	,platform,country_code
	,campaign_id
	,'ua' as campaign_type
	,media_channel
	,count(distinct af_id) as install_uv
	from(
	select * from( select app_name,app_id
	,campaign_name,campaign_id, ad_channel,media_source
	,af_id ,lower(platform) as platform ,upper(country_code) as country_code
	,install_date,created_date,event_time,install_time,(install_time::timestamp)::date as d_date
	,row_number() over(partition by af_id order by install_time asc) as rn
	,case when media_source in('metaweb_int','Facebook Ads','fb','restricted') then 'Meta'
		when media_source in('googleadwords_int','google') then 'Google'
		when media_source in('tiktokweb','tiktokglobal_int') then 'TikTok'
		when media_source in('mintegral_int') then 'Mintegral'
		when media_source in('Apple Search Ads') then 'Apple'
		when media_source in('organic') then 'Organic'
		else 'Others' end as media_channel
	-- from public.app_install_event_log
	from public.dwd_app_install_event_log
	where event_name='install'
	and install_date <= '${bizdate}'
	)a where rn=1
	)a
	group by
	d_date
	,platform,country_code
	,campaign_id
	,media_channel
)
,tmp_af_purchase as(
	select lower(platform) as platform ,upper(country_code) as country_code ,app_name ,app_id ,af_id ,event_revenue_usd::int4
	,campaign_id ,campaign_name ,ad_channel ,media_source ,conversion_type ,campaign_type
	,created_date ,install_date ,event_time,install_time
	,to_date(created_date::text,'yyyymmdd') p_date,(install_time::timestamp)::date as d_date
	,case when media_source in('metaweb_int','Facebook Ads','fb','restricted') then 'Meta'
		when media_source in('googleadwords_int','google') then 'Google'
		when media_source in('tiktokweb','tiktokglobal_int') then 'TikTok'
		when media_source in('mintegral_int') then 'Mintegral'
		when media_source in('Apple Search Ads') then 'Apple'
		when media_source in('organic') then 'Organic'
		else 'Others' end as media_channel
	-- from public.app_purchase_event_log pu
	-- where event_name='af_purchase'
	from public.dwd_app_purchase_event_log
	where event_name in('af_purchase','Purchase')
	and created_date <= '${bizdate}'
)
,tmp_total_pay as(
	select
	t1.p_date as d_date
	,t1.platform
	,t1.country_code
	,t1.campaign_id
	,t1.campaign_type
	,t1.media_channel
	,round(0.01*sum(t1.event_revenue_usd),2) as pay_amt
	,count(distinct af_id) as pay_uv
	from tmp_af_purchase t1
	group by
	t1.p_date
	,t1.platform
	,t1.country_code
	,t1.campaign_id
	,t1.campaign_type
	,t1.media_channel
)
,tmp_total_cost as(
	select
	cd.created_date
	,to_date(cd.created_date::text,'yyyymmdd') as d_date
	,'unknown' as platform
	,upper(cd.area) as country_code
	,campaign_id
	,'ua' as campaign_type
	,case when ad_channel='fb' then 'Meta'
		when ad_channel='gg' then 'Google'
		when ad_channel ='tt' then 'TikTok'
		when ad_channel='mintegral_int' then 'Mintegral'
		when ad_channel='apple' then 'Apple'
		else 'Others' end as media_channel
	,sum(0.0001*cost_amount) as ad_cost
	from public.ad_cost_data_log cd
	where 1=1
	and cd.account_id not in('3851320725139192','1248567319618926')
	and cd.created_date <= '${bizdate}'
	group by
	cd.created_date
	,upper(cd.area)
	,campaign_id
	,case when ad_channel='fb' then 'Meta'
		when ad_channel='gg' then 'Google'
		when ad_channel ='tt' then 'TikTok'
		when ad_channel='mintegral_int' then 'Mintegral'
		when ad_channel='apple' then 'Apple'
		else 'Others' end
)
,tmp_base as(
	select t1.d_date,t1.platform,t1.country_code,t1.campaign_id ,t1.media_channel,t1.campaign_type
	,coalesce(cn.campaign_name,'unknown') as campaign_name
	,coalesce(cn.vid,'unknown') as vid
	,coalesce(cn.vid_name,'unknown') as vid_name
	,coalesce(cn.lang,'unknown') as lang
	,coalesce(cn.lang_name,'unknown') as lang_name
	,coalesce(cc.area,'unknown') as area
	,coalesce(cc.country_name,'unknown') as country_name
	,concat('T-',coalesce(cc.label::text,'unknown')) as country_lable
	from(
	select  t1.d_date,t1.platform,t1.country_code,t1.campaign_id ,t1.media_channel,t1.campaign_type
	from(
		select distinct d_date,platform,country_code,campaign_id ,media_channel,campaign_type from new_reg_users union all
		select distinct d_date,platform,country_code,campaign_id ,media_channel,campaign_type from tmp_total_pay union all
		select distinct d_date,platform,country_code,campaign_id ,media_channel,campaign_type from tmp_total_cost
	)t1
	group by  t1.d_date,t1.platform,t1.country_code,t1.campaign_id ,t1.media_channel,t1.campaign_type
	)t1
	left join v_dim_ad_campaign_info cn on t1.campaign_id=cn.campaign_id
	left join v_dim_country_area cc on t1.country_code=cc.country_code
)
select * from(
select md5(concat(t0.d_date,t0.platform,t0.country_code,t0.campaign_id ,t0.media_channel,t0.campaign_type)) as id
,t0.d_date,t0.platform,t0.country_code,t0.campaign_id ,t0.media_channel,t0.campaign_type
,t0.campaign_name
,t0.vid
,t0.vid_name
,t0.lang
,t0.lang_name
,t0.area
,t0.country_name
,t0.country_lable
,coalesce(t1.install_uv,0) as install_uv
,coalesce(t2.pay_amt,0.0) as pay_amt
,coalesce(t2.pay_uv,0) as pay_uv
,coalesce(t3.ad_cost,0.0) as ad_cost
,t5.date_diff
,t5.af_d0
,t5.af_d1
,t5.af_d2
,t5.af_d3
,t5.af_d4
,t5.af_d5
,t5.af_d6
,t5.af_d7
,t5.af_d8
,t5.af_d9
,t5.af_d10
,t5.af_d11
,t5.af_d12
,t5.af_d13
,t5.af_d14
,t5.af_d15
,t5.af_d16
,t5.af_d17
,t5.af_d18
,t5.af_d19
,t5.af_d20
,t5.af_d21
,t5.af_d22
,t5.af_d23
,t5.af_d24
,t5.af_d25
,t5.af_d26
,t5.af_d27
,t5.af_d28
,t5.af_d29
,t5.af_d30
,t5.af_d31
,t5.af_d32
,t5.af_d33
,t5.af_d34
,t5.af_d35
,t5.af_d36
,t5.af_d37
,t5.af_d38
,t5.af_d39
,t5.af_d40
,t5.af_d41
,t5.af_d42
,t5.af_d43
,t5.af_d44
,t5.af_d45
,t5.af_d46
,t5.af_d47
,t5.af_d48
,t5.af_d49
,t5.af_d50
,t5.af_d51
,t5.af_d52
,t5.af_d53
,t5.af_d54
,t5.af_d55
,t5.af_d56
,t5.af_d57
,t5.af_d58
,t5.af_d59
,t5.af_d60
,t5.af_d61
,t5.af_d62
,t5.af_d63
,t5.af_d64
,t5.af_d65
,t5.af_d66
,t5.af_d67
,t5.af_d68
,t5.af_d69
,t5.af_d70
,t5.af_d71
,t5.af_d72
,t5.af_d73
,t5.af_d74
,t5.af_d75
,t5.af_d76
,t5.af_d77
,t5.af_d78
,t5.af_d79
,t5.af_d80
,t5.af_d81
,t5.af_d82
,t5.af_d83
,t5.af_d84
,t5.af_d85
,t5.af_d86
,t5.af_d87
,t5.af_d88
,t5.af_d89
,t5.af_d90
,t5.af_d91
,t5.af_d92
,t5.af_d93
,t5.af_d94
,t5.af_d95
,t5.af_d96
,t5.af_d97
,t5.af_d98
,t5.af_d99
,t5.af_d100
,t5.af_d101
,t5.af_d102
,t5.af_d103
,t5.af_d104
,t5.af_d105
,t5.af_d106
,t5.af_d107
,t5.af_d108
,t5.af_d109
,t5.af_d110
,t5.af_d111
,t5.af_d112
,t5.af_d113
,t5.af_d114
,t5.af_d115
,t5.af_d116
,t5.af_d117
,t5.af_d118
,t5.af_d119
,t5.af_d120
,t5.af_d121
,t5.af_d122
,t5.af_d123
,t5.af_d124
,t5.af_d125
,t5.af_d126
,t5.af_d127
,t5.af_d128
,t5.af_d129
,t5.af_d130
,t5.af_d131
,t5.af_d132
,t5.af_d133
,t5.af_d134
,t5.af_d135
,t5.af_d136
,t5.af_d137
,t5.af_d138
,t5.af_d139
,t5.af_d140
,t5.af_d141
,t5.af_d142
,t5.af_d143
,t5.af_d144
,t5.af_d145
,t5.af_d146
,t5.af_d147
,t5.af_d148
,t5.af_d149
,t5.af_d150
,t5.af_d151
,t5.af_d152
,t5.af_d153
,t5.af_d154
,t5.af_d155
,t5.af_d156
,t5.af_d157
,t5.af_d158
,t5.af_d159
,t5.af_d160
,t5.af_d161
,t5.af_d162
,t5.af_d163
,t5.af_d164
,t5.af_d165
,t5.af_d166
,t5.af_d167
,t5.af_d168
,t5.af_d169
,t5.af_d170
,t5.af_d171
,t5.af_d172
,t5.af_d173
,t5.af_d174
,t5.af_d175
,t5.af_d176
,t5.af_d177
,t5.af_d178
,t5.af_d179
,t5.af_d180
,coalesce(t5.new_pay_amt,0) as new_pay_amt
,t5.af_d185
,t5.af_d195
,t5.af_d205
,t5.af_d215
,t5.af_d225
,t5.af_d235
,t5.af_d245
,t5.af_d255
,t5.af_d265
,t5.af_d275
,t5.af_d285
,t5.af_d295
,t5.af_d305
,t5.af_d315
,t5.af_d325
,t5.af_d335
,t5.af_d345
,t5.af_d355
,t5.af_d365
from tmp_base t0
left join new_reg_users t1 on t0.d_date=t1.d_date and t0.platform=t1.platform and t0.country_code=t1.country_code and t0.campaign_id=t1.campaign_id and t0.media_channel=t1.media_channel and t0.campaign_type=t1.campaign_type
left join tmp_total_pay t2 on t0.d_date=t2.d_date and t0.platform=t2.platform and t0.country_code=t2.country_code and t0.campaign_id=t2.campaign_id and t0.media_channel=t2.media_channel and t0.campaign_type=t2.campaign_type
left join tmp_total_cost t3 on t0.d_date=t3.d_date and t0.platform=t3.platform and t0.country_code=t3.country_code and t0.campaign_id=t3.campaign_id and t0.media_channel=t3.media_channel and t0.campaign_type=t3.campaign_type
left join(
select
t1.d_date
,t1.platform
,t1.country_code
,t1.campaign_id
,t1.campaign_type
,t1.media_channel
,(current_date-t1.d_date) as date_diff
,round(0.01*sum(if(t1.p_date>=t1.d_date ,t1.event_revenue_usd,0)),2) as new_pay_amt
,round(0.01*sum(if(t1.p_date=t1.d_date,t1.event_revenue_usd,0)),2) as af_d0
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '1 d') and (current_date-t1.d_date)>=1 ,t1.event_revenue_usd,0)),2) as af_d1
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '2 d') and (current_date-t1.d_date)>=2 ,t1.event_revenue_usd,0)),2) as af_d2
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '3 d') and (current_date-t1.d_date)>=3 ,t1.event_revenue_usd,0)),2) as af_d3
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '4 d') and (current_date-t1.d_date)>=4 ,t1.event_revenue_usd,0)),2) as af_d4
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '5 d') and (current_date-t1.d_date)>=5 ,t1.event_revenue_usd,0)),2) as af_d5
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '6 d') and (current_date-t1.d_date)>=6 ,t1.event_revenue_usd,0)),2) as af_d6
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '7 d') and (current_date-t1.d_date)>=7 ,t1.event_revenue_usd,0)),2) as af_d7
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '8 d') and (current_date-t1.d_date)>=8 ,t1.event_revenue_usd,0)),2) as af_d8
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '9 d') and (current_date-t1.d_date)>=9 ,t1.event_revenue_usd,0)),2) as af_d9
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '10 d') and (current_date-t1.d_date)>=10 ,t1.event_revenue_usd,0)),2) as af_d10
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '11 d') and (current_date-t1.d_date)>=11 ,t1.event_revenue_usd,0)),2) as af_d11
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '12 d') and (current_date-t1.d_date)>=12 ,t1.event_revenue_usd,0)),2) as af_d12
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '13 d') and (current_date-t1.d_date)>=13 ,t1.event_revenue_usd,0)),2) as af_d13
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '14 d') and (current_date-t1.d_date)>=14 ,t1.event_revenue_usd,0)),2) as af_d14
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '15 d') and (current_date-t1.d_date)>=15 ,t1.event_revenue_usd,0)),2) as af_d15
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '16 d') and (current_date-t1.d_date)>=16 ,t1.event_revenue_usd,0)),2) as af_d16
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '17 d') and (current_date-t1.d_date)>=17 ,t1.event_revenue_usd,0)),2) as af_d17
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '18 d') and (current_date-t1.d_date)>=18 ,t1.event_revenue_usd,0)),2) as af_d18
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '19 d') and (current_date-t1.d_date)>=19 ,t1.event_revenue_usd,0)),2) as af_d19
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '20 d') and (current_date-t1.d_date)>=20 ,t1.event_revenue_usd,0)),2) as af_d20
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '21 d') and (current_date-t1.d_date)>=21 ,t1.event_revenue_usd,0)),2) as af_d21
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '22 d') and (current_date-t1.d_date)>=22 ,t1.event_revenue_usd,0)),2) as af_d22
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '23 d') and (current_date-t1.d_date)>=23 ,t1.event_revenue_usd,0)),2) as af_d23
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '24 d') and (current_date-t1.d_date)>=24 ,t1.event_revenue_usd,0)),2) as af_d24
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '25 d') and (current_date-t1.d_date)>=25 ,t1.event_revenue_usd,0)),2) as af_d25
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '26 d') and (current_date-t1.d_date)>=26 ,t1.event_revenue_usd,0)),2) as af_d26
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '27 d') and (current_date-t1.d_date)>=27 ,t1.event_revenue_usd,0)),2) as af_d27
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '28 d') and (current_date-t1.d_date)>=28 ,t1.event_revenue_usd,0)),2) as af_d28
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '29 d') and (current_date-t1.d_date)>=29 ,t1.event_revenue_usd,0)),2) as af_d29
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '30 d') and (current_date-t1.d_date)>=30 ,t1.event_revenue_usd,0)),2) as af_d30
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '31 d') and (current_date-t1.d_date)>=31 ,t1.event_revenue_usd,0)),2) as af_d31
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '32 d') and (current_date-t1.d_date)>=32 ,t1.event_revenue_usd,0)),2) as af_d32
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '33 d') and (current_date-t1.d_date)>=33 ,t1.event_revenue_usd,0)),2) as af_d33
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '34 d') and (current_date-t1.d_date)>=34 ,t1.event_revenue_usd,0)),2) as af_d34
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '35 d') and (current_date-t1.d_date)>=35 ,t1.event_revenue_usd,0)),2) as af_d35
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '36 d') and (current_date-t1.d_date)>=36 ,t1.event_revenue_usd,0)),2) as af_d36
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '37 d') and (current_date-t1.d_date)>=37 ,t1.event_revenue_usd,0)),2) as af_d37
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '38 d') and (current_date-t1.d_date)>=38 ,t1.event_revenue_usd,0)),2) as af_d38
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '39 d') and (current_date-t1.d_date)>=39 ,t1.event_revenue_usd,0)),2) as af_d39
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '40 d') and (current_date-t1.d_date)>=40 ,t1.event_revenue_usd,0)),2) as af_d40
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '41 d') and (current_date-t1.d_date)>=41 ,t1.event_revenue_usd,0)),2) as af_d41
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '42 d') and (current_date-t1.d_date)>=42 ,t1.event_revenue_usd,0)),2) as af_d42
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '43 d') and (current_date-t1.d_date)>=43 ,t1.event_revenue_usd,0)),2) as af_d43
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '44 d') and (current_date-t1.d_date)>=44 ,t1.event_revenue_usd,0)),2) as af_d44
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '45 d') and (current_date-t1.d_date)>=45 ,t1.event_revenue_usd,0)),2) as af_d45
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '46 d') and (current_date-t1.d_date)>=46 ,t1.event_revenue_usd,0)),2) as af_d46
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '47 d') and (current_date-t1.d_date)>=47 ,t1.event_revenue_usd,0)),2) as af_d47
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '48 d') and (current_date-t1.d_date)>=48 ,t1.event_revenue_usd,0)),2) as af_d48
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '49 d') and (current_date-t1.d_date)>=49 ,t1.event_revenue_usd,0)),2) as af_d49
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '50 d') and (current_date-t1.d_date)>=50 ,t1.event_revenue_usd,0)),2) as af_d50
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '51 d') and (current_date-t1.d_date)>=51 ,t1.event_revenue_usd,0)),2) as af_d51
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '52 d') and (current_date-t1.d_date)>=52 ,t1.event_revenue_usd,0)),2) as af_d52
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '53 d') and (current_date-t1.d_date)>=53 ,t1.event_revenue_usd,0)),2) as af_d53
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '54 d') and (current_date-t1.d_date)>=54 ,t1.event_revenue_usd,0)),2) as af_d54
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '55 d') and (current_date-t1.d_date)>=55 ,t1.event_revenue_usd,0)),2) as af_d55
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '56 d') and (current_date-t1.d_date)>=56 ,t1.event_revenue_usd,0)),2) as af_d56
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '57 d') and (current_date-t1.d_date)>=57 ,t1.event_revenue_usd,0)),2) as af_d57
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '58 d') and (current_date-t1.d_date)>=58 ,t1.event_revenue_usd,0)),2) as af_d58
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '59 d') and (current_date-t1.d_date)>=59 ,t1.event_revenue_usd,0)),2) as af_d59
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '60 d') and (current_date-t1.d_date)>=60 ,t1.event_revenue_usd,0)),2) as af_d60
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '61 d') and (current_date-t1.d_date)>=61 ,t1.event_revenue_usd,0)),2) as af_d61
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '62 d') and (current_date-t1.d_date)>=62 ,t1.event_revenue_usd,0)),2) as af_d62
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '63 d') and (current_date-t1.d_date)>=63 ,t1.event_revenue_usd,0)),2) as af_d63
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '64 d') and (current_date-t1.d_date)>=64 ,t1.event_revenue_usd,0)),2) as af_d64
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '65 d') and (current_date-t1.d_date)>=65 ,t1.event_revenue_usd,0)),2) as af_d65
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '66 d') and (current_date-t1.d_date)>=66 ,t1.event_revenue_usd,0)),2) as af_d66
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '67 d') and (current_date-t1.d_date)>=67 ,t1.event_revenue_usd,0)),2) as af_d67
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '68 d') and (current_date-t1.d_date)>=68 ,t1.event_revenue_usd,0)),2) as af_d68
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '69 d') and (current_date-t1.d_date)>=69 ,t1.event_revenue_usd,0)),2) as af_d69
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '70 d') and (current_date-t1.d_date)>=70 ,t1.event_revenue_usd,0)),2) as af_d70
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '71 d') and (current_date-t1.d_date)>=71 ,t1.event_revenue_usd,0)),2) as af_d71
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '72 d') and (current_date-t1.d_date)>=72 ,t1.event_revenue_usd,0)),2) as af_d72
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '73 d') and (current_date-t1.d_date)>=73 ,t1.event_revenue_usd,0)),2) as af_d73
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '74 d') and (current_date-t1.d_date)>=74 ,t1.event_revenue_usd,0)),2) as af_d74
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '75 d') and (current_date-t1.d_date)>=75 ,t1.event_revenue_usd,0)),2) as af_d75
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '76 d') and (current_date-t1.d_date)>=76 ,t1.event_revenue_usd,0)),2) as af_d76
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '77 d') and (current_date-t1.d_date)>=77 ,t1.event_revenue_usd,0)),2) as af_d77
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '78 d') and (current_date-t1.d_date)>=78 ,t1.event_revenue_usd,0)),2) as af_d78
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '79 d') and (current_date-t1.d_date)>=79 ,t1.event_revenue_usd,0)),2) as af_d79
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '80 d') and (current_date-t1.d_date)>=80 ,t1.event_revenue_usd,0)),2) as af_d80
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '81 d') and (current_date-t1.d_date)>=81 ,t1.event_revenue_usd,0)),2) as af_d81
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '82 d') and (current_date-t1.d_date)>=82 ,t1.event_revenue_usd,0)),2) as af_d82
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '83 d') and (current_date-t1.d_date)>=83 ,t1.event_revenue_usd,0)),2) as af_d83
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '84 d') and (current_date-t1.d_date)>=84 ,t1.event_revenue_usd,0)),2) as af_d84
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '85 d') and (current_date-t1.d_date)>=85 ,t1.event_revenue_usd,0)),2) as af_d85
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '86 d') and (current_date-t1.d_date)>=86 ,t1.event_revenue_usd,0)),2) as af_d86
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '87 d') and (current_date-t1.d_date)>=87 ,t1.event_revenue_usd,0)),2) as af_d87
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '88 d') and (current_date-t1.d_date)>=88 ,t1.event_revenue_usd,0)),2) as af_d88
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '89 d') and (current_date-t1.d_date)>=89 ,t1.event_revenue_usd,0)),2) as af_d89
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '90 d') and (current_date-t1.d_date)>=90 ,t1.event_revenue_usd,0)),2) as af_d90
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '91 d') and (current_date-t1.d_date)>=91 ,t1.event_revenue_usd,0)),2) as af_d91
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '92 d') and (current_date-t1.d_date)>=92 ,t1.event_revenue_usd,0)),2) as af_d92
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '93 d') and (current_date-t1.d_date)>=93 ,t1.event_revenue_usd,0)),2) as af_d93
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '94 d') and (current_date-t1.d_date)>=94 ,t1.event_revenue_usd,0)),2) as af_d94
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '95 d') and (current_date-t1.d_date)>=95 ,t1.event_revenue_usd,0)),2) as af_d95
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '96 d') and (current_date-t1.d_date)>=96 ,t1.event_revenue_usd,0)),2) as af_d96
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '97 d') and (current_date-t1.d_date)>=97 ,t1.event_revenue_usd,0)),2) as af_d97
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '98 d') and (current_date-t1.d_date)>=98 ,t1.event_revenue_usd,0)),2) as af_d98
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '99 d') and (current_date-t1.d_date)>=99 ,t1.event_revenue_usd,0)),2) as af_d99
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '100 d') and (current_date-t1.d_date)>=100 ,t1.event_revenue_usd,0)),2) as af_d100
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '101 d') and (current_date-t1.d_date)>=101 ,t1.event_revenue_usd,0)),2) as af_d101
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '102 d') and (current_date-t1.d_date)>=102 ,t1.event_revenue_usd,0)),2) as af_d102
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '103 d') and (current_date-t1.d_date)>=103 ,t1.event_revenue_usd,0)),2) as af_d103
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '104 d') and (current_date-t1.d_date)>=104 ,t1.event_revenue_usd,0)),2) as af_d104
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '105 d') and (current_date-t1.d_date)>=105 ,t1.event_revenue_usd,0)),2) as af_d105
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '106 d') and (current_date-t1.d_date)>=106 ,t1.event_revenue_usd,0)),2) as af_d106
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '107 d') and (current_date-t1.d_date)>=107 ,t1.event_revenue_usd,0)),2) as af_d107
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '108 d') and (current_date-t1.d_date)>=108 ,t1.event_revenue_usd,0)),2) as af_d108
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '109 d') and (current_date-t1.d_date)>=109 ,t1.event_revenue_usd,0)),2) as af_d109
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '110 d') and (current_date-t1.d_date)>=110 ,t1.event_revenue_usd,0)),2) as af_d110
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '111 d') and (current_date-t1.d_date)>=111 ,t1.event_revenue_usd,0)),2) as af_d111
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '112 d') and (current_date-t1.d_date)>=112 ,t1.event_revenue_usd,0)),2) as af_d112
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '113 d') and (current_date-t1.d_date)>=113 ,t1.event_revenue_usd,0)),2) as af_d113
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '114 d') and (current_date-t1.d_date)>=114 ,t1.event_revenue_usd,0)),2) as af_d114
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '115 d') and (current_date-t1.d_date)>=115 ,t1.event_revenue_usd,0)),2) as af_d115
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '116 d') and (current_date-t1.d_date)>=116 ,t1.event_revenue_usd,0)),2) as af_d116
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '117 d') and (current_date-t1.d_date)>=117 ,t1.event_revenue_usd,0)),2) as af_d117
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '118 d') and (current_date-t1.d_date)>=118 ,t1.event_revenue_usd,0)),2) as af_d118
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '119 d') and (current_date-t1.d_date)>=119 ,t1.event_revenue_usd,0)),2) as af_d119
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '120 d') and (current_date-t1.d_date)>=120 ,t1.event_revenue_usd,0)),2) as af_d120
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '121 d') and (current_date-t1.d_date)>=121 ,t1.event_revenue_usd,0)),2) as af_d121
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '122 d') and (current_date-t1.d_date)>=122 ,t1.event_revenue_usd,0)),2) as af_d122
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '123 d') and (current_date-t1.d_date)>=123 ,t1.event_revenue_usd,0)),2) as af_d123
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '124 d') and (current_date-t1.d_date)>=124 ,t1.event_revenue_usd,0)),2) as af_d124
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '125 d') and (current_date-t1.d_date)>=125 ,t1.event_revenue_usd,0)),2) as af_d125
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '126 d') and (current_date-t1.d_date)>=126 ,t1.event_revenue_usd,0)),2) as af_d126
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '127 d') and (current_date-t1.d_date)>=127 ,t1.event_revenue_usd,0)),2) as af_d127
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '128 d') and (current_date-t1.d_date)>=128 ,t1.event_revenue_usd,0)),2) as af_d128
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '129 d') and (current_date-t1.d_date)>=129 ,t1.event_revenue_usd,0)),2) as af_d129
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '130 d') and (current_date-t1.d_date)>=130 ,t1.event_revenue_usd,0)),2) as af_d130
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '131 d') and (current_date-t1.d_date)>=131 ,t1.event_revenue_usd,0)),2) as af_d131
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '132 d') and (current_date-t1.d_date)>=132 ,t1.event_revenue_usd,0)),2) as af_d132
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '133 d') and (current_date-t1.d_date)>=133 ,t1.event_revenue_usd,0)),2) as af_d133
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '134 d') and (current_date-t1.d_date)>=134 ,t1.event_revenue_usd,0)),2) as af_d134
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '135 d') and (current_date-t1.d_date)>=135 ,t1.event_revenue_usd,0)),2) as af_d135
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '136 d') and (current_date-t1.d_date)>=136 ,t1.event_revenue_usd,0)),2) as af_d136
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '137 d') and (current_date-t1.d_date)>=137 ,t1.event_revenue_usd,0)),2) as af_d137
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '138 d') and (current_date-t1.d_date)>=138 ,t1.event_revenue_usd,0)),2) as af_d138
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '139 d') and (current_date-t1.d_date)>=139 ,t1.event_revenue_usd,0)),2) as af_d139
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '140 d') and (current_date-t1.d_date)>=140 ,t1.event_revenue_usd,0)),2) as af_d140
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '141 d') and (current_date-t1.d_date)>=141 ,t1.event_revenue_usd,0)),2) as af_d141
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '142 d') and (current_date-t1.d_date)>=142 ,t1.event_revenue_usd,0)),2) as af_d142
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '143 d') and (current_date-t1.d_date)>=143 ,t1.event_revenue_usd,0)),2) as af_d143
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '144 d') and (current_date-t1.d_date)>=144 ,t1.event_revenue_usd,0)),2) as af_d144
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '145 d') and (current_date-t1.d_date)>=145 ,t1.event_revenue_usd,0)),2) as af_d145
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '146 d') and (current_date-t1.d_date)>=146 ,t1.event_revenue_usd,0)),2) as af_d146
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '147 d') and (current_date-t1.d_date)>=147 ,t1.event_revenue_usd,0)),2) as af_d147
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '148 d') and (current_date-t1.d_date)>=148 ,t1.event_revenue_usd,0)),2) as af_d148
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '149 d') and (current_date-t1.d_date)>=149 ,t1.event_revenue_usd,0)),2) as af_d149
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '150 d') and (current_date-t1.d_date)>=150 ,t1.event_revenue_usd,0)),2) as af_d150
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '151 d') and (current_date-t1.d_date)>=151 ,t1.event_revenue_usd,0)),2) as af_d151
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '152 d') and (current_date-t1.d_date)>=152 ,t1.event_revenue_usd,0)),2) as af_d152
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '153 d') and (current_date-t1.d_date)>=153 ,t1.event_revenue_usd,0)),2) as af_d153
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '154 d') and (current_date-t1.d_date)>=154 ,t1.event_revenue_usd,0)),2) as af_d154
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '155 d') and (current_date-t1.d_date)>=155 ,t1.event_revenue_usd,0)),2) as af_d155
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '156 d') and (current_date-t1.d_date)>=156 ,t1.event_revenue_usd,0)),2) as af_d156
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '157 d') and (current_date-t1.d_date)>=157 ,t1.event_revenue_usd,0)),2) as af_d157
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '158 d') and (current_date-t1.d_date)>=158 ,t1.event_revenue_usd,0)),2) as af_d158
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '159 d') and (current_date-t1.d_date)>=159 ,t1.event_revenue_usd,0)),2) as af_d159
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '160 d') and (current_date-t1.d_date)>=160 ,t1.event_revenue_usd,0)),2) as af_d160
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '161 d') and (current_date-t1.d_date)>=161 ,t1.event_revenue_usd,0)),2) as af_d161
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '162 d') and (current_date-t1.d_date)>=162 ,t1.event_revenue_usd,0)),2) as af_d162
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '163 d') and (current_date-t1.d_date)>=163 ,t1.event_revenue_usd,0)),2) as af_d163
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '164 d') and (current_date-t1.d_date)>=164 ,t1.event_revenue_usd,0)),2) as af_d164
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '165 d') and (current_date-t1.d_date)>=165 ,t1.event_revenue_usd,0)),2) as af_d165
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '166 d') and (current_date-t1.d_date)>=166 ,t1.event_revenue_usd,0)),2) as af_d166
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '167 d') and (current_date-t1.d_date)>=167 ,t1.event_revenue_usd,0)),2) as af_d167
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '168 d') and (current_date-t1.d_date)>=168 ,t1.event_revenue_usd,0)),2) as af_d168
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '169 d') and (current_date-t1.d_date)>=169 ,t1.event_revenue_usd,0)),2) as af_d169
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '170 d') and (current_date-t1.d_date)>=170 ,t1.event_revenue_usd,0)),2) as af_d170
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '171 d') and (current_date-t1.d_date)>=171 ,t1.event_revenue_usd,0)),2) as af_d171
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '172 d') and (current_date-t1.d_date)>=172 ,t1.event_revenue_usd,0)),2) as af_d172
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '173 d') and (current_date-t1.d_date)>=173 ,t1.event_revenue_usd,0)),2) as af_d173
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '174 d') and (current_date-t1.d_date)>=174 ,t1.event_revenue_usd,0)),2) as af_d174
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '175 d') and (current_date-t1.d_date)>=175 ,t1.event_revenue_usd,0)),2) as af_d175
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '176 d') and (current_date-t1.d_date)>=176 ,t1.event_revenue_usd,0)),2) as af_d176
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '177 d') and (current_date-t1.d_date)>=177 ,t1.event_revenue_usd,0)),2) as af_d177
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '178 d') and (current_date-t1.d_date)>=178 ,t1.event_revenue_usd,0)),2) as af_d178
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '179 d') and (current_date-t1.d_date)>=179 ,t1.event_revenue_usd,0)),2) as af_d179
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '180 d') and (current_date-t1.d_date)>=180 ,t1.event_revenue_usd,0)),2) as af_d180
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '185 d') and (current_date-t1.d_date)>=185 ,t1.event_revenue_usd,0)),2) as af_d185
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '195 d') and (current_date-t1.d_date)>=195 ,t1.event_revenue_usd,0)),2) as af_d195
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '205 d') and (current_date-t1.d_date)>=205 ,t1.event_revenue_usd,0)),2) as af_d205
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '215 d') and (current_date-t1.d_date)>=215 ,t1.event_revenue_usd,0)),2) as af_d215
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '225 d') and (current_date-t1.d_date)>=225 ,t1.event_revenue_usd,0)),2) as af_d225
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '235 d') and (current_date-t1.d_date)>=235 ,t1.event_revenue_usd,0)),2) as af_d235
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '245 d') and (current_date-t1.d_date)>=245 ,t1.event_revenue_usd,0)),2) as af_d245
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '255 d') and (current_date-t1.d_date)>=255 ,t1.event_revenue_usd,0)),2) as af_d255
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '265 d') and (current_date-t1.d_date)>=265 ,t1.event_revenue_usd,0)),2) as af_d265
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '275 d') and (current_date-t1.d_date)>=275 ,t1.event_revenue_usd,0)),2) as af_d275
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '285 d') and (current_date-t1.d_date)>=285 ,t1.event_revenue_usd,0)),2) as af_d285
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '295 d') and (current_date-t1.d_date)>=295 ,t1.event_revenue_usd,0)),2) as af_d295
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '305 d') and (current_date-t1.d_date)>=305 ,t1.event_revenue_usd,0)),2) as af_d305
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '315 d') and (current_date-t1.d_date)>=315 ,t1.event_revenue_usd,0)),2) as af_d315
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '325 d') and (current_date-t1.d_date)>=325 ,t1.event_revenue_usd,0)),2) as af_d325
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '335 d') and (current_date-t1.d_date)>=335 ,t1.event_revenue_usd,0)),2) as af_d335
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '345 d') and (current_date-t1.d_date)>=345 ,t1.event_revenue_usd,0)),2) as af_d345
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '355 d') and (current_date-t1.d_date)>=355 ,t1.event_revenue_usd,0)),2) as af_d355
,round(0.01*sum(if(t1.p_date between t1.d_date and (t1.d_date + interval '365 d') and (current_date-t1.d_date)>=365 ,t1.event_revenue_usd,0)),2) as af_d365
from tmp_af_purchase t1
group by
t1.d_date
,t1.platform
,t1.country_code
,t1.campaign_id
,t1.campaign_type
,t1.media_channel
)t5 on t0.d_date=t5.d_date and t0.platform=t5.platform and t0.country_code=t5.country_code and t0.campaign_id=t5.campaign_id and t0.media_channel=t5.media_channel and t0.campaign_type=t5.campaign_type
)a
on conflict(id)
do update set
d_date = excluded.d_date
,platform = excluded.platform
,country_code = excluded.country_code
,campaign_id = excluded.campaign_id
,media_channel = excluded.media_channel
,campaign_type = excluded.campaign_type
,campaign_name = excluded.campaign_name
,vid = excluded.vid
,vid_name = excluded.vid_name
,lang = excluded.lang
,lang_name = excluded.lang_name
,area = excluded.area
,country_name = excluded.country_name
,country_lable = excluded.country_lable
,install_uv = excluded.install_uv
,pay_amt = excluded.pay_amt
,pay_uv = excluded.pay_uv
,ad_cost = excluded.ad_cost
,date_diff = excluded.date_diff
,af_d0 = excluded.af_d0
,af_d1 = excluded.af_d1
,af_d2 = excluded.af_d2
,af_d3 = excluded.af_d3
,af_d4 = excluded.af_d4
,af_d5 = excluded.af_d5
,af_d6 = excluded.af_d6
,af_d7 = excluded.af_d7
,af_d8 = excluded.af_d8
,af_d9 = excluded.af_d9
,af_d10 = excluded.af_d10
,af_d11 = excluded.af_d11
,af_d12 = excluded.af_d12
,af_d13 = excluded.af_d13
,af_d14 = excluded.af_d14
,af_d15 = excluded.af_d15
,af_d16 = excluded.af_d16
,af_d17 = excluded.af_d17
,af_d18 = excluded.af_d18
,af_d19 = excluded.af_d19
,af_d20 = excluded.af_d20
,af_d21 = excluded.af_d21
,af_d22 = excluded.af_d22
,af_d23 = excluded.af_d23
,af_d24 = excluded.af_d24
,af_d25 = excluded.af_d25
,af_d26 = excluded.af_d26
,af_d27 = excluded.af_d27
,af_d28 = excluded.af_d28
,af_d29 = excluded.af_d29
,af_d30 = excluded.af_d30
,af_d31 = excluded.af_d31
,af_d32 = excluded.af_d32
,af_d33 = excluded.af_d33
,af_d34 = excluded.af_d34
,af_d35 = excluded.af_d35
,af_d36 = excluded.af_d36
,af_d37 = excluded.af_d37
,af_d38 = excluded.af_d38
,af_d39 = excluded.af_d39
,af_d40 = excluded.af_d40
,af_d41 = excluded.af_d41
,af_d42 = excluded.af_d42
,af_d43 = excluded.af_d43
,af_d44 = excluded.af_d44
,af_d45 = excluded.af_d45
,af_d46 = excluded.af_d46
,af_d47 = excluded.af_d47
,af_d48 = excluded.af_d48
,af_d49 = excluded.af_d49
,af_d50 = excluded.af_d50
,af_d51 = excluded.af_d51
,af_d52 = excluded.af_d52
,af_d53 = excluded.af_d53
,af_d54 = excluded.af_d54
,af_d55 = excluded.af_d55
,af_d56 = excluded.af_d56
,af_d57 = excluded.af_d57
,af_d58 = excluded.af_d58
,af_d59 = excluded.af_d59
,af_d60 = excluded.af_d60
,af_d61 = excluded.af_d61
,af_d62 = excluded.af_d62
,af_d63 = excluded.af_d63
,af_d64 = excluded.af_d64
,af_d65 = excluded.af_d65
,af_d66 = excluded.af_d66
,af_d67 = excluded.af_d67
,af_d68 = excluded.af_d68
,af_d69 = excluded.af_d69
,af_d70 = excluded.af_d70
,af_d71 = excluded.af_d71
,af_d72 = excluded.af_d72
,af_d73 = excluded.af_d73
,af_d74 = excluded.af_d74
,af_d75 = excluded.af_d75
,af_d76 = excluded.af_d76
,af_d77 = excluded.af_d77
,af_d78 = excluded.af_d78
,af_d79 = excluded.af_d79
,af_d80 = excluded.af_d80
,af_d81 = excluded.af_d81
,af_d82 = excluded.af_d82
,af_d83 = excluded.af_d83
,af_d84 = excluded.af_d84
,af_d85 = excluded.af_d85
,af_d86 = excluded.af_d86
,af_d87 = excluded.af_d87
,af_d88 = excluded.af_d88
,af_d89 = excluded.af_d89
,af_d90 = excluded.af_d90
,af_d91 = excluded.af_d91
,af_d92 = excluded.af_d92
,af_d93 = excluded.af_d93
,af_d94 = excluded.af_d94
,af_d95 = excluded.af_d95
,af_d96 = excluded.af_d96
,af_d97 = excluded.af_d97
,af_d98 = excluded.af_d98
,af_d99 = excluded.af_d99
,af_d100 = excluded.af_d100
,af_d101 = excluded.af_d101
,af_d102 = excluded.af_d102
,af_d103 = excluded.af_d103
,af_d104 = excluded.af_d104
,af_d105 = excluded.af_d105
,af_d106 = excluded.af_d106
,af_d107 = excluded.af_d107
,af_d108 = excluded.af_d108
,af_d109 = excluded.af_d109
,af_d110 = excluded.af_d110
,af_d111 = excluded.af_d111
,af_d112 = excluded.af_d112
,af_d113 = excluded.af_d113
,af_d114 = excluded.af_d114
,af_d115 = excluded.af_d115
,af_d116 = excluded.af_d116
,af_d117 = excluded.af_d117
,af_d118 = excluded.af_d118
,af_d119 = excluded.af_d119
,af_d120 = excluded.af_d120
,af_d121 = excluded.af_d121
,af_d122 = excluded.af_d122
,af_d123 = excluded.af_d123
,af_d124 = excluded.af_d124
,af_d125 = excluded.af_d125
,af_d126 = excluded.af_d126
,af_d127 = excluded.af_d127
,af_d128 = excluded.af_d128
,af_d129 = excluded.af_d129
,af_d130 = excluded.af_d130
,af_d131 = excluded.af_d131
,af_d132 = excluded.af_d132
,af_d133 = excluded.af_d133
,af_d134 = excluded.af_d134
,af_d135 = excluded.af_d135
,af_d136 = excluded.af_d136
,af_d137 = excluded.af_d137
,af_d138 = excluded.af_d138
,af_d139 = excluded.af_d139
,af_d140 = excluded.af_d140
,af_d141 = excluded.af_d141
,af_d142 = excluded.af_d142
,af_d143 = excluded.af_d143
,af_d144 = excluded.af_d144
,af_d145 = excluded.af_d145
,af_d146 = excluded.af_d146
,af_d147 = excluded.af_d147
,af_d148 = excluded.af_d148
,af_d149 = excluded.af_d149
,af_d150 = excluded.af_d150
,af_d151 = excluded.af_d151
,af_d152 = excluded.af_d152
,af_d153 = excluded.af_d153
,af_d154 = excluded.af_d154
,af_d155 = excluded.af_d155
,af_d156 = excluded.af_d156
,af_d157 = excluded.af_d157
,af_d158 = excluded.af_d158
,af_d159 = excluded.af_d159
,af_d160 = excluded.af_d160
,af_d161 = excluded.af_d161
,af_d162 = excluded.af_d162
,af_d163 = excluded.af_d163
,af_d164 = excluded.af_d164
,af_d165 = excluded.af_d165
,af_d166 = excluded.af_d166
,af_d167 = excluded.af_d167
,af_d168 = excluded.af_d168
,af_d169 = excluded.af_d169
,af_d170 = excluded.af_d170
,af_d171 = excluded.af_d171
,af_d172 = excluded.af_d172
,af_d173 = excluded.af_d173
,af_d174 = excluded.af_d174
,af_d175 = excluded.af_d175
,af_d176 = excluded.af_d176
,af_d177 = excluded.af_d177
,af_d178 = excluded.af_d178
,af_d179 = excluded.af_d179
,af_d180 = excluded.af_d180
,new_pay_amt = excluded.new_pay_amt
,af_d185 = excluded.af_d185
,af_d195 = excluded.af_d195
,af_d205 = excluded.af_d205
,af_d215 = excluded.af_d215
,af_d225 = excluded.af_d225
,af_d235 = excluded.af_d235
,af_d245 = excluded.af_d245
,af_d255 = excluded.af_d255
,af_d265 = excluded.af_d265
,af_d275 = excluded.af_d275
,af_d285 = excluded.af_d285
,af_d295 = excluded.af_d295
,af_d305 = excluded.af_d305
,af_d315 = excluded.af_d315
,af_d325 = excluded.af_d325
,af_d335 = excluded.af_d335
,af_d345 = excluded.af_d345
,af_d355 = excluded.af_d355
,af_d365 = excluded.af_d365
;