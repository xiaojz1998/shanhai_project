------------------------------------------------
--  ROI7 实现
--  含义：新用户未来7天的支付总和/当天的广告消耗金额
--  当日 ad_cost
--  pay_7
------------------------------------------------
-- 获取每一天的新增用户
with new_reg_users as(
	select v_date as created_date
	,d_date as d_date
	,uid::int8 as uid
	,country_code
	,lang
	,lang_name
	from public.dwd_user_info
),
-- 每日用户订单统计
tmp_user_pay as (
    select d_date,uid
	    ,sum(pay_amt) as pay_amt -- 新用户充值金额（未减退款，与指标概览保持一致）
	    from(
	    	-- 每日用户订单统计
	    	select
	    	to_char( to_timestamp(created_at),'YYYY-MM-DD') as d_date
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
)
-- pay_7 计算
select
    d_date,
    country_code,
    lang,
    sum(case when d_date::date between (p_date::date+interval'- 7 d')::date and p_date::date  and (d_date::date+interval' 7 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_7
from (select
    t1.d_date,
    t2.d_date as p_date,
    t1.country_code,
    t1.lang,
    t1.lang_name,
    sum(t2.pay_amt)::decimal(20,2) as new_pay_amt
from new_reg_users t1
left join tmp_user_pay t2 on t1.uid = t2.uid and t1.d_date <= t2.d_date
group by t1.d_date,t2.d_date,t1.country_code,t1.lang,t1.lang_name) t3
group by d_date,country_code, lang,lang_name;
-- 再用 日期 国家 语言的ad_cost 就能求出roi_7

------------------------------------------------
--  实现总7留存率
--  七天后的留存用户数（当天活跃用户中还活跃的）/ 当天的活跃用户数（dau）
--  当天活跃用户数dau在dw_operate_schedule中有
--  以下为求七天后的留存用户数、留存率、dau
--  维度： 日期、国家、语言
------------------------------------------------
-- 获取每一天的新增用户
with new_reg_users as (
    SELECT
        d_date::date ,
        a.country_code ,
        b.country_name,
        a.area ,
        uid::int8 as uid,
        a.lang ,
        a.lang_name
        FROM public.dwd_user_info a LEFT JOIN v_dim_country_area b ON UPPER(a.country_code) = b.country_code
),
-- 活跃用户表
tmp_active_users as (
    SELECT d_date as active_date, uid::int8 as uid
    FROM public.dwd_user_active
),
-- 活跃用户补充国家和语言字段
t as (
    select
        active_date,
        country_code,
        country_name,
        lang,
        lang_name,
        tmp_active_users.uid
    from tmp_active_users left join new_reg_users on tmp_active_users.uid = new_reg_users.uid
)
select
    a.active_date,
    a.country_code,
    a.country_name,
    a.lang,
    a.lang_name,
    COUNT(DISTINCT a.uid) dau,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 7 THEN b.uid ELSE NULL END) as total_retention_7d,
    case when COUNT(DISTINCT a.uid)=0 then null
		else 1.0*COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 7 THEN b.uid ELSE NULL END)/COUNT(DISTINCT a.uid)
		end as total_retention_rate_7d
from t a left join t b on a.uid = b.uid and b.active_date > a.active_date
where a.active_date is not null and a.active_date between '2024-07-01' and (current_date+interval '-1 day')
group by a.active_date,a.country_code,a.country_name,a.lang,a.lang_name;

------------------------------------------------
--  实现push点击率
--  因为国家字段提取困难，直接用dw_push_view 表统计语言即可
--  维度： 日期和语言
------------------------------------------------
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
    case when t2.lang_name='繁体中文' then '中文'
         when t2.lang_name='简体中文' then '中文'
         when t2.lang_name='英语阿拉伯语' then '阿拉伯语'
         when t2.lang_name=''or t2.lang_name is null then 'UNKNOWN' else t2.lang_name end as lang
from(
	select
	 id::text as push_id
	,to_timestamp(pushed_at) at time zone 'UTC-8' as push_time  --,to_timestamp(pushed_at) at time zone 'UTC-8'
	,json_array_elements(user_layered_configs::json ) ->> 'id' as layered_id
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





