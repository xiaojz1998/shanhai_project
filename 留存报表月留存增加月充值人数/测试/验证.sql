---------------------------------------------
-- File: 验证.sql
-- Time: 2025/6/9 16:27
-- User: xiaoj
-- Description:  
---------------------------------------------
with tmp_pay as (
    -- 维度：日期 uid
	-- 度量字段：每日uid 付款总金额
    select
        to_timestamp(created_at):: date as d_date   -- 交易日期
        , o.uid                                     -- 交易uid
        , sum(o.money) as  pay_amt                  -- 每个交易日期的付款金额
    from public.all_order_log o
    where 1=1
        and o.environment = 1
        and o.os in('android','ios')
        and o.status = 1
        and o.created_date>=20240701
    group by to_timestamp(created_at):: date , o.uid
    -- exclude refund?
)
select
    sum(uv)
from (select
    lang_name
    , b.country_grade
    , count(distinct tp.uid) uv
from tmp_pay tp
left join dwd_user_active a on tp.uid = a.uid and tp.d_date = a.d_date
left join v_dim_country_area  b on a.reg_country = b.country_code
where TO_CHAR(tp.d_date::timestamp, 'YYYY-MM') = '2025-05' and pay_amt > 0
group by a.lang_name, b.country_grade) t


select
    count(distinct uid)
from all_order_log o
where 1=1
    and o.environment = 1
    -- and o.os in('android','ios')
    and o.status = 1
    and o.created_date>=20240701
    and money > 0
    and TO_CHAR(to_timestamp(created_at), 'YYYY-MM') = '2025-04'



select
    sum(月充值人数)
from (select
    id
    , t.active_month
    , t.区域
    , t.国家
    , t.系统
    , mau
    , 总次月留存
    , 总2月留存
    , 总3月留存
    , 新用户数
    , 新用户次月留存
    , 新用户2月留存
    , 新用户3月留存
    , 新推广用户数
    , 新推广用户次月留存
    , 新推广用户2月留存
    , 新推广用户3月留存
    , 新自然用户数
    , 新自然用户次月留存
    , 新自然用户2月留存
    , 新自然用户3月留存
    , 老用户数
    , 老用户次月留存
    , 老用户2月留存
    , 老用户3月留存
    , 月充值人数
    , t.lang_name
    , is_paid
from (
    SELECT
        md5(CONCAT(a.active_month,a.area,a.country_name,a.os,a.lang_name)) as id
        ,a.active_month
        ,coalesce(a.area,'未知') AS 区域
        ,coalesce(a.country_name,'未知') as 国家
        ,coalesce(a.os,'未知') as 系统
        ,COUNT(DISTINCT a.uid) AS mau
        ,COUNT(DISTINCT CASE WHEN to_char(to_date(b.active_month,'yyyy-mm') - interval '1 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 总次月留存
        ,COUNT(DISTINCT CASE WHEN to_char(to_date(b.active_month,'yyyy-mm') - interval '2 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 总2月留存
        ,COUNT(DISTINCT CASE WHEN to_char(to_date(b.active_month,'yyyy-mm') - interval '3 month','yyyy-mm') = a.active_month  THEN b.uid ELSE NULL END) 总3月留存
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month THEN a.uid ELSE NULL END) 新用户数
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '1 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 新用户次月留存
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '2 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 新用户2月留存
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '3 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 新用户3月留存
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND a.user_source='推广流' THEN a.uid ELSE NULL END) 新推广用户数
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '1 month','yyyy-mm') = a.active_month AND a.user_source='推广流'  THEN b.uid ELSE NULL END) 新推广用户次月留存
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '2 month','yyyy-mm') = a.active_month AND a.user_source='推广流' THEN b.uid ELSE NULL END) 新推广用户2月留存
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '3 month','yyyy-mm') = a.active_month AND a.user_source='推广流'  THEN b.uid ELSE NULL END) 新推广用户3月留存
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND a.user_source='自然流' THEN a.uid ELSE NULL END) 新自然用户数
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '1 month','yyyy-mm') = a.active_month AND a.user_source='自然流'  THEN b.uid ELSE NULL END) 新自然用户次月留存
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '2 month','yyyy-mm') = a.active_month AND a.user_source='自然流' THEN b.uid ELSE NULL END) 新自然用户2月留存
        ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '3 month','yyyy-mm') = a.active_month AND a.user_source='自然流'  THEN b.uid ELSE NULL END) 新自然用户3月留存
        ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) THEN a.uid ELSE NULL END) 老用户数
        ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) AND to_char(to_date(b.active_month,'yyyy-mm') - interval '1 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 老用户次月留存
        ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) AND to_char(to_date(b.active_month,'yyyy-mm') - interval '2 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 老用户2月留存
        ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) AND to_char(to_date(b.active_month,'yyyy-mm') - interval '3 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 老用户3月留存
         -- ,count(distinct case when a.pay_amt > 0 then a.uid else null end) as 月充值人数
        ,coalesce(a.lang_name,'未知') as lang_name
        ,max(a.is_paid) as is_paid
    FROM tmp.dw_retention_detail_tmp01 a
    LEFT JOIN tmp.dw_retention_detail_tmp01 b ON a.uid = b.uid AND b.week_end > a.week_end
    WHERE a.active_date <= '2025-06-11'
    GROUP BY
        a.active_month
        ,a.area
        ,a.country_name
        ,a.os
        ,a.lang_name
) t left join (
    select
        to_char(to_timestamp(a.created_at),'yyyy-mm') as active_month
        , coalesce(b.area,'未知') as 区域
        , coalesce(b.country_name,'未知') as 国家
        , coalesce(b.os,'未知') as 系统
        , coalesce(b.lang_name,'未知') as lang_name
        , count(distinct a.uid) as 月充值人数
    from public.all_order_log a
    left join public.dwd_user_info b on a.uid::text = b.uid
    where 1=1
      and status = 1
      and environment  = 1
      and money > 0
    group by
        to_char(to_timestamp(a.created_at),'yyyy-mm')
        ,coalesce(b.area,'未知')
        ,coalesce(b.country_name,'未知')
        ,coalesce(b.os,'未知')
        ,coalesce(b.lang_name,'未知')
) t0 on t.active_month = t0.active_month
  and t.区域 = t0.区域
  and t.国家 = t0.国家
  and t.系统 = t0.系统
  and t.lang_name = t0.lang_name) t
where active_month = '2025-04'

-- 245712
select
    sum(月充值人数)
from (select
        to_char(to_timestamp(a.created_at),'yyyy-mm') as active_month
        , coalesce(b.area,'未知') as 区域
        , coalesce(b.country_name,'未知') as 国家
        , coalesce(b.os,'未知') as 系统
        , coalesce(b.lang_name,'未知') as lang_name
        , count(distinct a.uid) as 月充值人数
    from public.all_order_log a
    left join public.dwd_user_info b on a.uid::text = b.uid
    where 1=1
      and status = 1
      and environment  = 1
      and money > 0
    group by
        to_char(to_timestamp(a.created_at),'yyyy-mm')
        ,coalesce(b.area,'未知')
        ,coalesce(b.country_name,'未知')
        ,coalesce(b.os,'未知')
        ,coalesce(b.lang_name,'未知')) t
where active_month = '2025-04'



