---------------------------------------------
-- File: 修改.sql
-- Time: 2025/6/12 11:44
-- User: xiaoj
-- Description:  
---------------------------------------------
SET timezone ='UTC';
---------------------------------------------
-- 建表
---------------------------------------------

-- drop table if exists public.dw_retention_month;
CREATE TABLE if not exists public.dw_retention_month (
    id text NOT NULL,
    active_month text,
    "区域" text,
    "国家" text,
    "系统" text,
    mau bigint,
    "总次月留存" bigint,
    "总2月留存" bigint,
    "总3月留存" bigint,
    "新用户数" bigint,
    "新用户次月留存" bigint,
    "新用户2月留存" bigint,
    "新用户3月留存" bigint,
    "新推广用户数" bigint,
    "新推广用户次月留存" bigint,
    "新推广用户2月留存" bigint,
    "新推广用户3月留存" bigint,
    "新自然用户数" bigint,
    "新自然用户次月留存" bigint,
    "新自然用户2月留存" bigint,
    "新自然用户3月留存" bigint,
    "老用户数" bigint,
    "老用户次月留存" bigint,
    "老用户2月留存" bigint,
    "老用户3月留存" bigint,
    "月充值人数" bigint,
    lang_name text,
    is_paid integer
    ,PRIMARY KEY (id)
);


---------------------------------------------
-- 更新
---------------------------------------------






-- 月留存
create table if not exists tmp.tmp_dw_retention_month_tmp01  as
SELECT
    md5(CONCAT(a.active_month,a.area,a.country_name,a.os,a.lang_name)) as id
    ,a.active_month
    ,coalesce(a.area,'未知') AS 区域
    ,coalesce(a.country_name,'未知') as 国家
    ,coalesce(a.os,'未知') as 系统
    ,coalesce(a.lang_name,'未知') as lang_name
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
    ,max(a.is_paid) as is_paid
FROM tmp.dw_retention_detail_tmp01 a
LEFT JOIN tmp.dw_retention_detail_tmp01 b ON a.uid = b.uid AND b.week_end > a.week_end
WHERE a.active_date <= '2025-06-12'
GROUP BY
    a.active_month
    ,coalesce(a.area,'未知')
    ,coalesce(a.country_name,'未知')
    ,coalesce(a.os,'未知')
    ,coalesce(a.lang_name,'未知') ;

-- 补充月充值人数字段
create table if not exists tmp.tmp_dw_retention_month_tmp02 as
select
    to_char(to_timestamp(created_at),'yyyy-mm') as active_month
    ,coalesce(b.area,'未知') AS 区域
    ,coalesce(b.country_name,'未知') as 国家
    ,coalesce(b.os,'未知') as 系统
    ,coalesce(b.lang_name,'未知') as lang_name
    , count(distinct a.uid) as 月充值人数
from public.all_order_log a
left join public.dwd_user_info b on a.uid::text = b.uid
where 1 = 1
    and status = 1
    and environment = 1
    and money > 0
group by to_char(to_timestamp(created_at),'yyyy-mm')
    , coalesce(b.area,'未知')
    , coalesce(b.country_name,'未知')
    , coalesce(b.os,'未知')
    , coalesce(b.lang_name,'未知');

with tmp_primary as (
    select
        active_month,区域,国家,系统,lang_name
    from
    (
        select active_month,区域,国家,系统,lang_name from  tmp.tmp_dw_retention_month_tmp01
        union all
        select active_month,区域,国家,系统,lang_name from tmp.tmp_dw_retention_month_tmp02
    ) t
    group by active_month,区域,国家,系统,lang_name
)
insert into public.dw_retention_month
select
    md5(CONCAT(t.active_month,t.区域,t.国家,t.系统,t.lang_name)) as id
    , t.active_month
    , t.区域
    , t.国家
    , t.系统
    , t0.mau
    , t0.总次月留存
    , t0.总2月留存
    , t0.总3月留存
    , t0.新用户数
    , t0.新用户次月留存
    , t0.新用户2月留存
    , t0.新用户3月留存
    , t0.新推广用户数
    , t0.新推广用户次月留存
    , t0.新推广用户2月留存
    , t0.新推广用户3月留存
    , t0.新自然用户数
    , t0.新自然用户次月留存
    , t0.新自然用户2月留存
    , t0.新自然用户3月留存
    , t0.老用户数
    , t0.老用户次月留存
    , t0.老用户2月留存
    , t0.老用户3月留存
    , t1.月充值人数
    , t.lang_name
    , t0.is_paid
from tmp_primary t
left join tmp.tmp_dw_retention_month_tmp01 t0 on t.active_month = t0.active_month and t.区域 = t0.区域 and t.国家 = t0.国家 and t.系统 = t0.系统 and t.lang_name = t0.lang_name
left join tmp.tmp_dw_retention_month_tmp02 t1 on t.active_month = t1.active_month and t.区域 = t1.区域 and t.国家 = t1.国家 and t.系统 = t1.系统 and t.lang_name = t1.lang_name
ON CONFLICT(id)
DO UPDATE SET
    active_month = excluded.active_month
    ,区域 = excluded.区域
    ,国家 = excluded.国家
    ,系统 = excluded.系统
    ,MAU = excluded.MAU
    ,总次月留存 = excluded.总次月留存
    ,总2月留存 = excluded.总2月留存
    ,总3月留存 = excluded.总3月留存
    ,新用户数 = excluded.新用户数
    ,新用户次月留存 = excluded.新用户次月留存
    ,新用户2月留存 = excluded.新用户2月留存
    ,新用户3月留存 = excluded.新用户3月留存
    ,新推广用户数 = excluded.新推广用户数
    ,新推广用户次月留存 = excluded.新推广用户次月留存
    ,新推广用户2月留存 = excluded.新推广用户2月留存
    ,新推广用户3月留存 = excluded.新推广用户3月留存
    ,新自然用户数 = excluded.新自然用户数
    ,新自然用户次月留存 = excluded.新自然用户次月留存
    ,新自然用户2月留存 = excluded.新自然用户2月留存
    ,新自然用户3月留存 = excluded.新自然用户3月留存
    ,老用户数 = excluded.老用户数
    ,老用户次月留存 = excluded.老用户次月留存
    ,老用户2月留存 = excluded.老用户2月留存
    ,老用户3月留存 = excluded.老用户3月留存
    ,月充值人数 = excluded.月充值人数
    ,lang_name = excluded.lang_name
    ,is_paid = excluded.is_paid
;
drop table if exists tmp.tmp_dw_retention_month_tmp01;
drop table if exists tmp.tmp_dw_retention_month_tmp02;




--
-- with tmp_retention_data as (
--     SELECT md5(CONCAT(a.active_month,a.area,a.country_name,a.os,a.lang_name)) as id
--         ,a.active_month
--         ,a.area AS 区域
--         ,a.country_name as 国家
--         ,a.os as 系统
--         ,COUNT(DISTINCT a.uid) AS mau
--         ,COUNT(DISTINCT CASE WHEN to_char(to_date(b.active_month,'yyyy-mm') - interval '1 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 总次月留存
--         ,COUNT(DISTINCT CASE WHEN to_char(to_date(b.active_month,'yyyy-mm') - interval '2 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 总2月留存
--         ,COUNT(DISTINCT CASE WHEN to_char(to_date(b.active_month,'yyyy-mm') - interval '3 month','yyyy-mm') = a.active_month  THEN b.uid ELSE NULL END) 总3月留存
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month THEN a.uid ELSE NULL END) 新用户数
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '1 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 新用户次月留存
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '2 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 新用户2月留存
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '3 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 新用户3月留存
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND a.user_source='推广流' THEN a.uid ELSE NULL END) 新推广用户数
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '1 month','yyyy-mm') = a.active_month AND a.user_source='推广流'  THEN b.uid ELSE NULL END) 新推广用户次月留存
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '2 month','yyyy-mm') = a.active_month AND a.user_source='推广流' THEN b.uid ELSE NULL END) 新推广用户2月留存
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '3 month','yyyy-mm') = a.active_month AND a.user_source='推广流'  THEN b.uid ELSE NULL END) 新推广用户3月留存
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND a.user_source='自然流' THEN a.uid ELSE NULL END) 新自然用户数
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '1 month','yyyy-mm') = a.active_month AND a.user_source='自然流'  THEN b.uid ELSE NULL END) 新自然用户次月留存
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '2 month','yyyy-mm') = a.active_month AND a.user_source='自然流' THEN b.uid ELSE NULL END) 新自然用户2月留存
--         ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND to_char(to_date(b.active_month,'yyyy-mm') - interval '3 month','yyyy-mm') = a.active_month AND a.user_source='自然流'  THEN b.uid ELSE NULL END) 新自然用户3月留存
--         ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) THEN a.uid ELSE NULL END) 老用户数
--         ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) AND to_char(to_date(b.active_month,'yyyy-mm') - interval '1 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 老用户次月留存
--         ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) AND to_char(to_date(b.active_month,'yyyy-mm') - interval '2 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 老用户2月留存
--         ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) AND to_char(to_date(b.active_month,'yyyy-mm') - interval '3 month','yyyy-mm') = a.active_month THEN b.uid ELSE NULL END) 老用户3月留存
--         -- ,count(distinct case when a.pay_amt > 0 then a.uid else null end) as 月充值人数
--         ,a.lang_name
--         ,max(a.is_paid) as is_paid
--     FROM tmp.dw_retention_detail_tmp01 a
--     LEFT JOIN tmp.dw_retention_detail_tmp01 b ON a.uid = b.uid AND b.week_end > a.week_end
--     WHERE a.active_date <= '2025-06-11'
--     GROUP BY
--         a.active_month
--         ,a.area
--         ,a.country_name
--         ,a.os
--         ,a.lang_name
-- )
-- , tmp_pay as (
--     select
--         to_char(to_timestamp(created_at),'yyyy-mm') as active_month
--         , b.area as 区域
--         , b.country_name as 国家
--         , b.os as 系统
--         , b.lang_name
--         , count(distinct a.uid) as 月充值人数
--     from public.all_order_log a
--     left join public.dwd_user_info b on a.uid::text = b.uid
--     where 1 = 1
--         and status = 1
--         and environment = 1
--         and money > 0
--     group by to_char(to_timestamp(created_at),'yyyy-mm')
--             , b.area
--             , b.country_name
--             , b.os
--             , b.lang_name
-- )
-- insert into public.dw_retention_month
-- select
--     a.id
--     , a.active_month
--     , a.区域
--     , a.国家
--     , a.系统
--     , a.mau
--     , a.总次月留存
--     , a.总2月留存
--     , a.总3月留存
--     , a.新用户数
--     , a.新用户次月留存
--     , a.新用户2月留存
--     , a.新用户3月留存
--     , a.新推广用户数
--     , a.新推广用户次月留存
--     , a.新推广用户2月留存
--     , a.新推广用户3月留存
--     , a.新自然用户数
--     , a.新自然用户次月留存
--     , a.新自然用户2月留存
--     , a.新自然用户3月留存
--     , a.老用户数
--     , a.老用户次月留存
--     , a.老用户2月留存
--     , a.老用户3月留存
--     , b.月充值人数
--     , a.lang_name
--     , a.is_paid
-- from tmp_retention_data a
-- left join tmp_pay b
--     on a.active_month = b.active_month
--        and a.区域 = b.区域
--        and a.国家 = b.国家
--        and a.系统 = b.系统
--        and a.lang_name = b.lang_name
-- ON CONFLICT(id)
-- DO UPDATE SET
--     active_month = excluded.active_month
--     ,区域 = excluded.区域
--     ,国家 = excluded.国家
--     ,系统 = excluded.系统
--     ,MAU = excluded.MAU
--     ,总次月留存 = excluded.总次月留存
--     ,总2月留存 = excluded.总2月留存
--     ,总3月留存 = excluded.总3月留存
--     ,新用户数 = excluded.新用户数
--     ,新用户次月留存 = excluded.新用户次月留存
--     ,新用户2月留存 = excluded.新用户2月留存
--     ,新用户3月留存 = excluded.新用户3月留存
--     ,新推广用户数 = excluded.新推广用户数
--     ,新推广用户次月留存 = excluded.新推广用户次月留存
--     ,新推广用户2月留存 = excluded.新推广用户2月留存
--     ,新推广用户3月留存 = excluded.新推广用户3月留存
--     ,新自然用户数 = excluded.新自然用户数
--     ,新自然用户次月留存 = excluded.新自然用户次月留存
--     ,新自然用户2月留存 = excluded.新自然用户2月留存
--     ,新自然用户3月留存 = excluded.新自然用户3月留存
--     ,老用户数 = excluded.老用户数
--     ,老用户次月留存 = excluded.老用户次月留存
--     ,老用户2月留存 = excluded.老用户2月留存
--     ,老用户3月留存 = excluded.老用户3月留存
--     ,月充值人数 = excluded.月充值人数
--     ,lang_name = excluded.lang_name
--     ,is_paid = excluded.is_paid
-- ;




-- 验证



with tmp_pay as (
    select
    to_char(to_timestamp(created_at),'yyyy-mm') as active_month
    ,coalesce(b.area,'未知') AS 区域
    ,coalesce(b.country_name,'未知') as 国家
    ,coalesce(b.os,'未知') as 系统
    ,coalesce(b.lang_name,'未知') as lang_name
    , count(distinct a.uid) as 月充值人数
from public.all_order_log a
left join public.dwd_user_info b on a.uid::text = b.uid
where 1 = 1
    and status = 1
    and environment = 1
    and money > 0
group by to_char(to_timestamp(created_at),'yyyy-mm')
    , coalesce(b.area,'未知')
    , coalesce(b.country_name,'未知')
    , coalesce(b.os,'未知')
    , coalesce(b.lang_name,'未知')
)
-- select count(*) from tmp_pay where active_month = '2025-04' -- 1373
select *
from public.dw_retention_month t
left join tmp_pay t0 on t.active_month  = t0.active_month and t.区域 = t0.区域 and t.国家 = t0.国家 and t.系统 = t0.系统 and t.lang_name = t0.lang_name
where t.active_month = '2025-04' and t.月充值人数 is not null and t0.active_month is null


