---------------------------------------------
-- File: 验证sql.sql
-- Time: 2025/6/9 18:06
-- User: xiaoj
-- Description:  
---------------------------------------------

-- select
--     1.0*sum(次日留存)/sum(dau)
-- from (select
--     a.d_date
--     , count(distinct case when b.d_date - a.d_date = 1 then b.uid else 0 end) as 次日留存
--     , count(distinct a.uid) as dau
--     , 1.0*count(distinct case when b.d_date - a.d_date = 1 then b.uid else 0 end)/count(distinct a.uid)
-- from dwd_user_active a
-- left join dwd_user_active b on a.uid = b.uid and b.d_date > a.d_date
-- where a.d_date >= '2025-01-01' and a.d_date <= '2025-05-31'
-- group by a.d_date) c

-- select count(*) from public.dw_retention_daily;     -- 51w
-- select count(*) from public.dwd_user_active         -- 7kw

select distinct lang_name from public.dw_retention_daily
select distinct lang_name from public.ads_rpt_home_page_hi

select
    active_date
    , sum(t1.dau)
from (
    select
        -- 维度
        active_date
        , "区域"
        , "国家"
        , "系统"
        , lang_name
        -- 计算字段
        , sum("总次日留存")
        , sum("总3日留存")
        , sum("总7日留存")
        , sum("总14日留存")
        , sum("总30日留存")
        , sum("新用户数")
        , sum("新用户次日留存")
        , sum("新用户3日留存")
        , sum("新用户7日留存")
        , sum("新用户14日留存")
        , sum("新用户30日留存")
        , sum("新推广用户")
        , sum("新推广用户次日留存")
        , sum("新推广用户3日留存")
        , sum("新推广用户7日留存")
        , sum("新推广用户14日留存")
        , sum("新推广用户30日留存")
        , sum("新自然用户数")
        , sum("新自然用户次日留存")
        , sum("新自然用户3日留存")
        , sum("新自然用户7日留存")
        , sum("新自然用户14日留存")
        , sum("新自然用户30日留存")
        , sum("老用户数")
        , sum("老用户次日留存")
        , sum("老用户3日留存")
        , sum("老用户7日留存")
        , sum("老用户14日留存")
        , sum("老用户30日留存")
        , sum(dau_60login)
        , sum(dau_120login)
        , sum(new_dau_60login)
        , sum(new_dau_120login)
        , sum(new_dau_60login_campaign)
        , sum(new_dau_120login_campaign)
        , sum(new_dau_60login_natural)
        , sum(new_dau_120login_natural)
        , sum(old_dau_60login)
        , sum(old_dau_120login)
        , sum(dau_90login)
        , sum(dau_180login)
        , sum(dau_360login)
        , sum(new_dau_90login)
        , sum(new_dau_180login)
        , sum(new_dau_360login)
        , sum(old_dau_90login)
        , sum(old_dau_180login)
        , sum(old_dau_360login)
     from public.dw_retention_daily
     group by active_date, 区域, 国家, 系统, lang_name
) t
left join (
    select
        d_date
        ,area
        ,country_name
        ,os
        ,case when lang_name='其他' then 'UNKNOWN' else lang_name end as lang_name
        ,sum(active_uv) as dau
    from public.ads_rpt_home_page_hi
    group by d_date,area,country_name,os,lang_name
) t1 on  t.active_date = t1.d_date::date and t.区域 = t1.area and t.国家 =t1.country_name and t.系统 = t1.os and t.lang_name = t1.lang_name
left join (select country_name,country_grade from v_dim_country_area where not (country_name = '英国' and country_grade = '未分类') )t2 on t.国家 = t2.country_name
group by active_date

-- select * from v_dim_country_area where country_name = '英国'




-- 新自然次留验证
select
    a.d_date
    , 1.0*count(distinct b.uid)/count(distinct a.uid)
from dwd_user_info a
left join dwd_user_active b on a.uid = b.uid::text and a.d_date::date = b.d_date -1
where user_source = '自然流' and a.d_date >= '2025-06-01'
group by a.d_date

