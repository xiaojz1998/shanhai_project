------------------------------------------
-- file: 验证.sql
-- author: xiaoj
-- time: 2025/5/21 11:28
-- description:
------------------------------------------

-- 验证激活率
with camp_newuser_tb as (
  select
    d_date :: date as p_date,
    t.country_code,
    upper(os) as os,
    t.campaign_id,
    t.uid :: int8 as uid,
    coalesce(style::text,'未知') as style
    -- sid,
    -- string_id,
  from
    public.dwd_user_info t
  left join (select uid,sid from "oversea-api_osd_user") t0 on t.uid::bigint = t0.uid
  left join (select string_id,land_page_id from "oversea-api_osd_popular_links") t1 on t0.sid = t1.string_id
  left join (select id, style from "oversea-api_osd_landing_page") t2 on t1.land_page_id = t2.id
  where
    is_campaign = 1
),
--  推广流新增用户
camp_newuser_info as (
  select
    p_date,
    campaign_id,
    country_code,
    os,
    style,
    count(distinct uid) as register_users
  from
    camp_newuser_tb
  group by
    p_date,
    campaign_id,
    country_code,
    os,
    style
)select
     distinct style
 from camp_newuser_info
 where p_date = '2025-05-20'
