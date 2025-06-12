------------------------------------------
-- file: 测试1.sql
-- author: xiaoj
-- time: 2025/5/21 14:46
-- description:
------------------------------------------

-- "oversea-api_osd_home_page"  数据格式
select *
from  "oversea-api_osd_home_page"
limit 100

-- "oversea-api_osd_tabs"
select *
from  "oversea-api_osd_tabs"
limit 100

select
    ext_body
    ,CAST(ext_body::json ->> 'type' AS text) as type
from public.app_user_track_log
limit 10

select
    money
from public.app_user_track_log a
where event = 263
limit 100


select
    *
from "oversea-api_osd_recommend"
where english_name like '%playFinish%'

select
    column1
from "app_user_cover_show_log"
where 1=1
    and to_timestamp(created_at)::date between '2025-05-22' and  (CURRENT_DATE - INTERVAL '1 day')
    and event = 111
    and CAST(ext_body::json ->> 'page' AS int) = 3
    and ext_body::json ->> 'show_title' = 'playFinish'
limit 1000

select
    distinct recommend_name
from dw_recommend_playfinish