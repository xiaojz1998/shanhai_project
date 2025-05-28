---------------------------------------------
-- File: 测试.sql
-- Time: 2025/5/26 10:05
-- User: xiaoj
-- Description:  
---------------------------------------------
--  public.dw_push_view 数据量 115254
-- select count(*) from public.dw_push_view;

-- 查看user_layered_configs
select
    push_id,
    string_agg(layered_name ,';' order by layered_name)
from (select
    id::text as push_id,
    user_layered_configs ,
       json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'id' as layered_id,
       json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'name' as layered_name
from public."oversea-api_osd_pushed" ) t
group by push_id;

select
    id::text as push_id,
    user_layered_configs ,
       json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) as layered_id,
       json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) as layered_name
from public."oversea-api_osd_pushed";

select
    count(distinct  id)
from public."oversea-api_osd_pushed"

select
    count(*)
from public."oversea-api_osd_personalized_push_configs"

-- 12514
-- 2101
select
    push_time
    , to_timestamp(updated_at) at time zone 'UTC-8' as push_time_c
from public."oversea-api_osd_personalize_push_statistic" where push_id = 8455

-- 查看个性化推送会不会在同一天推送两次
-- 首先确认了 created_at utc-8 对应了 push_time
-- 不会出现同一天推送了两次的情况
select
    push_id,
    to_timestamp(created_at) at time zone 'UTC-0',
    count(*)
from public."oversea-api_osd_personalize_push_statistic"
group by push_id, created_at
having count(*) = 1

select
    *
from dw_push_view
where push_id like '%person%'
limit 100


select
    *
from public."oversea-api_osd_personalize_push_statistic" where id = 5339

