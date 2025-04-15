-------------------------------------------
-- 测试json_array_elements
-------------------------------------------
select user_layered_configs from public."oversea-api_osd_pushed";


select
     id::text as push_id
    ,to_timestamp(pushed_at) at time zone 'UTC-8' as push_time  --,to_timestamp(pushed_at) at time zone 'UTC-8'
    ,json_array_elements((REGEXP_MATCH(replace(user_layered_configs, '\', ''),'\[.*?\]'))[1]::json)->> 'id'
    --,json_array_elements(REPLACE(user_layered_configs, '\', '')::json ) ->> 'id' as layered_id
    ,delivered_count as push_unt
    ,click_count as click_unt
    from public."oversea-api_osd_pushed"

SELECT user_layered_configs,json_typeof(user_layered_configs::json),user_layered_configs
FROM public."oversea-api_osd_pushed"
WHERE json_typeof(user_layered_configs::json) != 'array';

--------------------------------------------
-- 测试转义
--------------------------------------------
-- 正确的方法
select replace('"[{\"id\":152,\"name\":\"PUSH日语-全量\"},{\"id\":3,\"name\":\"日语-临时分层\"}]"','\"','"')


select  json_array_elements((REGEXP_MATCH(replace('[{"id": 505, "name": "土耳其PUSH-全量用户"}, {"id": 14, "name": "土耳其语-临时分层"}]','\',''),'\[.*?\]'))[1]::json)->> 'id'
select json_array_elements(REGEXP_MATCH(replace('"[{\"id\":152,\"name\":\"PUSH日语-全量\"},{\"id\":3,\"name\":\"日语-临时分层\"}]"','\\',''),'\[(.*?)\]')::json)->>'id'