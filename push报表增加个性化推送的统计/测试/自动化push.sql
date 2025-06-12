---------------------------------------------
-- File: 自动化push.sql
-- Time: 2025/5/29 14:54
-- User: xiaoj
-- Description:  
---------------------------------------------
with tmp_user_layered_info as (
    select
        t1.id
        , t1.lang_code
	    , t2."name" as lang_name
	from(
        select
            id
            , "name"                                        -- 分层名
            , lang_config::json ->> 0 as lang_code          -- 语言码
        from public."oversea-api_osd_user_layered_configs"  -- 用户分层表
        where 1=1
            and lang_config <>'[]'
            -- and "name" like '%英语PUSH-全量用户%'
	) t1
	left join public."oversea-api_osd_lang" t2 on t1.lang_code=t2.lang_code
)
select
    push_id
    , push_time
    , string_agg(layered_name,';' order by layered_name) as layered_name        -- 用户分层名称
    , array_agg(distinct lang_name)::text as lang_name                          -- 用户分层语言
    , push_title
    , push_content
    , sent_unt
    , push_unt
    , click_unt
from (
    select
        t1.*
        , case when t2.lang_name='英语阿拉伯语' then '阿拉伯语' else t2.lang_name end as lang_name
    from (select
        concat('auto_',t0.id) as push_id
        , to_timestamp(t0.created_at)  at time zone 'UTC-8' as push_time
        , json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'id' as layered_id
        , json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'name' as layered_name
        , title as push_title                        -- 推送标题
        , "content" as push_content                  -- 推送内容
        , sent_count as sent_unt                     -- 命中用户数 推送人数
        , delivered_count as push_unt                -- 触达人数
        , click_count as click_unt                   -- 点击人数
    from public."oversea-api_osd_auto_pushes" t
    left join public."oversea-api_osd_auto_push_logs" t0 on t.id = t0.task_id) t1
    left join tmp_user_layered_info t2 on t1.layered_id = t2.id::text   -- 补充用户分层语言
) a
group by push_id
        , push_time
        , push_title
        , push_content
        , sent_unt
        , push_unt
        , click_unt

select * from public."oversea-api_osd_auto_pushes"

select * from public."oversea-api_osd_auto_push_logs"


