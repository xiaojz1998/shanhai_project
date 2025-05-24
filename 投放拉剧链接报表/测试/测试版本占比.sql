---------------------------------------------
-- File: 测试版本占比.sql
-- Time: 2025/5/23 15:09
-- User: xiaoj
-- Description:  
---------------------------------------------

with tmp_event_log as (
    select
        to_timestamp(created_at):: date as d_date       -- 日期
        , event                                         -- 时间
        , device_id                                     -- 设备id
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               else '未知' end as vid                    -- 取到vid 处理null
        , case when type = '0' then 'W2A'
               when type = '1' then '直投'
               else '未知' end as type                   -- 投放链路
        , app_version
        -- , case when ad_source_type = 0 then 1 else ad_source_type end as ad_source_type    -- 归因通道
    from "app_performance_event_log"
    where (event = 22 or event = 23 or event = 29 or event = 30) -- 限制event
         -- and CASE WHEN split_part(app_version, '.', 2) ~ '^\d+$' THEN split_part(app_version, '.', 2)::bigint ELSE 0 END >= 18 -- 限制版本
         and to_timestamp(created_at):: date >= '2025-05-01'::date  -- 限制时间
)
select
    d_date
    , 1.0*count(CASE WHEN split_part(app_version, '.', 2) ~ '^\d+$' and event = 22 THEN 1 ELSE null END )/count(CASE WHEN event = 22 THEN 1 ELSE null END) as r22
    , 1.0*count(CASE WHEN split_part(app_version, '.', 2) ~ '^\d+$' and event = 23 THEN 1 ELSE null END )/count(CASE WHEN event = 23 THEN 1 ELSE null END) as r23
    , 1.0*count(CASE WHEN split_part(app_version, '.', 2) ~ '^\d+$' and event = 29 THEN 1 ELSE null END )/count(CASE WHEN event = 29 THEN 1 ELSE null END) as r29
    , 1.0*count(CASE WHEN split_part(app_version, '.', 2) ~ '^\d+$' and event = 30 THEN 1 ELSE null END )/count(CASE WHEN event = 30 THEN 1 ELSE null END) as r30
from tmp_event_log
group by d_date