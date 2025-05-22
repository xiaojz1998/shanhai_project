------------------------------------------
-- file: 测试首页.sql
-- author: xiaoj
-- time: 2025/5/20 10:44
-- description:
------------------------------------------
-- 筛选条件
-- event 114: 展示  115：点击  127：跳转链接上报
-- ext_body.is_optimize     判断新老落地页，0：重构前 1：重构后
-- ext_body.reason          判断自动和手动 0：自动 1：手动
-- ext_body.go_page_url
-- device_id: 设备id

-- 114\115\127 检查完毕
select
    *
from "web_user_track_log"
where event = 114
     and page_url not like '%/app/%'
     -- and get_json_object(ext_body, '$.is_optimize') = '1'
     and to_timestamp(created_at) :: date between '2025-04-15' and CURRENT_DATE
limit 100

-- 138490262
select
    count(*)
from "web_user_track_log"
where event in (114,115,127)
    and to_timestamp(created_at) :: date between '2025-04-15' and CURRENT_DATE;


-- 测试取到落地页id
SELECT substring('https://fb.stardust-tv.com/app/25138.html?p1=SH_Facebook_W2A_%E7%BF%BB%E8%AF%91_5479_%E5%86%B0%'
                FROM '/app/([0-9]+)') AS app_id;

SELECT (regexp_matches('https://fb.stardust-tv.com/abp/25138.html?p1=SH_Facebook_W2A_%E7%BF%BB%E8%AF%91_5479_%E5%86%B0%',
                      '/app/([0-9]+)'))[1] AS app_id;

-- "oversea-api_osd_user" 表
select
    count(*)
from "oversea-api_osd_user";

-- "oversea-api_osd_popular_links" 36471
select
    count(*)
from "oversea-api_osd_popular_links";

select
    count(distinct string_id)
from "oversea-api_osd_popular_links";

-- "oversea-api_osd_landing_page" 26613
select
    count(*)
from "oversea-api_osd_landing_page"

-- 查看uid是否重复
select
    uid,
    count(*)
from "oversea-api_osd_user"
group by uid
having count(*)>1

-- 测试埋点表链接 落地页样式

select
    page_url,style
FROM "web_user_track_log" t
left join "oversea-api_osd_landing_page" t0 on substring(t.page_url,'/app/([0-9]+)') = t0.id::text
WHERE
    event in (114, 115, 127)        -- 这三个字段都能获取落地页样式字段
    and get_json_object(ext_body, '$.is_optimize') = '1'        -- 统一取重构后的落地页
limit 100


select null::text;
-------------------------------------------------------------
