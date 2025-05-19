------------------------------------------
-- file: 添加落地页样式.sql
-- author: xiaoj
-- time: 2025/5/16 16:36
-- description:
------------------------------------------
-- 测试 "oversea-api_osd_landing_page" 的数量
select
    count(*)
from "oversea-api_osd_landing_page"

-- 查看 "oversea-api_osd_landing_page" 内容
select
    *
from "oversea-api_osd_landing_page"
limit 100

-- 测试 "oversea-api_osd_user"
select
    count(*)
from "oversea-api_osd_user"

select
    *
from "oversea-api_osd_user"
limit 100





---------------------------------------------------------------
--
select
    *
from "web_user_track_log"
where uid != 0
limit 100


select *
from "oversea-api_osd_video_episodes"
limit 100