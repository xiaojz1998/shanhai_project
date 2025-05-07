------------------------------------------
-- file: 测试首页.sql
-- author: xiaoj
-- time: 2025/4/30 15:44
-- description:
------------------------------------------

select * from "web_user_track_log"
where event = 114 and  get_json_object(ext_body, '$.is_optimize') = '1'
limit 100

