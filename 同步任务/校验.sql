---------------------------------------------
-- File: 校验.sql
-- Time: 2025/6/2 15:57
-- User: xiaoj
-- Description:  
---------------------------------------------





-- push信息校验
select count(*) from "oversea-api_osd_personalized_push_configs"

select count(*) from "oversea-api_osd_personalize_push_statistic"

select count(*) from "oversea-api_osd_auto_push_logs"

select count(*) from "oversea-api_osd_auto_pushes"
