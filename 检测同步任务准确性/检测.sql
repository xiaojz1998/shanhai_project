------------------------------------------
-- file: 检测.sql
-- author: xiaoj
-- time: 2025/5/20 09:55
-- description:
------------------------------------------

-- ods_cs_data_log_ad_mob_total_log_hf 101042
select count(*) from ad_mob_total_log;

-- ad_media_report_data
select count(*) from ad_media_report_data;

--ods_oversea_api_osd_task_hf
select count(*) from "oversea-api_osd_task";

-- oversea_api.osd_video_scheduled_users
select count(*) from "oversea-api_osd_video_scheduled_users";

-- ods_oversea_api_osd_pushed_hf
select count(*) from "oversea-api_osd_pushed";

-- ods_oversea_api_osd_user_layered_configs_hf
select count(*) from "oversea-api_osd_user_layered_configs";

--  ods_oversea_api_osd_feedback_hf
select count(*) from "oversea-api_osd_feedback";

-- ods_oversea_api_osd_user_origin_hf
select count(*) from "oversea-api_osd_user_origin";

-- ods_oversea_api_osd_home_page_hf
select count(*) from "oversea-api_osd_home_page";

-- ods_oversea_api_osd_recommend_hf
select count(*) from "oversea-api_osd_recommend";