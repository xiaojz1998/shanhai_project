--------------------------------------------------------------
--  确认event和event_name
--  埋点表 app_user_track_log
--  firefly广告观看     rewards_firefly_go_click    266
--  adcloud广告观看     rewards_adcloud_go_click    269
--  看完开屏/插屏广告       ad_watch_finish           262
--  签到处点击           rewards_checkin_ads         39
--  点击签到后出现看广告弹窗rewards_checkin_popup       36
--  剧集解锁广告弹窗点击   drama_adpopup_click         5
--  阶梯广告观看          rewards_task_go            41 ext_body:{"task":"Watch Ads"}

--  性能埋点表 app_performance_event_log
--  广告页面填充成功       ads_load_succeed           6
--  广告页面填充失败	    ads_load_fail             7
--  广告页面展示成功	    ads_show_succeed          8
--  广告页面展示失败	    ads_show_fail             9
--------------------------------------------------------------
select
    event_name,
    event,
    ext_body
from app_user_track_log
where created_date='20250310'and event_name='rewards_task_go'
limit 10;


select
    *
from   app_performance_event_log
where event = 7 and type in ('0','1','2','3')
limit 1000
--------------------------------------------------------------
--  埋点数据量
--------------------------------------------------------------
-- 性能埋点表 10394310276 110亿
select
    count(*)
from app_performance_event_log;

-- 埋点表 5165010505   50亿
select
    count(*)
from app_user_track_log;

-- 埋点表广告数据量 19千万 2亿
select
    count(*)
from app_user_track_log
where event in (262,266,269,39,36,5,41);
--性能埋点表广告相关数据量 489450194 5亿
select
    count(*)
from app_performance_event_log
where event in (6,7,8,9);
--------------------------------------------------------------
--  广告观看埋点 最早日期 20240904
--  性能埋点    最早日期 20241017
--------------------------------------------------------------
select
    min(created_date)
from app_user_track_log
where event in (262,266,269,39,36,5) and created_date<='20250110';

select
    min(created_date)
from app_performance_event_log
where event in (6,7,8,9);

--------------------------------------------------------------
--  提取 阶梯广告观看
--------------------------------------------------------------
select
    event,
    event_name,
    GET_JSON_OBJECT(ext_body,'$.task')
from app_user_track_log
where to_timestamp(created_at) :: date  = '2025-01-20'
    and event = 41 and GET_JSON_OBJECT(ext_body,'$.task') = 'Watch Ads';
limit 20;

--------------------------------------------------------------
--  用户表数据量  880728688 8亿 每天一千万
--  一般不从这里面直接取值 从 dwd_user_info中取值
--------------------------------------------------------------

select
    count(*)
from "oversea-api_osd_user_daily"
where data_date = current_date+interval '-1 day'

--------------------------------------------------------------
--  订单数据量
--  未去重  7632139  7百万
--  去重   1349917  1百万
--------------------------------------------------------------
select
    count(*)
from (SELECT
        uid,
        to_timestamp(created_at)::date as d_date
    FROM all_order_log
    WHERE to_timestamp(created_at)::date >= '2024-07-01'
      AND environment = 1
      AND status = 1
    group by uid,to_timestamp(created_at)::date ) t;

-------------------------------------------------------------
-- 测试dws_traffic_user_ad_log_1d 数据量 81608902 81612397
-------------------------------------------------------------
select count(*)from dws_traffic_user_ad_log_1d
