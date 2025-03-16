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
--  广告页面填充成功        ads_load_succeed         6
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