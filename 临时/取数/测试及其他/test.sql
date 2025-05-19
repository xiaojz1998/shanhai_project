-- 找campaign_name
-- 订单表无
select campaign_name,ad_channel,uid,money*0.01 as money ,to_timestamp(created_at) from all_order_log
         where ad_channel like '%duxing%';


-- 埋点表
select * from app_user_track_log
         where campaign_name like '%CPS-xianyu-APP-70144-ceshi-en_US-pHBTS-p1281043645572845568-p3868%' limit 50;

-- middle-campaign
select
    *
from middle_campaign
where campaign_name like  '%CPS%' limit 50

--
select
    campaign_name,campaign_id,ad_channel
from app_user_track_log
where campaign_name  in ('CPS-xianyu-APP-11344-ceshi-en_US-pHBTS-p1273659651055161344-p3868',
                         'CPS-xianyu-APP-89888-ceshi-en_US-pHBTS-p1280999038151561216-p3868',
                         'CPS-xianyu-APP-22560-ceshi-en_US-pHBTS-p1276260460972478464-p3868',
                         'CPS-xianyu-APP-70144-ceshi-en_US-pHBTS-p1281043645572845568-p3868')
group by campaign_name,campaign_id,ad_channel;