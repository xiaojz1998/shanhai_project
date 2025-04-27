with tmp_t1 as (
    select
        campaign_name,ad_channel,uid,order_id
    from app_user_track_log
    where ad_channel = 'duxing_'
         and campaign_name  in ('CPS-xianyu-APP-11344-ceshi-en_US-pHBTS-p1273659651055161344-p3868',
                                'CPS-xianyu-APP-89888-ceshi-en_US-pHBTS-p1280999038151561216-p3868',
                                'CPS-xianyu-APP-22560-ceshi-en_US-pHBTS-p1276260460972478464-p3868',
                                'CPS-xianyu-APP-70144-ceshi-en_US-pHBTS-p1281043645572845568-p3868')
        and order_id is not null and order_id != ''
)
select tmp_t1.campaign_name,tmp_t1.ad_channel,tmp_t1.uid,all_order_log.money as money,to_timestamp(all_order_log.created_at)::date as created_date
from all_order_log join tmp_t1 on order_num = order_id and status = 1
group by tmp_t1.campaign_name,tmp_t1.ad_channel,tmp_t1.uid,order_id,all_order_log.money,to_timestamp(all_order_log.created_at)::date;

--
select
    media_source,
    campaign_id
from app_purchase_event_log
where media_source = 'duxing_'


