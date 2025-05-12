------------------------------------------
-- file: 验证.sql
-- author: xiaoj
-- time: 2025/5/9 10:05
-- description:
------------------------------------------
-- 测试广告收入
 select
     *
 from (select
        created_date :: date 日期,
        round(sum(adin_amt),2) 广告收入
    from dwd_adin_media_revenue
    group by created_date :: date) t
 where 日期 = '2025-05-08' or 日期 = '2025-05-07';


-- 验收sql
select
    lang_name ,
    sum(event_revenue)*0.01 event_revenue
from
(select uid,
        sum(event_revenue_usd)event_revenue
 from dwd_app_purchase_event_log
 where created_date::date = '2025-05-08'
   and conversion_type in ('install','unknown')
   and event_name in('af_purchase','Purchase')
   and install_date=created_date
 group by uid
 )a
left join dwd_user_info b on a.uid::text=b.uid
group by lang_name
