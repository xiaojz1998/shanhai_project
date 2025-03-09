-- 测试几个表的app_id
select
    count(distinct app_id)
from duanju_account_device

-- 测试ad_channel 是否出错

select
 t1.device_id
,t1.link_id
,t1.guiyin_date
,t2.ad_channel_id
,coalesce(t2.ad_channel,'归因缺失') as ad_channel
from(
	select * from(
	select
     device_id
    ,link_id -- 推广链接id
    ,to_date(created_at, 'YYYY-MM-DD HH24:MI:SS')::date AS guiyin_date
    ,row_number() over(partition by device_id order by created_at) as rn
    from mg_duanju_report_active_record
    )t where rn=1
)t1
left join(
	select distinct
     id as link_id
    ,kind as ad_channel_id
    ,case when kind=1 then '巨量(抖音)'
        when kind=2 then '快手'
        when kind=3 then '腾讯'
        when kind=4 then '百度'
        when kind=5 then '小米'
        when kind=6 then 'vivo'
        when kind=7 then 'oppo'
    	else '其他' end as ad_channel
	from duanju_promotion_links
)t2 on t1.link_id=t2.link_id