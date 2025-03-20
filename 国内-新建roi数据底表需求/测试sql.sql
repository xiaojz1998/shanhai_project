
-- 最小日期 ‘2024-12-17’
select
    min(enter_date)
from(select
      created_date as  enter_date,
      case when ad_channel is null then '自然流' else ad_channel end as ad_channel,
      case when os=1 then 'android' when os=2 then 'iOS' else '其他' end as os,
      device_id,
      link_id
    from dw.dwd_user_info
    where ad_channel !='归因缺失'
      and guiyin_date is not null
      and ad_channel='巨量(抖音)'
    group by
        created_date ,
        case when ad_channel is null then '自然流' else ad_channel end ,
        case when os=1 then 'android' when os=2 then 'iOS' else '其他' end ,
        device_id,
        link_id ) t