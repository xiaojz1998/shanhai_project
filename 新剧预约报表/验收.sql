-- 对方验收sql
select
     to_char(to_timestamp(created_at) +  interval '8 hours', 'YYYY-MM-DD') AS date,
     count(distinct uid) as 预约人数
from "oversea-api_osd_video_scheduled_users"    --预约剧的表
where status = 1             --状态为【已预约】
  and (to_timestamp(created_at) between timestamp '2025-02-01 00:00:00' + interval '8 hours'
                and timestamp '2025-03-31 23:59:59' + interval '8 hours')   --查从2月1号到3月31号的
  and vid = 2942         --限制剧2942
group by date
order by date


with scheduled as (
select
    uid
from "oversea-api_osd_video_scheduled_users"    --预约剧的表
where status = 1             --状态为【已预约】
  and (to_timestamp(created_at) between timestamp '2025-02-01 00:00:00'
                and timestamp '2025-03-31 23:59:59' )   --查从2月1号到3月31号的
  and vid = 2942      --限制剧2942
)
--观看
 SELECT
  to_char(to_timestamp(a.created_at) , 'YYYY-MM-DD') AS date,
  sum(case when event = 2 then watch_time end) as watch_time,          --预约总播放时间    event 2 是drama_watch_time
  count(distinct a.uid ) as 预约播放人数,
  sum(case when event = 2 then watch_time end)  / count(distinct a.uid ) as 人均播放时长
  FROM  "app_user_track_log"  a inner join scheduled b on a.uid = b.uid     --筛选出预约的人
  where (to_timestamp(created_at) between timestamp '2025-02-01 00:00:00'
                  and timestamp '2025-03-31 23:59:59' )   --查从2月1号到3月31号的    --查从2月1号到3月31号的
     and event in (1,2,13,14) and eid>0
     and vid = 2942
 group by date
 order by date

-- 统计2942剧的观看人数
select
    d_date,
    vid,
    count(distinct t1.uid) as watch_num
from (select
    to_timestamp(created_at) :: date as d_date,
    uid,
    vid
from app_user_track_log
where event in (1,2,13,14) and vid=2942 and eid>0 and created_date>='20250130'
group by to_timestamp(created_at) :: date , uid ,vid ) t1
join (
    select distinct uid from "oversea-api_osd_video_scheduled_users" where status = '1' and vid = 2942
) t2 on t1.uid = t2.uid
group by d_date,vid


-- 累计预约播放人数 调整
