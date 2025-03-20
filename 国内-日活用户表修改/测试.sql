select *
FROM dw.dwd_user_active t1
where (act_app_version is null or act_app_version ='')
and act_date='2025-03-11' and ad_channel='巨量(抖音)' and os=2
select *
from public.user_track_log
WHERE event_name in ('start_app','stay_app','enter_talo','show_cover', 'click_cover', 'enter_player','play_draama','stay_drama')
and created_date ='2025-03-11'
and device_id ='6016E08F-A04C-465E-99C1-573637461834'