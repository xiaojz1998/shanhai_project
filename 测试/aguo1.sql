SELECT log.uid,
       log.vid,
       log.event_name
FROM (
    SELECT DISTINCT uid,
                      vid,
                      event_name
      FROM app_user_track_log
      WHERE event_name IN ('drama_cover_show', 'drama_cover_click', 'drama_watch')
        and created_date = '20250412'
    ) log
         inner join (SELECT uid, vid FROM dwd_user_info) info on 1=1
where log.uid = info.uid::bigint
  AND log.vid != info.vid::bigint;