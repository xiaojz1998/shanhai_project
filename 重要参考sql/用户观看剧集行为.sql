SELECT
  to_timestamp(a.created_at)::date AS d_date,
  a.country_code,
  a.uid,
  count(DISTINCT a.vid) AS vid_watch_cnt -- 每人看短剧数
,
  count(DISTINCT a.eid) AS eid_watch_cnt -- 每人看剧集数
,
  count(
    DISTINCT CASE
      WHEN e.sort >= c.pay_num THEN a.eid
      ELSE NULL
    END
  ) AS eidpay_watch_cnt,
  count(
    DISTINCT CASE
      WHEN e.sort < c.pay_num THEN a.eid
      ELSE NULL
    END
  ) AS eidfree_watch_cnt,
  sum(
    CASE
      WHEN a.event = 2 THEN watch_time
      ELSE 0
    END
  ) AS watch_duration -- "看剧时长(分钟)"
FROM
  public.app_user_track_log a
  LEFT JOIN "oversea-api_osd_videos" c ON a.vid = c.id
  LEFT JOIN "oversea-api_osd_video_episodes" e ON a.eid = e.id
WHERE
  1 = 1
  AND a.event IN (1, 2, 13, 14)
  AND a.vid > 0
  AND a.eid > 0
  -- and a.watch_time >3
  AND to_timestamp(a.created_at)::date >= '2024-11-01'
GROUP BY
  to_timestamp(a.created_at)::date,
  a.country_code,
  a.uid