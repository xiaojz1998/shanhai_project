------------------------------------------
-- file: abtest流量池_剧维度.新sql
-- author: xiaoj
-- time: 2025/4/24 15:07
-- description:
------------------------------------------
set timezone = 'UTC-0';
-- 第一步：剧维度不过滤注册语言的用户且区分对照组
drop table if exists public.tmp_tb_fliter_user_abtest_vid;
create table public.tmp_tb_fliter_user_abtest_vid as
SELECT d_date      AS                                        register_date,
       a.uid::int8 AS                                        uid,
       country_name,
       area,
       ad_channel,
       -- a.lang,
       case when length(vid) = 0 then '-999999' else vid end vid,
       CASE
           WHEN RIGHT(CAST(a.uid AS VARCHAR), 3)::int >= 0 and
                RIGHT(CAST(a.uid AS VARCHAR), 3)::int <= 599 THEN '实验组'
           WHEN RIGHT(CAST(a.uid AS VARCHAR), 3)::int >= 600 and
                RIGHT(CAST(a.uid AS VARCHAR), 3)::int <= 999 THEN '对照组'
           ELSE NULL
           END     AS                                        user_group,
       u.created_at                                          register_time
FROM public.dwd_user_info a
         --  left join v_dim_ad_campaign_info b on a.campaign_id = b.campaign_id
         left join (select *
                    from (select uid,
                                 created_at,
                                 row_number() over (partition by uid order by created_at) rn
                          from public.user_log
                          where event = 1
                            and to_timestamp(created_at)::date >= '2024-07-01') t
                    where rn = 1) u on a.uid::bigint = u.uid;

-- 圈出每天曝光第一次的用户行为判断是否老用户
drop table if exists public.tmp_first_ifold_exposure_vid;
create table public.tmp_first_ifold_exposure_vid as
select a.*
from (select *
      from (select uid,
                   created_at,
                   to_timestamp(a.created_at)::date                                                          p_date,
                   row_number() over (partition by uid,to_timestamp(a.created_at)::date order by created_at) rn
            from public.app_user_cover_show_log a
            where to_timestamp(a.created_at)::date >= '2025-03-24'
              and to_timestamp(a.created_at)::date < current_date
              and event_name = 'drama_cover_show') t
      where rn = 1) a
         inner join public.tmp_tb_fliter_user_abtest_vid b on a.uid = b.uid
-- and a.vid::varchar!=b.vid
where extract(epoch from (to_timestamp(a.created_at) - to_timestamp(register_time))) /
      3600 > 24;


-- 第二步：曝光行为指定推荐位且指定印尼语的剧每天打上是否第一次标签
drop table if exists public.tmp_tb_fliter_exposure_abtest_vid;
create table public.tmp_tb_fliter_exposure_abtest_vid as
select *
from (select uid,
             a.created_at,
             get_json_object(ext_body, '$.page') page,
             model_id,
             alg_id,
             vid,
             category_id,
             to_timestamp(a.created_at)::date    p_date
      --       ,row_number() over (partition by uid,to_timestamp(a.created_at)::date  order by a.created_at) rn
      from public.app_user_cover_show_log a
               left join "oversea-api_osd_recommend" b on a.model_id = b.id::varchar
               left join "oversea-api_osd_videos" v on a.vid = v.id
      --   where to_timestamp(a.created_at)::date >= (current_date + interval '-3 day')
      where to_timestamp(a.created_at)::date >= '2025-03-24'
        and to_timestamp(a.created_at)::date < current_date
        and event_name = 'drama_cover_show'
        and category_id = 31) t
where (page = '3' and model_id in ('1', '2', '3'))
   or (page = '9' and model_id = '0')
   or (page = '1' and alg_id > 0);


-- 圈出每天埋点第一次的用户行为判断是否老用户

drop table if exists public.tmp_first_ifold_track_vid;
create table public.tmp_first_ifold_track_vid as
select a.*
from (select *
      from (select uid,
                   created_at,
                   to_timestamp(a.created_at)::date                                                          p_date,
                   row_number() over (partition by uid,to_timestamp(a.created_at)::date order by created_at) rn
            from public.app_user_track_log a
            where to_timestamp(a.created_at)::date >= '2025-03-24'
              and to_timestamp(a.created_at)::date < current_date) t
      where rn = 1) a
         inner join public.tmp_tb_fliter_user_abtest_vid b on a.uid = b.uid
-- and a.vid::varchar!=b.vid
where extract(epoch from (to_timestamp(a.created_at) - to_timestamp(register_time))) /
      3600 > 24;


-- 第三步：埋点行为指定推荐位且每天打上是否第一次标签
drop table if exists public.tmp_tb_fliter_track_abtest_vid;
create table public.tmp_tb_fliter_track_abtest_vid as

select *
from (select uid,
             a.created_at,
             get_json_object(ext_body, '$.page') page,
             column1                             model_id,
             alg_id,
             vid,
             eid,
             event,
             order_id,
             watch_time,
             to_timestamp(a.created_at)::date    p_date,
             category_id
      --       ,row_number() over (partition by uid,to_timestamp(a.created_at)::date order by a.created_at) rn
      from public.app_user_track_log a
               left join "oversea-api_osd_recommend" b on a.column1 = b.id::varchar
               left join "oversea-api_osd_videos" v on a.vid = v.id
      --    where to_timestamp(a.created_at)::date >= (current_date + interval '-3 day')
      where to_timestamp(a.created_at)::date >= '2025-03-24'
        and to_timestamp(a.created_at)::date < current_date
        --  and get_json_object(ext_body, '$.page') in ('3', '9', '1')
        and category_id = 31) t
where (page = '3' and model_id in ('1', '2', '3'))
   or (page = '9' and model_id = '0')
   or (page = '1' and alg_id > 0);


-- 曝光圈定老用户的时间
drop table if exists public.tmp_tb_fliter_old_exposure_abtest_vid;
create table public.tmp_tb_fliter_old_exposure_abtest_vid as
select a.*,
       b.country_name,
       b.area,
       b.ad_channel,
       -- b.lang,
       b.vid bvid,
       b.user_group,
       b.register_time
from (select a.*
      from public.tmp_tb_fliter_exposure_abtest_vid a
               inner join
           public.tmp_first_ifold_exposure_vid b on a.p_date = b.p_date and a.uid = b.uid) a
         inner join public.tmp_tb_fliter_user_abtest_vid b on a.uid = b.uid;
-- and a.vid::varchar!=b.vid
-- where extract(epoch from (to_timestamp(a.created_at) - to_timestamp(register_time))) /
--       3600 > 24;
-- 埋点圈定老用户的时间
drop table if exists public.tmp_tb_fliter_old_track_abtest_vid;
create table public.tmp_tb_fliter_old_track_abtest_vid as
select a.*,
       b.country_name,
       b.area,
       b.ad_channel,
       -- b.lang,
       b.vid bvid,
       b.user_group,
       b.register_time
from (select a.*
      from public.tmp_tb_fliter_track_abtest_vid a
               inner join
           public.tmp_first_ifold_track_vid b on a.p_date = b.p_date and a.uid = b.uid) a
         inner join public.tmp_tb_fliter_user_abtest_vid b on a.uid = b.uid;
-- and a.vid::varchar!=b.vid
-- where extract(epoch from (to_timestamp(a.created_at) - to_timestamp(register_time))) / 3600 >
--       24;
-- 曝光只统计老用户且剔除投放归因剧数据
drop table if exists public.tmp_tb_target_exposure_abtest_vid;
create table public.tmp_tb_target_exposure_abtest_vid as
select *
from (select a.*,
             b.country_name,
             b.area,
             b.ad_channel,
             --       b.lang,
             b.vid                                                                          bvid,
             b.user_group,
             b.register_time,
             case when a.vid::varchar != b.vid or b.uid is null then 'diff' else 'same' end gy_flag
      from public.tmp_tb_fliter_exposure_abtest_vid a
               inner join (select uid,p_date from public.tmp_tb_fliter_old_exposure_abtest_vid group by uid,p_date) o
                          on a.uid = o.uid and a.p_date = o.p_date
                        --   and a.created_at >= o.created_at
               left join public.tmp_tb_fliter_user_abtest_vid b
                         on a.uid = b.uid) t
where gy_flag = 'diff';
-- 埋点只统计老用户且剔除投放归因剧数据
drop table if exists public.tmp_tb_target_track_abtest_vid;
create table public.tmp_tb_target_track_abtest_vid as
select *
from (select a.*,
             b.country_name,
             b.area,
             b.ad_channel,
             --       b.lang,
             b.vid                                                                          bvid,
             b.user_group,
             b.register_time,
             case when a.vid::varchar != b.vid or b.uid is null then 'diff' else 'same' end gy_flag
      from public.tmp_tb_fliter_track_abtest_vid a
               inner join (select uid,p_date from  public.tmp_tb_fliter_old_track_abtest_vid group by uid,p_date) o
                          on a.uid = o.uid and a.p_date = o.p_date
                        --   and a.created_at >= o.created_at
               inner join public.tmp_tb_fliter_user_abtest_vid b
                          on a.uid = b.uid

      where a.event::int IN (112, 1, 192, 191, 2, 13, 14)) t
where gy_flag = 'diff';

drop table if exists public.tmp_aggregated_exposure_abtest_vid;
create table public.tmp_aggregated_exposure_abtest_vid AS
select to_timestamp(a.created_at)::date p_date,
       user_group,
       area,
       country_name,
       ad_channel,
       category_id                      lang,
       uid,
       vid,
       -- count(DISTINCT uid) as           exposure_users, --曝光人数
       count(1) as                      exposure_times --曝光次数
from public.tmp_tb_target_exposure_abtest_vid a
group by to_timestamp(a.created_at)::date,
         user_group,
         area,
         country_name,
         ad_channel,
         category_id,
         vid,
         uid;
drop table if exists public.tmp_successful_recharges_abtest_vid;
create table public.tmp_successful_recharges_abtest_vid AS
SELECT a.uid,
       to_timestamp(created_at) :: date p_date,
       order_num,
       1       AS                       successful_recharge_times,--充值成功次数
       a.money AS                       total_recharge_amount--充值金额
from public.all_order_log a
--   WHERE to_timestamp(created_at) :: date >= (current_date + interval '-15 day')
where to_timestamp(a.created_at)::date >= '2025-03-24'
  and to_timestamp(created_at) :: date < current_date
  AND status = 1
  AND environment = 1;
-- 上报数据会重复，造成订单量重复，做个去重处理
drop table if exists public.tmp_order_data_distinct_abtest_vid;
create table public.tmp_order_data_distinct_abtest_vid as
select p_date,
       user_group,
       area,
       country_name,
       ad_channel,
       lang,
       uid,
       vid,
       -- count(distinct uid)            pay_uv,
       sum(successful_recharge_times) successful_recharge_times,
       sum(total_recharge_amount)     total_recharge_amount
from (select t1.p_date,
             user_group,
             area,
             country_name,
             ad_channel,
             category_id                 lang,
             successful_recharge_times,
             total_recharge_amount,
             t1.uid,
             vid,
             ROW_NUMBER() OVER (PARTITION BY t1.p_date,t1.uid,order_id
                 ORDER BY created_at) AS rn
      from public.tmp_tb_target_track_abtest_vid t1
               inner join public.tmp_successful_recharges_abtest_vid t2
                          on t1.uid = t2.uid and t1.p_date = t2.p_date
                              --    AND CONCAT('SH', t1.order_id) = t2.order_num
                              AND if(POSITION('SH' IN t1.order_id) = 0,
                                     CONCAT('SH', t1.order_id), t1.order_id) = t2.order_num
      where event = 192) t
where rn = 1
group by p_date, user_group,
         area,
         country_name,
         ad_channel,
         lang,
         uid, vid;
drop table if exists public.tmp_aggregated_click_abtest_vid;
create table public.tmp_aggregated_click_abtest_vid AS
SELECT p_date,
       user_group,
       area,
       country_name,
       ad_channel,
       category_id                                                                                               lang,
       vid,

       COUNT(DISTINCT CASE WHEN event = 112 THEN uid END)                                                     AS click_users,
       COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END)                                         AS play_users,
       COUNT(DISTINCT CASE WHEN event = 192 THEN uid END)                                                     AS recharge_submission_users,
       COUNT(DISTINCT CASE WHEN event = 191 THEN uid END)                                                     AS episode_unlock_users,
       COUNT(CASE WHEN event = 112 THEN uid END)                                                              AS click_times,
       COUNT(CASE WHEN event IN (1, 2, 13, 14) THEN uid END)                                                  AS play_times,
       -- COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_uv,
       COUNT(CASE WHEN event = 192 THEN uid END)                                                              AS recharge_submission_times,
       COUNT(CASE WHEN event = 191 THEN concat(vid, eid) END)                                                 AS episode_unlocks,-- 解锁剧集数
       -- COUNT(distinct CASE WHEN event = 191 THEN uid END) AS episode_unlocks_uv,
       cast(ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) as numeric(30, 8)) AS watch_duration_minutes
FROM public.tmp_tb_target_track_abtest_vid
GROUP BY p_date,
         user_group,
         area,
         country_name,
         ad_channel,
         category_id,
         vid;
drop table if exists public.tmp_aggregated_exposure_behave_abtest_vid;
create table public.tmp_aggregated_exposure_behave_abtest_vid as
select p_date,
       user_group,
       area,
       country_name,
       ad_channel,
       category_id                                        lang,
       uid,
       vid,
       CASE
           WHEN event IN (1, 2, 13, 14)
               THEN concat(vid, eid) END               AS play_eid,--播放集数
       CASE WHEN event = 191 THEN concat(vid, eid) END AS episode_unlocks,-- 解锁剧集数
       CASE WHEN event = 2 THEN watch_time ELSE 0 END  AS watch_duration_minutes
--        cast(ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) as numeric(30, 8)) AS watch_duration_minutes
FROM public.tmp_tb_target_track_abtest_vid;

-- SELECT * FROM public.tmp_tb_target_track_abtest_vid


drop table if exists public.tmp_abtest_dim_vid;
create table public.tmp_abtest_dim_vid as
select p_date,
       user_group,
       area,
       country_name,
       ad_channel,
       vid::varchar(64) vid,
       category_id      lang
from public.tmp_tb_target_exposure_abtest_vid
group by p_date,
         user_group,
         area,
         country_name,
         ad_channel,
         vid,
         category_id
union
select p_date,
       user_group,
       area,
       country_name,
       ad_channel,
       vid::varchar(64),
       category_id lang
from public.tmp_tb_target_track_abtest_vid
group by p_date,
         user_group,
         area,
         country_name,
         ad_channel,
         vid,
         category_id
union
select p_date,
       user_group,
       area,
       country_name,
       ad_channel,
       vid::varchar(64),
       lang
from public.tmp_order_data_distinct_abtest_vid
group by p_date,
         user_group,
         area,
         country_name,
         ad_channel,
         vid,
         lang
union
select p_date,
       user_group,
       area,
       country_name,
       ad_channel,
       vid::varchar(64),
       lang
from public.tmp_aggregated_exposure_abtest_vid
group by p_date,
         user_group,
         area,
         country_name,
         ad_channel,
         vid,
         lang;

--剧维度
truncate table public.dwd_abtest_pool_vid_di;
insert into public.dwd_abtest_pool_vid_di
SELECT di.p_date,--日期

       di.area,
       di.country_name,
       di.ad_channel,
       case
           when di.lang = '13' then '日语'
           when di.lang = '14' then '韩语'
           when di.lang = '20' then '英语'
           when di.lang = '22' then '西班牙语'
           when di.lang = '31' then '印尼语'
           when di.lang = '34' then '中文简体'
           when di.lang = '35' then '法语'
           when di.lang = '36' then '葡萄牙语'
           when di.lang = '37' then '德语'
           when di.lang = '38' then '泰语'
           when di.lang = '39' then '繁体中文'
           when di.lang = '40' then '阿拉伯语'
           when di.lang = '41' then '越南语'
           when di.lang = '42' then '土耳其语'
           end                                      lang,
       di.user_group,--实验分组
       di.vid,
       coalesce(exposure_users, 0)                  exposure_users, --曝光人数
       coalesce(ac.click_users, 0)                  click_users,--点击人数
       COALESCE(ac.play_users, 0)                AS play_users,--播放人数
       COALESCE(ac.recharge_submission_users, 0) AS recharge_submission_users,--提交充值人数
       coalesce(ap.pay_uv, 0)                       pay_uv,--支付成功人数
       COALESCE(ac.episode_unlock_users, 0)      AS episode_unlock_users,--解锁剧集人数
       coalesce(ap.total_recharge_amount, 0)        total_recharge_amount,--总支付金额

       coalesce(exposure_times, 0)                  exposure_times, --曝光次数
       COALESCE(ac.click_times, 0)               AS click_times,--点击次数
       COALESCE(ac.recharge_submission_times, 0) AS recharge_submission_times,--提交充值次数
       COALESCE(ap.successful_recharge_times, 0) AS successful_recharge_times,--支付成功次数

       coalesce(ac.episode_unlocks, 0)              episode_unlocks,--解锁集数
       COALESCE(ac.watch_duration_minutes, 0)    AS watch_duration_minutes,--观看时长_分钟


       coalesce(play_eid, 0)                        exposure_play_eid,--曝光用户观看集数
       coalesce(ei.episode_unlocks, 0)              exposure_episode_unlocks,--曝光用户解锁集数
       coalesce(ei.watch_duration_minutes, 0)       exposure_watch_duration_minutes,--曝光用户播放时长
       coalesce(ei.total_recharge_amount, 0)        exposure_total_recharge_amount--曝光用户充值arpu

FROM public.tmp_abtest_dim_vid di
         left join (select p_date,
                           user_group,
                           vid,
                           area,
                           country_name,
                           ad_channel,
                           category_id            lang,
                           count(DISTINCT uid) as exposure_users, --曝光人数
                           count(1)            as exposure_times  --曝光次数
                    from public.tmp_tb_target_exposure_abtest_vid a
                    group by p_date,
                             user_group,
                             area,
                             country_name,
                             ad_channel,
                             category_id, vid) ae
                   ON di.p_date = ae.p_date
                       AND di.user_group = ae.user_group
                       AND di.area = ae.area
                       AND di.country_name = ae.country_name
                       AND di.ad_channel = ae.ad_channel
                       AND di.lang = ae.lang
                       and di.vid::varchar = ae.vid ::varchar
         LEFT JOIN (SELECT p_date,
                           user_group,
                           vid,
                           area,
                           country_name,
                           ad_channel,
                           category_id                                                                                               lang,


                           COUNT(DISTINCT CASE WHEN event = 112 THEN uid END)                                                     AS click_users,
                           COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END)                                         AS play_users,
                           COUNT(DISTINCT CASE WHEN event = 192 THEN uid END)                                                     AS recharge_submission_users,
                           COUNT(DISTINCT CASE WHEN event = 191 THEN uid END)                                                     AS episode_unlock_users,
                           COUNT(CASE WHEN event = 112 THEN uid END)                                                              AS click_times,
                           COUNT(CASE WHEN event IN (1, 2, 13, 14) THEN uid END)                                                  AS play_times,
                           -- COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_uv,
                           COUNT(CASE WHEN event = 192 THEN uid END)                                                              AS recharge_submission_times,
                           COUNT(CASE WHEN event = 191 THEN concat(vid, eid) END)                                                 AS episode_unlocks,-- 解锁剧集数
                           -- COUNT(distinct CASE WHEN event = 191 THEN uid END) AS episode_unlocks_uv,
                           cast(ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) as numeric(30, 8)) AS watch_duration_minutes
                    FROM public.tmp_tb_target_track_abtest_vid
                    GROUP BY p_date,
                             user_group,
                             vid,
                             area,
                             country_name,
                             ad_channel,
                             category_id) ac ON di.p_date = ac.p_date
    AND di.user_group = ac.user_group
    AND di.area = ac.area
    AND di.country_name = ac.country_name
    AND di.ad_channel = ac.ad_channel
    AND di.lang = ac.lang
    and di.vid::varchar = ac.vid::varchar
         LEFT JOIN (select p_date,
                           user_group,
                           vid,
                           area,
                           country_name,
                           ad_channel,
                           lang,

                           count(distinct uid)            pay_uv,
                           sum(successful_recharge_times) successful_recharge_times,
                           sum(total_recharge_amount)     total_recharge_amount
                    from public.tmp_order_data_distinct_abtest_vid
                    group by p_date,
                             user_group,
                             vid,
                             area,
                             country_name,
                             ad_channel,
                             lang) ap ON di.p_date = ap.p_date
    AND di.user_group = ap.user_group
    AND di.area = ap.area
    AND di.country_name = ap.country_name
    AND di.ad_channel = ap.ad_channel
    AND di.lang = ap.lang and di.vid::varchar = ap.vid::varchar

         left join (select p_date,
                           user_group,
                           vid,
                           area,
                           country_name,
                           ad_channel,
                           lang,
                           sum(play_eid)               play_eid,
                           sum(episode_unlocks)        episode_unlocks,
                           sum(watch_duration_minutes) watch_duration_minutes,
                           sum(total_recharge_amount)  total_recharge_amount
                    from (select a.p_date,
                                 a.user_group,
                                 a.area,
                                 a.country_name,
                                 a.ad_channel,
                                 a.lang,
                                 a.vid,
                                 coalesce(play_eid, 0)               play_eid,
                                 coalesce(episode_unlocks, 0)        episode_unlocks,
                                 coalesce(watch_duration_minutes, 0) watch_duration_minutes,
                                 coalesce(total_recharge_amount)     total_recharge_amount
                          from public.tmp_aggregated_exposure_abtest_vid a
                                   left join
                               (SELECT uid,
                                       p_date,
                                       user_group,
                                       area,
                                       country_name,
                                       ad_channel,
                                       lang,
                                       vid,
                                       COUNT(DISTINCT play_eid)                                                          play_eid,
                                       COUNT(DISTINCT episode_unlocks)                                                   episode_unlocks,
                                       cast(ROUND(COALESCE(SUM(watch_duration_minutes) / 60.0, 0)) as numeric(30, 8)) AS watch_duration_minutes
                                FROM public.tmp_aggregated_exposure_behave_abtest_vid
                                group by uid, p_date, user_group, area, country_name, ad_channel, lang, vid) b
                               on a.uid = b.uid and a.p_date = b.p_date
                                   and a.user_group = b.user_group and a.area = b.area and
                                  a.country_name = b.country_name
                                   and a.ad_channel = b.ad_channel and a.lang = b.lang and a.vid::varchar = b.vid::varchar
                                   left join public.tmp_order_data_distinct_abtest_vid o
                                             on a.uid = o.uid and a.p_date = o.p_date
                                                 and a.user_group = o.user_group and a.area = o.area and
                                                a.country_name = o.country_name
                                                 and a.ad_channel = o.ad_channel and a.lang = o.lang and
                                                a.vid::varchar = o.vid::varchar) t
                    group by p_date, user_group, vid, area, country_name, ad_channel, lang) ei ON di.p_date = ei.p_date
    AND di.user_group = ei.user_group
    AND di.area = ei.area
    AND di.country_name = ei.country_name
    AND di.ad_channel = ei.ad_channel
    AND di.lang = ei.lang
    and di.vid::varchar = ei.vid::varchar
where di.vid != '0';


