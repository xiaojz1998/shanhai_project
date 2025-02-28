-- 建表语句
drop table if exists public.dws_behave_user_watch_charge_scheduled_video;
create table if not exists public.dws_behave_user_watch_charge_scheduled_video
(
    d_date                          date,
    vid                             text,
    uid                             text,
    name                            text,
    english_name                    text,
    display_date                    date,
    watch_pv                        bigint,
    total_watch_uv_on_d_date        bigint,
    watch_duration_sec              bigint,
    pay_pv                          bigint,
    pay_amt                         numeric(20,2)
);


-- 每日注入
set timezone ='UTC-0';
truncate table public.dws_behave_user_watch_charge_scheduled_video;
insert into public.dws_behave_user_watch_charge_scheduled_video
with t0 as (
        SELECT
        uid,
        vid,
        t1.name,
        t1.english_name,
        to_timestamp(t1.display_time)::date as display_date
    from  "oversea-api_osd_video_scheduled_users" t0
          join "oversea-api_osd_videos" t1
    on t0.vid = t1.id
    WHERE t0.status = '1'
), t1 as (
    select generate_series(date '2025-01-30',current_date ,'1 day')::date d_date
), t2 as (
    select
        *
    from t0,t1
    where t0.display_date<= t1.d_date
), tmp_video_watch as (
    select
        to_timestamp(created_at)::date as d_date,
        uid,
        vid,
        count(case when event in(1,2,13,14) then 1 else null end) as watch_pv,
        sum(case when event=2 then watch_time else 0 end) as watch_duration_sec
    from app_user_track_log
    where event in (1,2,13,14) and vid>0 and eid>0
    group by to_timestamp(created_at)::date,uid,vid
), tmp_video_charge as (
        select
        uid,
        goods_id as vid,
        count(*) as pay_pv,
        sum(money) as pay_amt,
        to_timestamp(created_at)::date as d_date
    from all_order_log
    where (order_type = 1 or order_type = 4 or order_type = 5) and goods_id !=0
           and  status = 1 and environment = 1
    group by uid,goods_id,to_timestamp(created_at)::date
), tmp_video_watch_uv as  (
    select
        d_date,
        vid,
        count(distinct uid) total_watch_uv_on_d_date
    from tmp_video_watch
    group by d_date, vid
)
select
    t2.d_date,
    t2.vid,
    t2.uid,
    t2.name,
    t2.english_name,
    t2.display_date,
    t3.watch_pv,
    total_watch_uv_on_d_date,
    t3.watch_duration_sec,
    t4.pay_pv,
    t4.pay_amt
from t2
    left join tmp_video_watch as t3
        on t2.uid=t3.uid and t2.vid = t3.vid and t2.d_date = t3.d_date
    left join tmp_video_charge as t4
        on t2.uid=t4.uid and t2.vid = t4.vid and t2.d_date = t4.d_date
    left join tmp_video_watch_uv as t5
        on t2.d_date = t5.d_date and t2.vid = t5.vid;

