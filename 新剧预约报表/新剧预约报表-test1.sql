-- 订阅表 uid数量 几百个
select
    uid,vid
from public."oversea-api_osd_video_scheduled_users"
group by uid,vid;
--埋点数据数量 4435747230
select
    count(*)
from app_user_track_log;
-- 总充值次数6755124
select
    count(*)
from all_order_log;
-- 测试一下充值类型 goods_id goods_sku_id null的情况为0
select
    order_type,
    goods_id,
    goods_sku_id
from all_order_log
where (order_type = 1 or order_type = 4 or order_type = 5) and goods_id=0 and goods_sku_id!=0
limit 500;
-- 测试一下goods 只有几百个
------------------------------------------------------------------
-- 需要的临时表
------------------------------------------------------------------
-- 找到uid对vid 不同日期充值的金额和次数 主键uid vid d_date
select
    uid,
    goods_id as vid,
    count(*) as pay_pv,
    sum(money) as pay_amt,
    to_timestamp(created_at)::date as d_date
from all_order_log
where (order_type = 1 or order_type = 4 or order_type = 5) and goods_id !=0
       and  status = 1 and environment = 1
group by uid,goods_id,to_timestamp(created_at)::date;
-- 用户预约剧集表
SELECT
    uid,
    vid
from  "oversea-api_osd_video_scheduled_users" t0
WHERE t0.status = '1';
-- 用户播放剧集表（参考wendy）
select
    to_timestamp(created_at)::date as d_date,
    uid,
    vid,
    count(case when event in(1,2,13,14) then 1 else null end) as watch_pv,
    sum(case when event=2 then watch_time else 0 end) as watch_duration_sec
from app_user_track_log
where event in (1,2,13,14) and vid>0 and eid>0
group by to_timestamp(created_at)::date,
         uid,
         vid;
--用户预约剧集表 补全剧集字段 这里只需要考虑上架日期，而不需要考虑预约时间
SELECT
    uid,
    vid,
    t1.name,
    t1.english_name,
    to_timestamp(t1.display_time)::date as display_date
from  "oversea-api_osd_video_scheduled_users" t0
      join "oversea-api_osd_videos" t1
on t0.vid = t1.id
WHERE t0.status = '1';
-- 新剧最早上架日期 '2025-01-30
select
    min(display_date)
from (SELECT
    uid,
    vid,
    t1.name,
    t1.english_name,
    to_timestamp(t1.display_time)::date as display_date
from  "oversea-api_osd_video_scheduled_users" t0
      join "oversea-api_osd_videos" t1
on t0.vid = t1.id
WHERE t0.status = '1') t2;
-- 生成日期
select generate_series(date '2025-01-30',current_date ,'1 day')::date d;
-- 补全数据版 新剧预约表
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
)select * from t2;
-- 补全播放和充值数据
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
)
select
    t2.d_date,
    t2.vid,
    t2.uid,
    t2.name,
    t2.english_name,
    t2.display_date,
    t3.watch_pv,
    t3.watch_duration_sec,
    t4.pay_pv,
    t4.pay_amt
from t2
    left join tmp_video_watch as t3
        on t2.uid=t3.uid and t2.vid = t3.vid and t2.d_date = t3.d_date
    left join tmp_video_charge as t4
        on t2.uid=t4.uid and t2.vid = t4.vid and t2.d_date = t4.d_date


