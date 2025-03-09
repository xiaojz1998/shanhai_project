with t0 as (
    -- 对所有观看过的用户id形成一个列表
    select
    d_date,
    vid,
    array_agg(case when watch_pv>0 then uid else null end) over(partition by vid order by d_date range between unbounded preceding and current row ) as uid_array
from public.dws_behave_user_watch_charge_scheduled_video
), t0_add_null as (
    select
        d_date,
        vid,
        ARRAY_APPEND(uid_array,null) as uid_array
    from t0
),t1 as (
    -- 炸裂列表
    select
        d_date,
        vid,
        unnest (uid_array) tmp_uid
    from t0_add_null
), t2 as (
    -- 求导所有观看了的uid数量 -1是减去null
    select
        d_date,
        vid,
        array_length(array_agg( distinct tmp_uid),1)-1  as total_watch_uv
    from t1
    group by d_date,vid
), t3 as (
    --
    select
    tmp_t.d_date  as d_date     ,
    tmp_t.vid     as vid     ,
    uid                ,
    name               ,
    english_name       ,
    display_date       ,
    watch_pv           ,
    total_watch_uv     ,
    total_watch_uv_on_d_date,
    watch_duration_sec ,
    pay_pv             ,
    pay_amt
    from public.dws_behave_user_watch_charge_scheduled_video tmp_t
    join t2 on tmp_t.d_date = t2.d_date and tmp_t.vid = t2.vid
)
select
    d_date as "日期",
    vid as "剧id",
    name as "剧名",
    english_name as "剧译名",
    display_date as "上架日期",
    count(distinct uid) as "预约人数",
    sum(case when watch_pv>0 and watch_duration_sec>0 then 1 else 0 end) as "预约播放人数",
    total_watch_uv as "累计预约播放人数",
    case when sum(case when watch_pv>0 and watch_duration_sec>0 then 1 else 0 end)>0 then 1.0*sum(watch_duration_sec)/sum(case when watch_pv>0 and watch_duration_sec>0 then 1 else 0 end) else 0 end as "预约用户人均播放时长",
    sum(case when pay_amt>0 and pay_pv>0 then 1 else 0 end ) as "预约用户充值人数",
    sum(pay_pv) as "预约用户充值次数",
    sum(pay_amt) as "预约用户充值金额",
    case when count(distinct uid)>0 then 1.0*sum(case when pay_amt>0 and pay_pv>0 then 1 else 0 end )/count(distinct uid) else 0 end as "预约用户付费率",
    case when total_watch_uv_on_d_date>0 then 1.0*sum(case when watch_pv>0 and watch_duration_sec>0 then 1 else 0 end)/total_watch_uv_on_d_date else 0 end as "预约播放占比"
from t3
group by d_date,vid,name,english_name,display_date,total_watch_uv,total_watch_uv_on_d_date