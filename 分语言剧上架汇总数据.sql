---------------------------------------------------------------------
-- 测试语句
---------------------------------------------------------------------
select
    *
from (select
 	t.d_date d_date,t.vid vid,t.name "name",t.english_name english_name,t.lang lang,
    t.vid_source vid_source,t.vid_type vid_type,t.complex complex,t.category "category",
    t.is_pay is_pay,t.display_status display_status,t.pay_num pay_num,
    t.drama_duration drama_duration,t.display_date display_date,
    date(date_trunc('month',d_date)) month_begin,
    --
    sum(t.ad_cost) ad_cost_sum,
    case when min(t.current_diff)>=0 and sum(t.ad_cost)>0 then sum(coalesce(t.pay_0,0))/sum(t.ad_cost)  else null end as roi_0,
    case when sum(t.watch_uv)>0 then 1.0*sum(t.pay_uv)/sum(t.watch_uv) else null end as payment_rate
    --
from public.dw_video_overview t
group by t.d_date,t.vid,t.name,t.english_name,t.lang,
         t.vid_source,t.vid_type,t.complex,t.category,
         t.is_pay,t.display_status,t.pay_num,
         t.drama_duration,t.display_date ) t0
where roi_0 is not null
order by roi_0 desc;

-- 测试 roi 和 payment_rate公式
select
    *
from (select
 	t.d_date,t.vid,t.lang,t.display_date,
    date(date_trunc('month',d_date)) month_begin,
    --
    sum(t.ad_cost) ad_cost_sum,
    case when min(t.current_diff)>=0 and sum(t.ad_cost)>0 then sum(coalesce(t.pay_0,0))/sum(t.ad_cost)  else null end as roi_0,
    case when sum(t.watch_uv)>0 then 1.0*sum(t.pay_uv)/sum(t.watch_uv) else null end as payment_rate
    --
from public.dw_video_overview t
group by t.d_date,t.vid,t.lang,t.display_date ) t0
where roi_0 is not null
order by roi_0 desc;

-- 数量测试
select
    d_date,
    count(distinct  vid) num
from public.dw_video_overview
where extract(month from d_date) = 2 and display_date <= d_date and display_date >=date_trunc('month',d_date)::date
group by d_date;

select
    count(distinct id)
from "oversea-api_osd_videos"
where   to_timestamp(display_time)::date>='2025-02-01' and to_timestamp(display_time)::date<='2025-02-19' and display_status=1

select
    count(distinct id)
from "oversea-api_osd_videos"
where to_timestamp(display_time)::date>='2025-02-01' and to_timestamp(display_time)::date<='2025-02-15'
     and display_status=1 and category_id = 20 ;



select count(*) from "oversea-api_osd_videos"; -- 剧总数3619

---------------------------------------------------------------------
-- hologres建表语句 暂时先不用
---------------------------------------------------------------------
drop table if exists  tmp.dw_video_language_tmp01 ;
CREATE TABLE IF NOT EXISTS tmp.dw_video_language_tmp01 (
    d_date date NOT NULL,
    lang text,
    total_display_video_num integer,
    total_test_video_num integer,
    total_roi_0_video_num integer,
    total_payment_rate_video_num integer,
    PRIMARY KEY ( d_date, lang)
);
---------------------------------------------------------------------
-- 求值语句1.0
---------------------------------------------------------------------
select
    d_date,
    lang,
    --
    sum(date_flag) total_display_video_num,
    sum(test_flag & date_flag)total_test_video_num,
    sum(roi_0_flag & date_flag) total_roi_video_num,
    sum(payment_rate_flag & date_flag) total_payment_rate_video_num
    --
from (select
    *,
    --
    case when display_date <= d_date and display_date >= month_begin then 1 else 0 end date_flag,
    case when roi_0 is not null and roi_0>0.4 then 1 else 0 end roi_0_flag,
    case when payment_rate is not null and payment_rate >0.02 then 1 else 0 end payment_rate_flag,
    case when ad_cost_sum > 0 then 1 else 0 end test_flag
    --
from (
    select
 	t.d_date d_date,t.vid vid,t.name "name",t.english_name english_name,t.lang lang,
    t.vid_source vid_source,t.vid_type vid_type,t.complex complex,t.category "category",
    t.is_pay is_pay,t.country_name country_name,t.display_status display_status,t.pay_num pay_num,
    t.drama_duration drama_duration,t.display_date display_date,
    date(date_trunc('month',d_date)) month_begin,
    --
    sum(t.ad_cost) ad_cost_sum,
    case when min(t.current_diff)>=0 and sum(t.ad_cost)>0 then sum(coalesce(t.pay_0,0))/sum(t.ad_cost)  else null end as roi_0,
    case when sum(t.watch_uv)>0 then 1.0*sum(t.pay_uv)/sum(t.watch_uv) else null end as payment_rate
    --
from public.dw_video_overview t
group by t.d_date,t.vid,t.name,t.english_name,t.lang,
         t.vid_source,t.vid_type,t.complex,t.category,
         t.is_pay,t.country_name,t.display_status,t.pay_num,
         t.drama_duration,t.display_date ) t0 ) t1
group by lang , d_date;
---------------------------------------------------------------------
-- 求值语句2.0
---------------------------------------------------------------------
select
    d_date,
    lang,
    count(distinct vid) total_display_video_num,
    sum(case when ad_cost_sum_until_d_date >0 then 1 else 0 end) as total_test_video_num ,
    sum(case when roi_0_until_date > 0.4 then 1 else 0 end ) as total_roi_0_video_num,
    sum(case when  payment_rate_until_date > 0.02 then 1 else 0 end ) as total_payment_rate_video_num
from (select
    *,
    case when watch_uv_sum_until_d_date > 0 then 1.0*pay_uv_sum_until_d_date/watch_uv_sum_until_d_date else 0 end as payment_rate_until_date,
    case when ad_cost_sum_until_d_date > 0 then 1.0*pay_0_sum_until_d_date/ad_cost_sum_until_d_date else 0 end as roi_0_until_date
    --
    --sum(date_flag) total_display_video_num,
    --sum(test_flag & date_flag)total_test_video_num,
    --sum(roi_0_flag & date_flag) total_roi_video_num,
    --sum(payment_rate_flag & date_flag) total_payment_rate_video_num
    --
from (select
    *,
    sum(pay_0_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as pay_0_sum_until_d_date,
    sum(ad_cost_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as ad_cost_sum_until_d_date,
    sum(pay_uv_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as pay_uv_sum_until_d_date,
    sum(watch_uv_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as watch_uv_sum_until_d_date
    --
    --case when display_date <= d_date and display_date >= month_begin then 1 else 0 end date_flag
    --case when ad_cost_sum > 0 then 1 else 0 end test_flag,
    --case when roi_0 is not null and roi_0>0.4 then 1 else 0 end roi_0_flag,
    --case when payment_rate is not null and payment_rate >0.02 then 1 else 0 end payment_rate_flag
    --
from (
    select
 	t.d_date,t.vid,t.lang,t.display_date,
    date(date_trunc('month',d_date)) month_begin,
    --
    case when min(t.current_diff)>=0 then sum(coalesce(t.pay_0,0)) else 0 end as pay_0_sum,
    case when min(t.current_diff)>=0 then sum(coalesce(t.ad_cost,0))  else 0 end as ad_cost_sum,
    1.0*sum(coalesce(t.pay_uv,0)) pay_uv_sum,
    sum(coalesce(t.watch_uv,0)) watch_uv_sum
    --case when min(t.current_diff)>=0 and sum(t.ad_cost)>0 then sum(coalesce(t.pay_0,0))/sum(t.ad_cost)  else null end as roi_0,
    --case when sum(t.watch_uv)>0 then 1.0*sum(t.pay_uv)/sum(t.watch_uv) else null end as payment_rate
    --
from public.dw_video_overview t
group by t.d_date,t.vid,t.lang,t.display_date ) t0
where display_date <= d_date and display_date >= month_begin ) t1 ) t2
group by lang , d_date;
---------------------------------------------------------------------
-- 求值语句3.0
---------------------------------------------------------------------
select
    d_date as "数据日期",
    lang as "剧语言",
    count(distinct vid)  as "当月上架剧数量",
    sum(case when ad_cost_sum_until_d_date >0 then 1 else 0 end)  as "剧测试数量",
    sum(case when roi_0_until_date > 0.4 then 1 else 0 end ) as "首日ROI大于40%剧数量",
    sum(case when  payment_rate_until_date > 0.02 then 1 else 0 end )  as "付费率大于2%剧数量"
from (select
    *,
    case when watch_uv_sum_until_d_date > 0 then 1.0*pay_uv_sum_until_d_date/watch_uv_sum_until_d_date else 0 end as payment_rate_until_date,
    case when ad_cost_sum_until_d_date > 0 then 1.0*pay_0_sum_until_d_date/ad_cost_sum_until_d_date else 0 end as roi_0_until_date
    --
    --sum(date_flag) total_display_video_num,
    --sum(test_flag & date_flag)total_test_video_num,
    --sum(roi_0_flag & date_flag) total_roi_video_num,
    --sum(payment_rate_flag & date_flag) total_payment_rate_video_num
    --
from (select
    *,
    sum(pay_0_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as pay_0_sum_until_d_date,
    sum(ad_cost_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as ad_cost_sum_until_d_date,
    sum(pay_uv_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as pay_uv_sum_until_d_date,
    sum(watch_uv_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as watch_uv_sum_until_d_date
    --
    --case when display_date <= d_date and display_date >= month_begin then 1 else 0 end date_flag
    --case when ad_cost_sum > 0 then 1 else 0 end test_flag,
    --case when roi_0 is not null and roi_0>0.4 then 1 else 0 end roi_0_flag,
    --case when payment_rate is not null and payment_rate >0.02 then 1 else 0 end payment_rate_flag
    --
from (
    select
 	t.d_date,t.vid,t.lang,t.display_date,
    date(date_trunc('month',d_date)) month_begin,
    --
    case when min(t.current_diff)>=0 then sum(coalesce(t.pay_0,0)) else 0 end as pay_0_sum,
    case when min(t.current_diff)>=0 then sum(coalesce(t.ad_cost,0))  else 0 end as ad_cost_sum,
    1.0*sum(coalesce(t.pay_uv,0)) pay_uv_sum,
    sum(coalesce(t.watch_uv,0)) watch_uv_sum
    --case when min(t.current_diff)>=0 and sum(t.ad_cost)>0 then sum(coalesce(t.pay_0,0))/sum(t.ad_cost)  else null end as roi_0,
    --case when sum(t.watch_uv)>0 then 1.0*sum(t.pay_uv)/sum(t.watch_uv) else null end as payment_rate
    --
from public.dw_video_overview t
group by t.d_date,t.vid,t.lang,t.display_date ) t0
where display_date <= d_date and display_date >= month_begin ) t1 ) t2
group by lang , d_date;
---------------------------------------------------------------------
-- 求值语句4.0 错误的
---------------------------------------------------------------------
with t0 as (
    select
 	    t.d_date,t.vid,t.lang,t.display_date,
        date(date_trunc('month',d_date)) month_begin,
        case when min(t.current_diff)>=0 then sum(coalesce(t.pay_0,0)) else 0 end as pay_0_sum,
        case when min(t.current_diff)>=0 then sum(coalesce(t.ad_cost,0))  else 0 end as ad_cost_sum,
        1.0*sum(coalesce(t.pay_uv,0)) pay_uv_sum,
        sum(coalesce(t.watch_uv,0)) watch_uv_sum
    from public.dw_video_overview t
    group by t.d_date,t.vid,t.lang,t.display_date
    ),
    t1 as (
        select
            *,
            sum(pay_0_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as pay_0_sum_until_d_date,
            sum(ad_cost_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as ad_cost_sum_until_d_date,
            sum(pay_uv_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as pay_uv_sum_until_d_date,
            sum(watch_uv_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as watch_uv_sum_until_d_date
        from t0
        where display_date <= d_date and display_date >= month_begin
    ),
    t2 as (
        select
            *,
            case when watch_uv_sum_until_d_date > 0 then 1.0*pay_uv_sum_until_d_date/watch_uv_sum_until_d_date else 0 end as payment_rate_until_date,
            case when ad_cost_sum_until_d_date > 0 then 1.0*pay_0_sum_until_d_date/ad_cost_sum_until_d_date else 0 end as roi_0_until_date
        from t1
    ),
    t3 as (
        select
            d_date,
            lang,
            count(distinct vid) total_display_video_num,
            sum(case when ad_cost_sum_until_d_date >0 then 1 else 0 end) as total_test_video_num ,
            sum(case when roi_0_until_date > 0.4 then 1 else 0 end ) as total_roi_0_video_num,
            sum(case when  payment_rate_until_date > 0.02 then 1 else 0 end ) as total_payment_rate_video_num
        from t2
        group by lang , d_date
    )
select * from t3;
---------------------------------------------------------------------
-- 求值语句5.0
---------------------------------------------------------------------
with t0 as (
    select
 	    t.d_date,t.vid,t.lang,t.display_date,
        date(date_trunc('month',d_date)) month_begin,
        case when min(t.current_diff)>=0 then sum(coalesce(t.pay_0,0)) else 0 end as pay_0_sum,
        case when min(t.current_diff)>=0 then sum(coalesce(t.ad_cost,0))  else 0 end as ad_cost_sum,
        1.0*sum(coalesce(t.pay_uv,0)) pay_uv_sum,
        sum(coalesce(t.watch_uv,0)) watch_uv_sum
    from public.dw_video_overview t
    group by t.d_date,t.vid,t.lang,t.display_date
    ),
    t1 as (
        select
            *,
            sum(pay_0_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as pay_0_sum_until_d_date,
            sum(ad_cost_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as ad_cost_sum_until_d_date,
            sum(pay_uv_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as pay_uv_sum_until_d_date,
            sum(watch_uv_sum) over (partition by vid order by d_date rows between unbounded preceding and current row) as watch_uv_sum_until_d_date
        from t0
        where display_date <= d_date and display_date >= month_begin
    ),
    t2 as (
        select
            *,
            case when watch_uv_sum_until_d_date > 0 then 1.0*pay_uv_sum_until_d_date/watch_uv_sum_until_d_date else 0 end as payment_rate_until_date,
            case when ad_cost_sum_until_d_date > 0 then 1.0*pay_0_sum_until_d_date/ad_cost_sum_until_d_date else 0 end as roi_0_until_date
        from t1
    ),
    t3 as (
      select
          "oversea-api_osd_videos".id, to_timestamp(display_time)::date as display_date,"oversea-api_osd_categories".name as lang
      from "oversea-api_osd_videos" join "oversea-api_osd_categories" on "oversea-api_osd_videos".category_id = "oversea-api_osd_categories".id
      where "oversea-api_osd_videos".display_status = 1
    ),
    t4 as (
        select
            d_date,
            lang,
            sum(case when ad_cost_sum_until_d_date >0 then 1 else 0 end) as total_test_video_num ,
            sum(case when roi_0_until_date > 0.4 then 1 else 0 end ) as total_roi_0_video_num,
            sum(case when  payment_rate_until_date > 0.02 then 1 else 0 end ) as total_payment_rate_video_num
        from t2
        group by lang , d_date
    ),
    t5 as (
        select
            t4.*,
            t3.id as id
        from t4 join t3
        on display_date >= date(date_trunc('month',d_date)) and display_date <= t4.d_date and t4.lang = t3.lang
    )
select
    t5.d_date,
    t5.lang,
    count(distinct id) total_display_video_num,
    t5.total_test_video_num,
    t5.total_roi_0_video_num,
    t5.total_payment_rate_video_num
from t5
group by t5.d_date,t5.lang,t5.total_test_video_num,t5.total_roi_0_video_num,t5.total_payment_rate_video_num
