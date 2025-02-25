--
select
    *
from (select
    date(date_trunc('month',d_date)) month_begin,
    d_date,
    display_date,
    case when d_date<display_date then 1 else 0 end flag
from public.dw_video_overview t
where date(date_trunc('month',d_date)) = '2025-02-01' )t0
where flag =1 ;
-- 结果 存在display_date > d_date的情况，但是不影响
-- 20250220日 付费率 137 449 应该是我计算错了
select
    sum (case when ad_cost_sum > 0 then 1 else 0 end ) payment_rate_num
from (select
    vid ,
    case when min(t.current_diff)>=0 then sum(coalesce(t.ad_cost,0))  else 0 end as ad_cost_sum
from public.dw_video_overview t
where d_date <= '2025-02-24' and display_date <= d_date and display_date >= date(date_trunc('month',date '2025-02-24'))
group by vid ) t0;

select
    vid,payment_rate
from (select
    vid ,
    case when sum(watch_uv)>0 then 1.0*sum(pay_uv)/sum(watch_uv) else null end payment_rate
from public.dw_video_overview t
where d_date <= '2025-02-20' and display_date <= d_date and display_date >= date(date_trunc('month',date '2025-02-20'))
group by vid ) t0
where payment_rate> 0.02;
--
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
    )
select
    vid , payment_rate_until_date
from t2
where payment_rate_until_date > 0.02 and d_date = '2025-02-20';
--
with tmp_all_video as (
    select
          "oversea-api_osd_videos".id as vid,
          to_timestamp(display_time)::date as display_date,
          "oversea-api_osd_categories".name as lang
      from "oversea-api_osd_videos" join "oversea-api_osd_categories" on "oversea-api_osd_videos".category_id = "oversea-api_osd_categories".id
      where "oversea-api_osd_videos".display_status = 1
), tmp_date as (
    select generate_series(date '2024-07-02',current_date ,'1 day')::date d_date
), all_video_date as (
    select *,
        0 as pay_0,
        0 as ad_cost,
        0 as pay_uv,
        0 as watch_uv
    from tmp_all_video join tmp_date
    on display_date <= d_date and display_date >= date(date_trunc('month', d_date))
    group by d_date, vid, display_date, lang
 )
select
    d_date,
    count(distinct vid ) num
from all_video_date
group by d_date
having date(date_trunc('month',d_date)) = '2025-02-01'