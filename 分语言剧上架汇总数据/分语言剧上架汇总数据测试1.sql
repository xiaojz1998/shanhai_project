select
    vid,payment_rate
from (select
    vid ,
    case when sum(watch_uv)>0 then 1.0*sum(pay_uv)/sum(watch_uv) else null end payment_rate
from public.dw_video_overview t
where d_date <= '2025-02-20' and display_date <= d_date and display_date >= date(date_trunc('month',date '2025-02-20'))
group by vid ) t0
where payment_rate> 0.02;

select generate_series(date '2024-07-02',current_date ,'1 day')::date d;


