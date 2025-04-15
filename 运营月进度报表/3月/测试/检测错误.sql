-- push点击率
-- 简体中文
select
    push_id as push_id,
    push_unt as push_unt,
    click_unt as click_unt
from dw_push_view
where push_time ::date = '2025-03-01' and lang_name = '{土耳其语}'

select
    lang_name,
    sum(push_unt) as push_unt,
    sum(click_unt) as click_unt
from dw_push_view
where push_time ::date = '2025-03-01'
group by lang_name

select
    *
from dw_operate_schedule
where lang_name = '简体中文' and d_date = '2025-03-01'

-- 总7留
select
    d_date,
    sum(dau) as dau,
    sum(dau_7login) as dau_7login,
    case when sum(dau) =0 then null else 1.0*sum(dau_7login)/sum(dau) end as dau_7login_rate
from dw_operate_view
group by d_date
order by d_date desc
--                              日期      dau   dau_3/7login
-- dw_operate_view 总7日的    2025-03-09,216852,46618
-- dw_operate_view 总3日的    2025-03-13,222943,56735
--dw_retention_daily 总7日的  2025-03-09,216852,54110
--dw_retention_daily 总3日的  2025-03-13,222943,65551

select
    active_date,
    sum(dau) as dau,
    sum("总3日留存") as "总3日留存"
from dw_retention_daily
where active_date = '2025-03-13'
group by active_date

-- 总3留
