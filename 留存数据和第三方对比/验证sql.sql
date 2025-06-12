---------------------------------------------
-- File: 验证sql.sql
-- Time: 2025/6/9 18:06
-- User: xiaoj
-- Description:  
---------------------------------------------
select
    1.0*sum(次日留存)/sum(dau)
from (select
    a.d_date
    , count(distinct case when b.d_date - a.d_date = 1 then b.uid else 0 end) as 次日留存
    , count(distinct a.uid) as dau
    , 1.0*count(distinct case when b.d_date - a.d_date = 1 then b.uid else 0 end)/count(distinct a.uid)
from dwd_user_active a
left join dwd_user_active b on a.uid = b.uid and b.d_date > a.d_date
where a.d_date >= '2025-01-01' and a.d_date <= '2025-05-31'
group by a.d_date) c