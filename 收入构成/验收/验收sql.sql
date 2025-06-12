---------------------------------------------
-- File: 验收sql.sql
-- Time: 2025/6/3 10:48
-- User: xiaoj
-- Description:  
---------------------------------------------
-- 剧级
select
    sum(money)
from all_order_log
where to_timestamp(created_at)::date = '2025-06-02'::date
    and environment = 1
    and os ='ios'
    and status = 1