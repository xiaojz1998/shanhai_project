------------------------------------------
-- file: 测试.sql
-- author: xiaoj
-- time: 2025/5/16 11:46
-- description:
------------------------------------------
-- 测试经营报表条目数量 4147200
select
    count(*)
from public.dw_operate_view

-- 测试country_code 和 country_grade 唯一性
select
    count(country_code)
from "v_dim_country_area"

-- 测试all_order_log 条目数量 2726892
select
    count(*)
from all_order_log o
where environment = 1
     and (os ='android' or os = 'ios')
     and status = 1


select count(*) from dwd_user_info
select * from "oversea-api_osd_categories" limit 100



select count(*) from public.dws_order_recharge_all_dimension_stat_di
select sum(money) from public.dws_order_recharge_all_dimension_stat_di where d_date = '2025-05-18'
