------------------------------------------
-- file: 总充值金额-订单维度构成表.sql
-- author: xiaoj
-- time: 2025/5/19 18:53
-- description:
------------------------------------------
with tmp_total as (
    select
        t.d_date
        , sum(money) as "当日总充值金额"
    from public.dws_order_recharge_all_dimension_stat_di t
    group by t.d_date
)
select
    t.d_date as "日期"
    , order_type as "订单类型"
    , entrance as "下单入口"
    , popup_entrance as "支付入口"
    , os as "下单应用平台"
    , sum(money) as "总充值金额"
    , max(t1."当日总充值金额")as "当日总充值金额"
from dws_order_recharge_all_dimension_stat_di t
left join tmp_total t1 on t.d_date = t1.d_date
group by t.d_date, order_type, entrance, popup_entrance, os