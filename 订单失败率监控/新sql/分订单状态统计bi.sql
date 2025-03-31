select
    order_date as 日期,
    os as 平台,
    area as 区域,
    country_name as 国家,
    sum(total_order_num) as 发起订单数,
    sum(total_unpay_order_num) as 待支付订单数,
    sum(total_cancel_order_num) as 取消支付订单数,
    sum(total_pay_order_num) as 支付成功订单数,
    sum(total_pay_fail_order_num) as 支付失败订单数,
    case when sum(total_unpay_order_num) > 0 then 1.0*sum(total_unpay_order_num)/sum(total_order_num) else null end as 待支付占比,
    case when sum(total_cancel_order_num) > 0 then 1.0*sum(total_cancel_order_num)/sum(total_order_num) else null end as 取消支付占比,
    case when sum(total_pay_order_num) > 0 then 1.0*sum(total_pay_order_num)/sum(total_order_num) else null end as 支付成功占比,
    case when sum(total_pay_fail_order_num) > 0 then 1.0*sum(total_pay_fail_order_num)/sum(total_order_num) else null end as 支付失败占比
from dw_order_status_statistics
group by order_date, os, area, country_name ;