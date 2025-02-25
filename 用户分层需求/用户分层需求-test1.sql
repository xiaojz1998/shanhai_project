-- account_id和总数一样，意味着这是个账户vip表
select count(distinct account_id)
from duanju_account_vip;
-- 13087 要比duanju_vip_order的数量小
with t0 as (
    select  * from duanju_vip_order
), t1 as (
    select  * from duanju_account_vip
)
select
    count(*)
from t0 join t1 on t0.account_id = t1.account_id;
-- 测试to_timestamp和比较大小
select
    to_timestamp(expire_time)::date,
    current_timestamp,
    case when current_timestamp < to_timestamp(expire_time) then 0 else 1 end flag
from duanju_account_vip;
-- 看看此表中vip没有过期的账户数量 1833
select
    account_id
from duanju_account_vip
where current_timestamp <= to_timestamp(expire_time);
-- 检测duanju_vip_order 8859 8661
-- 说明有同一个设备对应不同账号的情况，具体人为一个设备，而不是一个账号
select
    count(*)
from (
select
    device_id,product_name,account_id
from duanju_vip_order
group by device_id,product_name,account_id) t;
-- 测试vip 未过期的设备是1770
select
    count(*)
from tmp.dw_user_layer_tag_tmp02
where subscription_status =2