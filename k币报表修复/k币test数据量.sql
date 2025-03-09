------------------------------------------
-- 看一眼数据类型
------------------------------------------
select
    *
from middle_user_consume_record_00
where type=8 and sub_type='firefly_ad'
limit 500;
------------------------------------------
-- oversea-api_osd_user_daily 数据量——750133764    7.5亿
--                 按照uid和date_date 去重746093449
--725970426
--721930120

--719993068
--715952764

-->= 2025-3-5
--22105350
--22105332
------------------------------------------
SELECT
    count(*)
FROM "oversea-api_osd_user_daily" -- 每日全量用户表
WHERE  data_date ='2025-03-06';
select
    count(*)
from (SELECT
    uid,data_date
FROM "oversea-api_osd_user_daily" -- 每日全量用户表
group by uid,data_date) t;
------------------------------------------
--  oversea-api_osd_user_daily 最小时间 2024-11-11
------------------------------------------
select
    min(data_date)
from "oversea-api_osd_user_daily"
where status = 1;
------------------------------------------
--  oversea-api_osd_user 数据量11165981
--                       去重数据量11166011
------------------------------------------
select  count(*)
from (select
    uid
from "oversea-api_osd_user") t;
------------------------------------------
-- all_order_log 数据量——1301810   1百万
------------------------------------------
select count(*)
from all_order_log
WHERE order_type = 1 --指充K币
AND status = 1 --订单状态成功
AND environment = 1 -- 沙箱 1 = 生产
------------------------------------------
--  middle_user_consume_record数据量——187300338    2亿
------------------------------------------
select
    count(*)
from (SELECT uid,
    use_balance_give,
    to_timestamp(created_at)::date AS order_date,
    type,
    sub_type
FROM "middle_user_consume_record_00"
where type=5 or type=8 or type = 0
union all
SELECT uid,
       use_balance_give,
    to_timestamp(created_at)::date AS order_date,
    type,
    sub_type
FROM "middle_user_consume_record_01"
where type=5 or type=8 or type = 0
union all
SELECT uid,
       use_balance_give,
    to_timestamp(created_at)::date AS order_date,
    type,
    sub_type
FROM "middle_user_consume_record_03"
where type=5 or type=8 or type = 0
union all
SELECT uid,
       use_balance_give,
    to_timestamp(created_at)::date AS order_date,
    type,
    sub_type
FROM "middle_user_consume_record_04"
where type=5 or type=8 or type = 0 ) t;
------------------------------------------
--  dw_coin_consume数据量——53018 5万
------------------------------------------
select count(*) from dw_coin_consume;
------------------------------------------
--  user_log数据量——10797585 1千万
------------------------------------------
select count(*) from user_log
where event = 1 AND created_date >= 20240701
------------------------------------------
--  k_cal 数据量 53948 5万
--  bal_info 数据量 53948
------------------------------------------

