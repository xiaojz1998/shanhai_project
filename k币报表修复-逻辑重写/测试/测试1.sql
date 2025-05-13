------------------------------------------
-- file: 测试1.sql
-- author: xiaoj
-- time: 2025/5/12 14:05
-- description:
------------------------------------------
-- 11-11日
select
    min(日期)
from public.dw_coin_consume_1;


-- 2024-06-24
select
    min(to_timestamp(created_at)::date)
from public.middle_user_consume_record_02


-- 测试赠送币过期
select
    *
from public.middle_user_consume_record_01
where type = 9
limit 100



-- 测试当天充值购买金额
select
    sum(money)
from (SELECT uid,
    order_id,
    money,
    use_balance_give,
    use_recharge_balance,
    to_timestamp(created_at)::date AS order_date,
    type,
    sub_type
FROM "middle_user_consume_record_00"
where type=5 or type=8 or type = 0 or type = 7 or type = 4
union all
SELECT uid,
    order_id,
    money,
    use_balance_give,
    use_recharge_balance,
    to_timestamp(created_at)::date AS order_date,
    type,
    sub_type
FROM "middle_user_consume_record_01"
where type=5 or type=8 or type = 0 or type = 7 or type = 4
union all
SELECT uid,
    order_id,
    money,
    use_balance_give,
    use_recharge_balance,
    to_timestamp(created_at)::date AS order_date,
    type,
    sub_type
FROM "middle_user_consume_record_02"
where type=5 or type=8 or type = 0 or type = 7 or type = 4
union all
SELECT uid,
    order_id,
    money,
    use_balance_give,
    use_recharge_balance,
    to_timestamp(created_at)::date AS order_date,
    type,
    sub_type
FROM "middle_user_consume_record_03"
where type=5 or type=8 or type = 0 or type = 7 or type = 4
union all
SELECT uid,
    order_id,
    money,
    use_balance_give,
    use_recharge_balance,
    to_timestamp(created_at)::date AS order_date,
    type,
    sub_type
FROM "middle_user_consume_record_04"
where type=5 or type=8 or type = 0 or type = 7 or type = 4) t
where type = 4 and order_date = '2025-04-10'
