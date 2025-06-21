---------------------------------------------
-- File: 2025-06-17报表问题回复.sql
-- Time: 2025/6/17 18:47
-- User: xiaoj
-- Description:  
---------------------------------------------

-- 广告观看人数
select
    count(distinct uid)
from public.app_user_track_log
where
    to_timestamp(created_at)::date = '2025-06-16'
    and (event IN (262,266,269,39,36,5,263) OR (event = 41 AND GET_JSON_OBJECT(ext_body,'$.task') = 'Watch Ads' ))


-- k币报表
select
    order_date,
    sum(case when type=8 and sub_type='earn_ad' then money else 0 end ) as earn_ad_coin_give_num
from (
    SELECT uid,
            order_id,
            money,
            use_balance_give,
            use_recharge_balance,
            to_timestamp(created_at)::date AS order_date,
            type,
            sub_type
        FROM "middle_user_consume_record_00"
        where type=5 or type=8 or type = 0 or type = 7 or type = 4 or type = 9
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
        where type=5 or type=8 or type = 0 or type = 7 or type = 4 or type = 9
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
        where type=5 or type=8 or type = 0 or type = 7 or type = 4 or type = 9
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
        where type=5 or type=8 or type = 0 or type = 7 or type = 4 or type = 9
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
        where type=5 or type=8 or type = 0 or type = 7 or type = 4 or type = 9
) t
group by order_date;