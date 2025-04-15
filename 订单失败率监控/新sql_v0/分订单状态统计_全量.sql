---------------------------------------------------------------------
-- 建表
---------------------------------------------------------------------
drop table if exists tmp_dw_order_status_statistics;
create table if not exists tmp_dw_order_status_statistics (
    order_date date,
    os text,
    area text,
    country_name text,
    total_order_num bigint,
    total_unpay_order_num bigint,
    total_pay_order_num bigint,
    total_cancel_order_num bigint,
    total_pay_fail_order_num bigint
);

drop table if exists dw_order_status_statistics;
create table if not exists dw_order_status_statistics (
    order_date date,
    os text,
    area text,
    country_name text,
    total_order_num bigint,
    total_unpay_order_num bigint,
    total_pay_order_num bigint,
    total_cancel_order_num bigint,
    total_pay_fail_order_num bigint
);


---------------------------------------------------------------------
set timezone ='UTC-0';
truncate table tmp_dw_order_status_statistics;
insert into public.tmp_dw_order_status_statistics
with tmp_all_order_log as (
    -- 获取订单信息
    -- 同一天重复订单以最后一个状态为准
    select
        *
    from (select
        order_date::date as order_date,
        case
            when os = 'ios' then 'IOS'
            when os = 'android' then '安卓'
            else '未知'
        end as os,
        uid,
        order_num,
        status,
        row_number() over (partition by order_num,order_date order by created_at desc ) as rn
    from all_order_log
    where environment = 1 and order_date:: date >= '2025-03-22' ) t
    where rn = 1
),
    tmp_user_info as (
        -- 补充用户信息
        SELECT
          *
        FROM
          (
            SELECT
              uid,
              area,
              to_timestamp(created_at) :: date AS register_date,
              ROW_NUMBER() OVER (
                PARTITION BY uid
                ORDER BY
                  created_at
              ) AS ranking
            FROM
              "user_log"
            WHERE
              1 = 1
              AND event = 1
              AND created_date >= 20240701
          ) a
        WHERE
          ranking = 1
    ),
    tmp_country_info as (
        -- 补充国家信息
        SELECT
          area,
          country_code,
          country_name
        FROM
          "v_dim_country_area"
    )
select
    tmp_all_order_log.order_date,
    tmp_all_order_log.os,
    COALESCE(
        tmp_country_info.area,
        '未知'
      ),
    COALESCE(
      tmp_country_info.country_name,
      '未知'
    ),
    count(distinct tmp_all_order_log.order_num) as total_order_num,
    count(distinct case when status = 0 then tmp_all_order_log.order_num else null end) as total_unpay_order_num,
    count(distinct case when status = 1 then tmp_all_order_log.order_num else null end) as total_pay_order_num,
    count(distinct case when status = 2 then tmp_all_order_log.order_num else null end) as total_cancel_order_num,
    --count(distinct case when status = 3 then tmp_all_order_log.order_num else null end) as total_refund_order_num,
    count(distinct case when status = 4 then tmp_all_order_log.order_num else null end) as total_pay_fail_order_num
from tmp_all_order_log
left join tmp_user_info on tmp_all_order_log.uid = tmp_user_info.uid
left join tmp_country_info on upper(tmp_user_info.area) = tmp_country_info.country_code
group by
    tmp_all_order_log.order_date,
    tmp_all_order_log.os,
    COALESCE(
        tmp_country_info.area,
        '未知'
      ),
    COALESCE(
      tmp_country_info.country_name,
      '未知'
    );


truncate table dw_order_status_statistics;
insert into dw_order_status_statistics select * from tmp_dw_order_status_statistics;