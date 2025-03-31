set timezone ='UTC-0';

----失败率数据
-- delete from public.dw_order_failed where 日期 >=(current_date+interval '-1 day');
truncate table public.dw_order_failed;
INSERT INTO public.dw_order_failed
select
  order_info.order_date as "日期",
  order_info.os as "平台",
  order_info.area as "区域",
  order_info.country_name as "国家",
  COALESCE(all_orders, 0) as "生成订单数",
  COALESCE(success_orders, 0) as "支付成功订单数",
  COALESCE(failed_orders, 0) as "支付失败订单数"
from
  (
    -- 维度： 日期、平台、区域、国家
    -- 度量字段：所有订单数、成功订单数
    select
      order_user.order_date,
      order_user.os,
      COALESCE(
        country_info.area,
        '未知'
      ) as area,
      COALESCE(
        country_info.country_name,
        '未知'
      ) as country_name,
      count(distinct order_num) as all_orders,
      count(
        distinct case
          when status = 1 then order_num
          else null
        end
      ) as success_orders
    from(
        -- 所有订单表
        SELECT
          order_date :: date as order_date,
          case
            when os = 'ios' then 'IOS'
            when os = 'android' then '安卓'
            else '未知'
          end as os,
          uid,
          order_num,
          status
        FROM
          "all_order_log" --订单日志表
        WHERE
          environment = 1
          and order_date :: date BETWEEN '2024-11-23'
          AND current_date
      ) as order_user
      LEFT JOIN(
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
      ) AS user_info ON order_user.uid = user_info.uid
      LEFT JOIN (
        -- 补充国家信息
        SELECT
          area,
          country_code,
          country_name
        FROM
          "v_dim_country_area"
      ) AS country_info ON upper(user_info.area) = country_info.country_code
    group by
      order_user.order_date,
      order_user.os,
      COALESCE(
        country_info.area,
        '未知'
      ),
      COALESCE(
        country_info.country_name,
        '未知'
      )
  ) as order_info

  left join(
    -- 求支付报错订单数
    select
      failed_user.created_date,
      failed_user.platform,
      COALESCE(
        country_info.area,
        '未知'
      ) AS area,
      COALESCE(
        country_info.country_name,
        '未知'
      ) AS country_name,
      count(distinct id) as failed_orders
    from
      (
        SELECT
          to_timestamp(created_at) :: date as created_date,
          ext_body :: json ->> 'type' AS type,
          ext_body :: json ->> 'paytype' :: VARCHAR AS paytype,
          id,
          case
            when platform = 'ios' then 'IOS'
            when platform = 'android' then '安卓'
            else '未知'
          end as platform,
          uid
        FROM
          "app_user_track_log"
        where
          event = 195
          and ext_body :: json ->> 'paytype' :: VARCHAR in ('1', '2')
          and ext_body :: json ->> 'type' <> '1060'
    and ext_body :: json ->> 'type' <> '1060 Please do not confirm again'
    AND ext_body :: json ->> 'type' <>  '1033 The current order is being processed...'
          and TO_TIMESTAMP(created_at) :: date BETWEEN '2024-11-23'
          AND current_date
      ) as failed_user
      LEFT JOIN(
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
      ) AS user_info ON failed_user.uid = user_info.uid
      LEFT JOIN (
        SELECT
          area,
          country_code,
          country_name
        FROM
          "v_dim_country_area"
      ) AS country_info ON upper(user_info.area) = country_info.country_code
    group by
      failed_user.created_date,
      failed_user.platform,
      COALESCE(
        country_info.area,
        '未知'
      ),
      COALESCE(
        country_info.country_name,
        '未知'
      )
  ) as failed_info on order_info.order_date = failed_info.created_date
  and order_info.os = failed_info.platform
  and order_info.area = failed_info.area
  and order_info.country_name = failed_info.country_name;


-- set timezone ='UTC-0';
----细分数据
-- delete from public.dw_order_failed_subdivision where 日期 >=(current_date+interval '-1 day');
truncate table public.dw_order_failed_subdivision;
INSERT INTO public.dw_order_failed_subdivision
  select
  failed_user.created_date as "日期",
  failed_user.platform as "平台",
  CASE
        WHEN failed_user.type LIKE 'IOS%' THEN
            'IOS {"error":' || REGEXP_REPLACE(failed_user.type, '.*"error":"([^"]+)".*', '\1') || '}'
        WHEN failed_user.type LIKE 'GooglePayManager%' THEN
            'GooglePayManager {"msg":' || REGEXP_REPLACE(failed_user.type, '.*"msg":"([^"]+)".*', '\1') || '}'
        ELSE failed_user.type
    END AS "错误码",
--   failed_user.type as "错误码",
  COALESCE(
    country_info.area,
    '未知'
  ) AS "区域",
  COALESCE(
    country_info.country_name,
    '未知'
  ) AS "国家",
  COALESCE(count(distinct id),0) as "支付失败订单数"
from
  (
    SELECT
      to_timestamp(created_at) :: date as created_date,
      ext_body :: json ->> 'type' AS type,
      ext_body :: json ->> 'paytype' :: VARCHAR AS paytype,
      id,
      case
        when platform = 'ios' then 'IOS'
        when platform = 'android' then '安卓'
        else '未知'
      end as platform,
      uid
    FROM
      "app_user_track_log"
    where
      event = 195
      and ext_body :: json ->> 'paytype' :: VARCHAR in ('1', '2')
      and ext_body :: json ->> 'type' <> '1060'
    and ext_body :: json ->> 'type' <> '1060 Please do not confirm again'
    AND ext_body :: json ->> 'type' <>  '1033 The current order is being processed...'
      and TO_TIMESTAMP(created_at) :: date BETWEEN '2024-11-23'
      AND current_date
  ) as failed_user
  LEFT JOIN(
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
  ) AS user_info ON failed_user.uid = user_info.uid
  LEFT JOIN (
    SELECT
      area,
      country_code,
      country_name
    FROM
      "v_dim_country_area"
  ) AS country_info ON upper(user_info.area) = country_info.country_code
group by
  failed_user.created_date,
  failed_user.platform,
--   failed_user.type,
  CASE
        WHEN failed_user.type LIKE 'IOS%' THEN
            'IOS {"error":' || REGEXP_REPLACE(failed_user.type, '.*"error":"([^"]+)".*', '\1') || '}'
        WHEN failed_user.type LIKE 'GooglePayManager%' THEN
            'GooglePayManager {"msg":' || REGEXP_REPLACE(failed_user.type, '.*"msg":"([^"]+)".*', '\1') || '}'
        ELSE failed_user.type
    END,
  COALESCE(
    country_info.area,
    '未知'
  ),
  COALESCE(
    country_info.country_name,
    '未知'
  )
;




  ----监控数据
-- delete from public.dw_order_failed_monitor where 日期 >=(current_date+interval '-1 day');
truncate table public.dw_order_failed_monitor;
INSERT INTO public.dw_order_failed_monitor
select
  failed_user.created_date as "日期",
  failed_user.platform as "平台",
  COALESCE(
    country_info.area,
    '未知'
  ) AS "区域",
  COALESCE(
    country_info.country_name,
    '未知'
  ) AS "国家",
  failed_user.uid as "用户ID",
  failed_user.type as "错误码"
from
  (
    SELECT
      to_timestamp(created_at) :: date as created_date,
      ext_body :: json ->> 'type' AS type,
      ext_body :: json ->> 'paytype' :: VARCHAR AS paytype,
      id,
      case
        when platform = 'ios' then 'IOS'
        when platform = 'android' then '安卓'
        else '未知'
      end as platform,
      uid
    FROM
      "app_user_track_log"
    where
      event = 195
      and ext_body :: json ->> 'paytype' :: VARCHAR in ('1', '2')
      and ext_body :: json ->> 'type' <> '1060'
    and ext_body :: json ->> 'type' <> '1060 Please do not confirm again'
    AND ext_body :: json ->> 'type' <>  '1033 The current order is being processed...'
      and TO_TIMESTAMP(created_at) :: date BETWEEN '2024-11-23'
      AND current_date
  ) as failed_user
  LEFT JOIN(
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
  ) AS user_info ON failed_user.uid = user_info.uid
  LEFT JOIN (
    SELECT
      area,
      country_code,
      country_name
    FROM
      "v_dim_country_area"
  ) AS country_info ON upper(user_info.area) = country_info.country_code;