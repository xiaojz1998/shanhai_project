set timezone ='UTC-0';
delete from  tmp_dw_order_status_statistics where order_date >= (current_date+interval '-2 day') ;
insert into public.tmp_dw_order_status_statistics
with tmp_all_order_log as (
    -- 获取订单信息
    -- 同一天重复订单以最后一个状态为准
    select
        *
    from (select
        created_date::text::date as created_date,
        case
            when os = 'ios' then 'IOS'
            when os = 'android' then '安卓'
            else '未知'
        end as os,
        uid,
        order_num,
        status,
        row_number() over (partition by order_num,created_date order by created_at desc, case when status = 1 then 3
            when status = 3 then 4 when status = 4 then 2 when status = 2 then 1 else status end desc ) as rn
    from all_order_log
    where environment = 1 and created_date::text:: date >= '2025-03-22' ) t
    where rn = 1 and created_date::text::date >= (current_date+interval '-2 day')
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
    ),
    tmp_failed_order_statistics as (
        select
            t.created_date,
            t.platform,
            coalesce(tmp_country_info.area, '未知') as area,
            coalesce(tmp_country_info.country_name, '未知') as country_name,
            count(distinct t.id) as failed_order_num
        from(
        SELECT
          to_timestamp(created_at) :: date as created_date,
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
          event = 60
          and TO_TIMESTAMP(created_at) :: date BETWEEN '2025-03-22' AND current_date
          and TO_TIMESTAMP(created_at) :: date >=  (current_date+interval '-2 day')
          ) t
        left join tmp_user_info on t.uid = tmp_user_info.uid
        left join tmp_country_info on upper(tmp_user_info.area) = tmp_country_info.country_code
        group by t.created_date,
                 t.platform,
                 coalesce(tmp_country_info.area, '未知'),
                 coalesce(tmp_country_info.country_name, '未知')
    )
select
    t.created_date as order_date,
    t.os,
    t.area,
    t.country_name,
    coalesce(t.total_order_num,0)+ coalesce(t1.failed_order_num,0) as total_order_request_num,
    coalesce(t1.failed_order_num,0),
    coalesce(t.total_order_num,0),
    coalesce(t.total_unpay_order_num,0),
    coalesce(t.total_pay_order_num,0),
    coalesce(t.total_cancel_order_num,0),
    coalesce(t.total_pay_fail_order_num,0)
from (select
    tmp_all_order_log.created_date,
    tmp_all_order_log.os,
    COALESCE(
        tmp_country_info.area,
        '未知'
      ) as area,
    COALESCE(
      tmp_country_info.country_name,
      '未知'
    ) as country_name,
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
    tmp_all_order_log.created_date,
    tmp_all_order_log.os,
    COALESCE(
        tmp_country_info.area,
        '未知'
      ),
    COALESCE(
      tmp_country_info.country_name,
      '未知'
    )) t
left join tmp_failed_order_statistics t1
on t.created_date = t1.created_date
and t.os = t1.platform
and t.area = t1.area
and t.country_name = t1.country_name;

delete from dw_order_status_statistics where order_date >= (current_date+interval '-2 day') ;
insert into dw_order_status_statistics select * from tmp_dw_order_status_statistics where order_date >= (current_date+interval '-2 day');