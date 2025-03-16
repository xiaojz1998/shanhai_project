set timezone ='UTC-0';

delete from public.dw_coin_consume where 日期 >=(current_date+interval '-1 day');
INSERT INTO public.dw_coin_consume
WITH k_info AS(
  SELECT
    data_date,
    uid,
    balance,
    recharge_coin,
    balance_give_expired,
    total_balance_give,
    balance_give
  FROM
    "oversea-api_osd_user_daily"
  WHERE
    status = 1
),
user_info as (
  SELECT
    *
  FROM
    (
      SELECT
        uid,
        area,
        to_timestamp(created_at) :: date AS register_date,
        CASE
          WHEN LENGTH(COALESCE(campaign_id, '')) > 1 THEN '投流用户'
          WHEN LENGTH(COALESCE(campaign_id, '')) <= 1 THEN '自然流用户'
          ELSE '未知'
        END AS user_type,
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
country_info as (
  SELECT
    area,
    country_code,
    country_name
  FROM
    "v_dim_country_area"
),
k_cal as (
  select
    k_info.data_date,
    COALESCE(
      country_info.area,
      '未知'
    ) as area,
    COALESCE(
      country_info.country_name,
      '未知'
    ) as country_name,
    user_info.user_type,
    sum(balance) AS balance,
    sum(recharge_coin) as recharge_coin,
    sum(balance_give_expired) as balance_give_expired,
    sum(total_balance_give) as total_balance_give,
    sum(balance_give) as balance_give
  from
    k_info
    LEFT JOIN user_info ON k_info.uid = user_info.uid
    AND user_info.register_date <= k_info.data_date
    LEFT JOIN country_info ON upper(user_info.area) = country_info.country_code
  group by
    k_info.data_date,
    COALESCE(
      country_info.area,
      '未知'
    ),
    COALESCE(
      country_info.country_name,
      '未知'
    ),
    user_info.user_type
),
bal_info as (
  select
    k1.data_date,
    k1.area,
    k1.country_name,
    k1.user_type,
    sum(
      case
        when k2.data_date = k1.data_date then k1.balance
        else null
      end
    ) as balance,
    sum(
      case
        when k2.data_date + 1 = k1.data_date then k2.balance
        else null
      end
    ) as lag_1d_balance,
    sum(
      case
        when k2.data_date = k1.data_date then k1.recharge_coin
        else null
      end
    ) as recharge_coin,
    sum(
      case
        when k2.data_date + 1 = k1.data_date then k2.recharge_coin
        else null
      end
    ) as lag_1d_recharge_coin,
    sum(
      case
        when k2.data_date = k1.data_date then k1.balance_give_expired
        else null
      end
    ) as balance_give_expired,
    sum(
      case
        when k2.data_date = k1.data_date then k1.total_balance_give
        else null
      end
    ) as total_balance_give,
    sum(
      case
        when k2.data_date = k1.data_date then k1.balance_give
        else null
      end
    ) as balance_give,
    sum(
      case
        when k2.data_date + 1 = k1.data_date then k2.balance_give
        else null
      end
    ) as lag_1d_balance_give
  from
    k_cal as k1
    left join k_cal as k2 on k1.area = k2.area
    and k1.country_name = k2.country_name
    and k1.user_type = k2.user_type
  group by
    k1.data_date,
    k1.area,
    k1.country_name,
    k1.user_type
)
select
  bal_info.data_date AS "日期",
  bal_info.area AS "区域",
  bal_info.country_name AS "国家",
  bal_info.user_type AS "用户类型",
  COALESCE(balance, 0) AS "K币余额",
  COALESCE(balance, 0) - COALESCE(lag_1d_balance, 0) AS "K币余额日变动量",
  COALESCE(balance, 0) - COALESCE(balance_give, 0) AS "购买币余额",
  COALESCE(balance_give, 0) AS "赠送币余额",
  COALESCE(balance_give_expired, 0) AS "赠送币回收数量",
  COALESCE(use_balance_give, 0) AS "赠送币使用数量",
  (
    COALESCE(lag_1d_balance, 0) - COALESCE(lag_1d_balance_give, 0)
  ) + COALESCE(recharge_num, 0) - (
    COALESCE(balance, 0) - COALESCE(balance_give, 0)
  ) AS "当天消耗的充值K币",
  COALESCE(recharge_num, 0) AS "当天充值购买的K币总数",
  COALESCE(recharge_coin, 0) AS "累计购买K币",
  COALESCE(coin_give, 0) + COALESCE(coin_num, 0) + COALESCE(gift_num, 0) + COALESCE(activity_num, 0) AS "累计赠送K币",
  COALESCE(coin_give, 0) AS "累计充值赠币",
  COALESCE(coin_num, 0) AS "累计签到赠币",
  COALESCE(activity_num1, 0) AS "累计看广告赠币",
  COALESCE(activity_num2, 0) AS "累计看剧任务赠币",
  COALESCE(activity_num3, 0) AS "累计分享赠币",
  COALESCE(activity_num4, 0) AS "累计看特定的剧集赠币",
  COALESCE(activity_num5, 0) AS "累计特定剧集时长赠币",
  COALESCE(activity_num6, 0) AS "累计其他任务赠币",
  COALESCE(gift_num, 0) AS "累计社媒活动赠币"
from
  bal_info
  left join(
    SELECT
      k_info.data_date,
      COALESCE(
        country_info.area,
        '未知'
      ) AS area,
      COALESCE(
        country_info.country_name,
        '未知'
      ) AS country_name,
      user_info.user_type,
      sum(use_balance_give) as use_balance_give,
      sum(recharge_num) as recharge_num,
      sum(coin_give) as coin_give,
      sum(coin_num) as coin_num,
      sum(gift_num) as gift_num,
      sum(activity_num) as activity_num,
      sum(activity_num1) as activity_num1,
      sum(activity_num2) as activity_num2,
      sum(activity_num3) as activity_num3,
      sum(activity_num4) as activity_num4,
      sum(activity_num5) as activity_num5,
      sum(activity_num6) as activity_num6
    FROM
      k_info
      LEFT JOIN user_info ON k_info.uid = user_info.uid
      AND user_info.register_date <= k_info.data_date
      LEFT JOIN country_info ON upper(user_info.area) = country_info.country_code
      LEFT JOIN(
        SELECT
          k_info.uid,
          k_info.data_date,
          sum(coin_give) AS coin_give
        FROM
          k_info
          LEFT JOIN (
            SELECT
              uid,
              order_num,
              order_date :: date order_date,
              coin_give --充值赠送金币数
            FROM
              "all_order_log" --订单日志表
            WHERE
              order_type = 1
              AND status = 1
              AND environment = 1
          ) AS recharge_info ON k_info.uid = recharge_info.uid
          AND recharge_info.order_date <= k_info.data_date
        GROUP BY
          k_info.uid,
          k_info.data_date
      ) AS recharge_coin_give_info ON k_info.uid = recharge_coin_give_info.uid
      and k_info.data_date = recharge_coin_give_info.data_date
      LEFT JOIN(
        SELECT
          k_info.uid,
          k_info.data_date,
          sum(recharge_num) AS recharge_num
        FROM
          k_info
          LEFT JOIN (
            SELECT
              uid,
              order_num,
              order_date :: date order_date,
              coin-coin_give AS recharge_num --充值购买金币数
            FROM
              "all_order_log" --订单日志表
            WHERE
              order_type = 1
              AND status = 1
              AND environment = 1
          ) AS day_recharge_info ON k_info.uid = day_recharge_info.uid
          AND day_recharge_info.order_date = k_info.data_date
        GROUP BY
          k_info.uid,
          k_info.data_date
      ) AS recharge_coin_info ON k_info.uid = recharge_coin_info.uid
      and k_info.data_date = recharge_coin_info.data_date
      LEFT JOIN (
        SELECT
          k_info.uid,
          k_info.data_date,
          sum(
            CASE
              WHEN TYPE IN (
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20
              ) THEN reward_coin
              ELSE 0
            END
          ) AS activity_num,
          sum(
            CASE
              WHEN TYPE IN (1, 12, 16) THEN reward_coin
              ELSE 0
            END
          ) AS activity_num1,
          sum(
            CASE
              WHEN TYPE IN (2, 4) THEN reward_coin
              ELSE 0
            END
          ) AS activity_num2,
          sum(
            CASE
              WHEN TYPE = 15 THEN reward_coin
              ELSE 0
            END
          ) AS activity_num3,
          sum(
            CASE
              WHEN TYPE = 17 THEN reward_coin
              ELSE 0
            END
          ) AS activity_num4,
          sum(
            CASE
              WHEN TYPE = 18 THEN reward_coin
              ELSE 0
            END
          ) AS activity_num5,
          sum(
            CASE
              WHEN TYPE IN (3, 5, 6, 7, 8, 9, 10, 11, 13, 14, 19, 20) THEN reward_coin
              ELSE 0
            END
          ) AS activity_num6
        FROM
          k_info
          LEFT join (
            SELECT
              c.task_date,
              c.uid,
              TYPE,
              reward_coin
            FROM
              (
                SELECT
                  uid,
                  task_id,
                  to_timestamp(created_at) :: date AS task_date
                FROM
                  "oversea-api_osd_user_task" --任务用户关联表
                WHERE
                  status = 3
              ) c
              LEFT JOIN (
                SELECT
                  id,
                  TYPE,
                  --活动类型
                  reward_coin --奖励K币数
                FROM
                  "oversea-api_osd_task" --任务配置表
                WHERE
                  status = 1
              ) d ON c.task_id = d.id
          ) AS activity_info ON k_info.uid = activity_info.uid
          AND activity_info.task_date <= k_info.data_date
        GROUP BY
          k_info.uid,
          k_info.data_date
      ) AS activity_coin_give_info ON k_info.uid = activity_coin_give_info.uid
      AND activity_coin_give_info.data_date = k_info.data_date
      LEFT JOIN(
        SELECT
          k_info.uid,
          k_info.data_date,
          sum(coin_num) AS coin_num
        FROM
          k_info
          LEFT join (
            SELECT
              uid,
              to_timestamp(created_at) :: date AS sign_date,
              coin_num --签到赠币数量
            FROM
              --用户签到表
              "oversea-api_osd_user_sign_record"
          ) AS sign_info ON k_info.uid = sign_info.uid
          AND sign_info.sign_date <= k_info.data_date
        GROUP BY
          k_info.uid,
          k_info.data_date
      ) AS sign_coin_give_info ON k_info.uid = sign_coin_give_info.uid
      AND sign_coin_give_info.data_date = k_info.data_date
      LEFT JOIN (
        SELECT
          k_info.uid,
          k_info.data_date,
          sum(gift_num) AS gift_num
        FROM
          k_info
          LEFT JOIN (
            SELECT
              e.gift_date,
              e.uid,
              coin AS gift_num
            FROM
              (
                SELECT
                  uid,
                  gift_code_id,
                  to_timestamp(created_at) :: date AS gift_date
                FROM
                  "oversea-api_osd_user_gift_code_log" --用户金币兑换记录表
              ) e
              LEFT JOIN (
                SELECT
                  id,
                  coin --单次兑换金额
                FROM
                  "oversea-api_osd_gift_code" --社媒金币赠送活动表
                WHERE
                  status = 1
              ) f ON e.gift_code_id = f.id
          ) AS gift_info ON k_info.uid = gift_info.uid
          AND gift_info.gift_date <= k_info.data_date
        GROUP BY
          k_info.uid,
          k_info.data_date
      ) AS gift_coin_give_info ON k_info.uid = gift_coin_give_info.uid
      AND gift_coin_give_info.data_date = k_info.data_date
      left join(
        SELECT
          k_info.uid,
          k_info.data_date,
          sum(use_balance_give) AS use_balance_give
        FROM
          k_info
          LEFT JOIN (
            SELECT
              uid,
              order_id,
              to_timestamp(created_at) :: date AS order_date,
              use_balance_give
            FROM
              "middle_user_consume_record_00"
            WHERE
              TYPE = 0
            UNION
            SELECT
              uid,
              order_id,
              to_timestamp(created_at) :: date AS order_date,
              use_balance_give
            FROM
              "middle_user_consume_record_01"
            WHERE
              TYPE = 0
            UNION
            SELECT
              uid,
              order_id,
              to_timestamp(created_at) :: date AS order_date,
              use_balance_give
            FROM
              "middle_user_consume_record_02"
            WHERE
              TYPE = 0
            UNION
            SELECT
              uid,
              order_id,
              to_timestamp(created_at) :: date AS order_date,
              use_balance_give
            FROM
              "middle_user_consume_record_03"
            WHERE
              TYPE = 0
            UNION
            SELECT
              uid,
              order_id,
              to_timestamp(created_at) :: date AS order_date,
              use_balance_give
            FROM
              "middle_user_consume_record_04"
            WHERE
              TYPE = 0
          ) AS use_info ON k_info.uid = use_info.uid
          AND use_info.order_date <= k_info.data_date
        GROUP BY
          k_info.uid,
          k_info.data_date
      ) as use_give_info ON k_info.uid = use_give_info.uid
      AND use_give_info.data_date = k_info.data_date
    GROUP BY
      k_info.data_date,
      COALESCE(
        country_info.area,
        '未知'
      ),
      COALESCE(
        country_info.country_name,
        '未知'
      ),
      user_info.user_type
  ) as dd on bal_info.data_date = dd.data_date
  and bal_info.area = dd.area
  and bal_info.country_name = dd.country_name
  and bal_info.user_type = dd.user_type
  -- where bal_info.data_date between '2024-11-12' AND current_date
  where bal_info.data_date >=(current_date+interval '-1 day')
