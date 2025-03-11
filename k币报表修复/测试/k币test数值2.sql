WITH k_info AS (
    -- 从每日全量用户表中找到每日的用户k币充值数据等
    SELECT data_date,
           uid,
           balance,
           recharge_coin,
           balance_give_expired,
           total_balance_give,
           balance_give
    FROM "oversea-api_osd_user_daily"
    WHERE status = 1),
     user_info as (
         -- 补全user字段的，从登陆注册表中找到相关属性
         SELECT *
         FROM (SELECT uid,
                      area,
                      to_timestamp(created_at) :: date AS register_date,
                      CASE
                          WHEN LENGTH(COALESCE(campaign_id, '')) > 1 THEN '投流用户'
                          WHEN LENGTH(COALESCE(campaign_id, '')) <= 1 THEN '自然流用户'
                          ELSE '未知'
                          END                          AS user_type,
                      ROW_NUMBER() OVER (
                          PARTITION BY uid
                          ORDER BY
                              created_at
                          )                            AS ranking
               FROM "user_log"
               WHERE 1 = 1
                 AND event = 1
                 AND created_date >= 20240701) a
         WHERE ranking = 1),
     country_info as (

                    SELECT area,
                             country_code,
                             country_name
                      FROM "v_dim_country_area"),
     k_cal as (select k_info.data_date,
                      COALESCE(
                              country_info.area,
                              '未知'
                          )                     as area,
                      COALESCE(
                              country_info.country_name,
                              '未知'
                          )                     as country_name,
                      user_info.user_type,
                      sum(balance)              AS balance,
                      sum(recharge_coin)        as recharge_coin,
                      sum(balance_give_expired) as balance_give_expired,
                      sum(total_balance_give)   as total_balance_give,
                      sum(balance_give)         as balance_give
               from k_info
                        LEFT JOIN user_info ON k_info.uid = user_info.uid
                   AND user_info.register_date <= k_info.data_date
                        LEFT JOIN country_info ON upper(user_info.area) = country_info.country_code
               group by k_info.data_date,
                        COALESCE(
                                country_info.area,
                                '未知'
                            ),
                        COALESCE(
                                country_info.country_name,
                                '未知'
                            ),
                        user_info.user_type),
     bal_info as (select k1.data_date,
                         coalesce(k1.area,'未知') as area,
                         coalesce(k1.country_name,'未知') as country_name,
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
                  from k_cal as k1
                           left join k_cal as k2 on k1.area = k2.area
                      and k1.country_name = k2.country_name
                      and k1.user_type = k2.user_type
                  group by k1.data_date,
                           coalesce(k1.area,'未知'),
                           coalesce(k1.country_name,'未知'),
                           k1.user_type)
    select * from bal_info
    where data_date='2025-03-09'
    order by area,
            country_name,
            user_type;

    select  sum("k币余额") from dw_coin_consume_1
    where "日期"='2025-03-09'


