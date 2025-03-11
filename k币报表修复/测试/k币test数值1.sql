WITH k_info AS (
        -- 从每日全量用户表中找到每日的用户k币充值数据等
        SELECT data_date,
            -- 日期
            uid,
            -- 用户id
            balance,
            -- K币余额
            recharge_coin,
            -- 总充值K币
            balance_give_expired,
            --赠送金币过期数量
            total_balance_give,
            -- 累计赠送K币总额
            balance_give -- 赠送金币余额
        FROM "oversea-api_osd_user_daily" -- 每日全量用户表
        WHERE status = 1
    ),
    tmp_uid_date_info as (
        -- 对uid和date_date去重后表格,用于主表
        select
            uid,
            data_date
        from "oversea-api_osd_user_daily" -- 每日全量用户表
        WHERE status = 1
        group by uid, data_date
    ),
    user_info as (
        -- 补全user字段表，从登陆注册表中找到相关属性
        SELECT *
        FROM (
                SELECT uid,
                    area,
                    -- 地区 用于跟国家匹配
                    to_timestamp(created_at)::date AS register_date,
                    -- 注册日期
                    CASE
                        WHEN LENGTH(COALESCE(campaign_id, '')) > 1 THEN '投流用户'
                        WHEN LENGTH(COALESCE(campaign_id, '')) <= 1 THEN '自然流用户'
                        ELSE '未知'
                    END AS user_type,
                    -- 用户类型
                    ROW_NUMBER() OVER (
                        PARTITION BY uid
                        ORDER BY created_at
                    ) AS ranking -- 用于去重
                FROM "user_log"
                WHERE 1 = 1
                    AND event = 1
                    AND created_date >= 20240701
            ) a
        WHERE ranking = 1
    ),
    country_info as (
        -- 补全国家相关字段
        SELECT area,
            country_code,
            country_name
        FROM "v_dim_country_area"
    ),
     k_cal as (
        -- k币计算表
        -- 按日期、国家、用户种类（0:游客,1:Apple,2:Google,3:Facebook,4:Tiktok） 聚合k币和充值数据
        select k_info.data_date,
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
            user_info.user_type
    )
        -- 余额信息表
        -- 自连接，补充到data_date的连续n天的值字段
        -- 8个度量字段
        select k1.data_date,
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
            and k1.user_type = k2.user_type -- 优化：加入一个过滤条件
            and (
                k2.data_date = k1.data_date
                or k2.data_date + 1 = k1.data_date
            )
        group by k1.data_date,
            coalesce(k1.area,'未知'),
            coalesce(k1.country_name,'未知'),
            k1.user_type
        having k1.data_date='2025-03-09'
        order by area,
            country_name,
            user_type