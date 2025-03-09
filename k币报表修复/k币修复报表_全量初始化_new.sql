set timezone = 'UTC-0';
-- delete from public.dw_coin_consume where 日期 >=(current_date+interval '-1 day');
-- truncate table public.dw_coin_consume;
-- insert into public.dw_coin_consume
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
    tmp_total_user_consume_record as (
        -- 将middle_user_consume_record_00-04这几个表合并做中间表
        SELECT uid,
            order_id,
            use_balance_give,
            to_timestamp(created_at)::date AS order_date,
            type,
            sub_type
        FROM "middle_user_consume_record_00"
        where type=5 or type=8 or type = 0
        union all
        SELECT uid,
            order_id,
            use_balance_give,
            to_timestamp(created_at)::date AS order_date,
            type,
            sub_type
        FROM "middle_user_consume_record_01"
        where type=5 or type=8 or type = 0
        union all
        SELECT uid,
            order_id,
            use_balance_give,
            to_timestamp(created_at)::date AS order_date,
            type,
            sub_type
        FROM "middle_user_consume_record_03"
        where type=5 or type=8 or type = 0
        union all
        SELECT uid,
            order_id,
            use_balance_give,
            to_timestamp(created_at)::date AS order_date,
            type,
            sub_type
        FROM "middle_user_consume_record_04"
        where type=5 or type=8 or type = 0
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
    ),
    bal_info as (
        -- 余额信息表
        -- 自连接，补充到data_date的连续n天的值字段
        -- 8个度量字段
        select k1.data_date,
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
        from k_cal as k1
            left join k_cal as k2 on k1.area = k2.area
            and k1.country_name = k2.country_name
            and k1.user_type = k2.user_type -- 优化：加入一个过滤条件
            and (
                k2.data_date = k1.data_date
                or k2.data_date + 1 = k1.data_date
            )
        group by k1.data_date,
            k1.area,
            k1.country_name,
            k1.user_type
    ),
    --FIXME：
    -- 链接条件是等于号，不算太严重
    recharge_coin_info as (
        -- 用户在date_date的当天充值购买金币总数 recharge_coin_info
        -- 1个度量字段
        SELECT tmp_uid_date_info.uid,
            tmp_uid_date_info.data_date,
            sum(recharge_num) AS recharge_coin_num
        FROM tmp_uid_date_info
            LEFT JOIN (
                SELECT uid,
                    order_num,
                    order_date::date order_date,
                    coin - coin_give AS recharge_num --充值的币数（包含赠送）- 赠送的币数 = 充值购买金币数
                FROM "all_order_log" --订单日志表
                WHERE order_type = 1
                    AND status = 1
                    AND environment = 1
            ) AS day_recharge_info ON tmp_uid_date_info.uid = day_recharge_info.uid
            AND day_recharge_info.order_date = tmp_uid_date_info.data_date
        GROUP BY tmp_uid_date_info.uid,
            tmp_uid_date_info.data_date
    ),
    -- FIXME： 这个爆表了！！
    total_coin_give_info as (
        -- 用户在date_date之前的不同类型赠币以及累计使用赠币统计
        -- 13个度量字段
        select
            tmp_uid_date_info.uid,
            tmp_uid_date_info.data_date,
            -- 用户累计充值赠币
            sum(case when type = 7 and order_id >0 then use_balance_give else 0 end ) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row ) as recharge_coin_give_num,
            -- 用户累计签到获得赠币
            sum(case when type = 5 then use_balance_give else 0 end) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row ) as sign_coin_give_num,
            -- 用户累计赠送币使用数量之和
            sum(case when type = 0 then use_balance_give else 0 end) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as use_balance_give_num,
            -- 用户累计做任务赠币
            sum(case when type = 8 then use_balance_give else 0 end) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as total_activity_coin_give_num,
            -- 用户累计看阶梯广告赠币
            sum(case when type=8 and sub_type='ladder_ad' then use_balance_give else 0 end) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as ladder_ad_coin_give_num,
            -- 用户累计看firefly广告赠币
            sum(case when type=8 and sub_type='firefly_ad' then use_balance_give else 0 end) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as firefly_ad_coin_give_num,
            -- 用户累计看adcloud广告赠币
            sum(case when type=8 and sub_type='ad_cloud' then use_balance_give else 0 end ) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as ad_cloud_coin_give_num,
            -- 用户累计看签到广告赠币
            sum(case when type=8 and sub_type='sign_ad' then use_balance_give else 0 end) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as sign_ad_coin_give_num,
            -- 用户累计看剧任务赠币
            sum(case when type=8 and sub_type='episode_time' then use_balance_give else 0 end) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as episode_time_coin_give_num,
            -- 用户累计分享赠币
            sum(case when type=8 and sub_type='app_share' then use_balance_give else 0 end ) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as app_share_coin_give_num,
            -- 用户累计看特定的剧集赠送币
            sum(case when type=8 and sub_type='specific_episodes_nodes' then use_balance_give else 0 end) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as specific_episodes_nodes_coin_give_num,
            -- 用户累计特定剧集时长赠送币
            sum(case when type=8 and sub_type='specific_episodes_time' then use_balance_give else 0 end ) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as specific_episodes_time_coin_give_num,
            -- 用户累计社媒活动赠币
            sum(case when type=8 and sub_type='gift_code' then use_balance_give else 0 end) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as gift_code_coin_give_num,
            -- 用户累计其他任务赠币
            sum(case when type=8 and sub_type not in ('ladder_ad','firefly_ad','ad_cloud','sign_ad','episode_time','app_share','specific_episodes_nodes','specific_episodes_time','gift_code') then use_balance_give else 0 end) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as other_activity_coin_give_num
        from tmp_uid_date_info
        left join tmp_total_user_consume_record
        on tmp_uid_date_info.uid = tmp_total_user_consume_record.uid
        and tmp_total_user_consume_record.order_date = tmp_uid_date_info.data_date
    ),
    tmp_total_statistics as (
        select
            tmp_uid_date_info.data_date,
            coalesce(country_info.area,'未知') as area,
            coalesce(country_info.country_name,'未知') as country_name,
            user_info.user_type,
            -- 4个维度 15个度量字段
            sum(recharge_coin_give_num) as recharge_coin_give_num, --充值赠送币
            sum(recharge_coin_num) as recharge_coin_num,           --充值购买金币数量
            sum(sign_coin_give_num) as sign_coin_give_num ,        -- 签到赠送币数量
            sum(use_balance_give_num) as use_balance_give_num,     --赠送币使用数量
            sum(total_activity_coin_give_num) as total_activity_coin_give_num, --做任务赠币总数
            sum(ladder_ad_coin_give_num) as ladder_ad_coin_give_num, -- 阶梯广告赠币
            sum(firefly_ad_coin_give_num) as firefly_ad_coin_give_num,
            sum(ad_cloud_coin_give_num) as ad_cloud_coin_give_num,
            sum(sign_ad_coin_give_num) as sign_ad_coin_give_num,
            sum(episode_time_coin_give_num) as episode_time_coin_give_num, --累计看剧任务赠币
            sum(app_share_coin_give_num) as app_share_coin_give_num,       --累计分享赠币
            sum(specific_episodes_nodes_coin_give_num) as specific_episodes_nodes_coin_give_num, --累计看特定的剧集赠送币
            sum(specific_episodes_time_coin_give_num) as specific_episodes_time_coin_give_num, --累计特定剧集时长赠送币
            sum(gift_code_coin_give_num) as gift_code_coin_give_num, -- 累计社媒赠币
            sum(other_activity_coin_give_num) as other_activity_coin_give_num -- 其他任务赠币
        from tmp_uid_date_info
        left join user_info on tmp_uid_date_info.uid = user_info.uid --用户k币数据和用户信息 关联
        and user_info.register_date <= tmp_uid_date_info.data_date
        left join country_info ON upper(user_info.area) = country_info.country_code -- 国家信息关联
        left join recharge_coin_info on tmp_uid_date_info.uid = recharge_coin_info.uid
        and tmp_uid_date_info.data_date = recharge_coin_info.data_date
        left join total_coin_give_info on tmp_uid_date_info.uid = total_coin_give_info.uid
        and tmp_uid_date_info.data_date = total_coin_give_info.data_date
        group by tmp_uid_date_info.data_date,
                  coalesce(country_info.area,'未知'),
                  coalesce(country_info.country_name,'未知'),
                  user_info.user_type
    )
    -- 临时表定义结束
select
      -- 4个维度
      bal_info.data_date as "日期",
      bal_info.area as "区域",
      bal_info.country_name as "国家",
      bal_info.user_type as "用户类型",
      --
      coalesce(balance,0) as "k币余额",
      COALESCE(balance, 0) - COALESCE(lag_1d_balance, 0) AS "K币余额日变动量",
      COALESCE(balance, 0) - COALESCE(balance_give, 0) AS "购买币余额",
      COALESCE(balance_give, 0) AS "赠送币余额",
      COALESCE(balance_give_expired, 0) AS "赠送币回收数量",
      coalesce(use_balance_give_num, 0) as "赠送币使用数量",
      (COALESCE(lag_1d_balance, 0) - COALESCE(lag_1d_balance_give, 0))
          + COALESCE(recharge_coin_num, 0) - (COALESCE(balance, 0) - COALESCE(balance_give, 0)
      ) AS "当天消耗的充值K币",
      coalesce(recharge_coin_num,0) as "当天充值购买的K币总数",
      coalesce(recharge_coin,0) as "累计购买K币",
      coalesce(total_activity_coin_give_num,0)+coalesce(recharge_coin_give_num,0)
          +coalesce(sign_coin_give_num,0) as "累计赠送K币",
      coalesce(recharge_coin,0)+coalesce(total_activity_coin_give_num,0)+coalesce(recharge_coin_give_num,0)
          +coalesce(sign_coin_give_num,0) as "累计总k币",
      coalesce(recharge_coin_give_num,0) as "累计充值赠币",
      coalesce(sign_coin_give_num,0) as "累计签到赠币",
      coalesce(ladder_ad_coin_give_num,0) as "累计看阶梯广告赠币",
      coalesce(firefly_ad_coin_give_num,0) as "累计看firefly广告赠币",
      coalesce(ad_cloud_coin_give_num,0) as "累计看adcloud广告赠币",
      coalesce(sign_ad_coin_give_num,0) as "累计看签到广告赠币",
      coalesce(episode_time_coin_give_num,0) as "累计看剧任务赠币",
      coalesce(app_share_coin_give_num,0) as "累计分享赠币",
      coalesce(specific_episodes_nodes_coin_give_num,0) as "累计看特定的剧集赠送币",
      coalesce(specific_episodes_time_coin_give_num,0) as "累计特定剧集时长赠送币",
      coalesce(gift_code_coin_give_num,0) as "累计社媒活动赠币",
      coalesce(other_activity_coin_give_num,0) as "累计其他任务赠币"
from bal_info
left join tmp_total_statistics
    on bal_info.data_date = tmp_total_statistics.data_date
        and bal_info.area = tmp_total_statistics.area
        and bal_info.country_name = tmp_total_statistics.country_name
        and bal_info.user_type = tmp_total_statistics.user_type
where bal_info.data_date  between '2024-11-12' AND current_date

