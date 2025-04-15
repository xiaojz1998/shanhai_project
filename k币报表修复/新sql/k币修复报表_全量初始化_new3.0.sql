set timezone = 'UTC-0';
-----------------------------------------------------------
-- 注入cal01
-----------------------------------------------------------
truncate table tmp.dw_coin_consume_cal01;
insert into tmp.dw_coin_consume_cal01
WITH tmp_uid_date_info as (
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
        -- 要合并每天的uid和type的值
        select
            uid,
            order_date,
            --  order_id,
            --  money,
            --  type,
            --  sub_type
            -- 用户累计充值赠币
            sum(case when type = 7 and order_id >0 then money else 0 end )  as recharge_coin_give_num,
            -- 用户累计签到获得赠币
            sum(case when type = 5 then money else 0 end) as sign_coin_give_num,
            -- 用户累计赠送币使用数量之和
            sum(case when type = 0 then money else 0 end) as use_balance_give_num,
            -- 用户累计做任务赠币
            sum(case when type = 8 then money else 0 end) as total_activity_coin_give_num,
            -- 用户累计看阶梯广告赠币
            sum(case when type=8 and sub_type='ladder_ad' then money else 0 end) as ladder_ad_coin_give_num,
            -- 用户累计看firefly广告赠币
            sum(case when type=8 and sub_type='firefly_ad' then money else 0 end) as firefly_ad_coin_give_num,
            -- 用户累计看adcloud广告赠币
            sum(case when type=8 and sub_type='ad_cloud' then money else 0 end ) as ad_cloud_coin_give_num,
            -- 用户累计看签到广告赠币
            sum(case when type=8 and (sub_type='sign_ad' or sub_type='sign_firefly_ad' or sub_type='sign_adcloud_ad') then money else 0 end) as sign_ad_coin_give_num,
            -- 用户累计奖励广告赠币
            sum(case when type=8 and sub_type='earn_ad' then money else 0 end ) as earn_ad_coin_give_num,
            -- 用户累计看剧任务赠币
            sum(case when type=8 and sub_type='episode_time' then money else 0 end) as episode_time_coin_give_num,
            -- 用户累计分享赠币
            sum(case when type=8 and (sub_type='app_share' or sub_type='video_share') then money else 0 end ) as app_share_coin_give_num,
            -- 用户累计看特定的剧集赠送币
            sum(case when type=8 and sub_type='specific_episodes_nodes' then money else 0 end) as specific_episodes_nodes_coin_give_num,
            -- 用户累计特定剧集时长赠送币
            sum(case when type=8 and sub_type='specific_episodes_time' then money else 0 end ) as specific_episodes_time_coin_give_num,
            -- 用户累计社媒活动赠币
            sum(case when type=8 and sub_type='gift_code' then money else 0 end) as gift_code_coin_give_num
        from (SELECT uid,
            order_id,
            money,
            to_timestamp(created_at)::date AS order_date,
            type,
            sub_type
        FROM "middle_user_consume_record_00"
        where type=5 or type=8 or type = 0 or type = 7
        union all
        SELECT uid,
            order_id,
            money,
            to_timestamp(created_at)::date AS order_date,
            type,
            sub_type
        FROM "middle_user_consume_record_01"
        where type=5 or type=8 or type = 0 or type = 7
        union all
        SELECT uid,
            order_id,
            money,
            to_timestamp(created_at)::date AS order_date,
            type,
            sub_type
        FROM "middle_user_consume_record_03"
        where type=5 or type=8 or type = 0 or type = 7
        union all
        SELECT uid,
            order_id,
            money,
            to_timestamp(created_at)::date AS order_date,
            type,
            sub_type
        FROM "middle_user_consume_record_04"
        where type=5 or type=8 or type = 0 or type = 7 ) t
        group by uid, order_date
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
    )
        select
            t.data_date as "日期",
            coalesce(country_info.area,'未知') as "区域",
            coalesce(country_info.country_name,'未知') as "国家",
            user_info.user_type as "用户类型",
            -- 4个维度 15个度量字段
            sum(recharge_coin_give_num) as "累计充值赠币", --充值赠送币
            sum(recharge_coin_num) as "当天充值购买的K币总数",
            sum(sign_coin_give_num) as "累计签到赠币" ,        -- 签到赠送币数量
            sum(use_balance_give_num) as "赠送币使用数量",     --赠送币使用数量
            sum(total_activity_coin_give_num) as "累计做任务赠币", --做任务赠币总数
            sum(ladder_ad_coin_give_num) as "累计看阶梯广告赠币", -- 阶梯广告赠币
            sum(firefly_ad_coin_give_num) as "累计看firefly广告赠币",
            sum(ad_cloud_coin_give_num) as "累计看adcloud广告赠币",
            sum(sign_ad_coin_give_num) as "累计看签到广告赠币",
            sum(earn_ad_coin_give_num) as "累计奖励广告赠币",
            sum(episode_time_coin_give_num) as "累计看剧任务赠币", --累计看剧任务赠币
            sum(app_share_coin_give_num) as "累计分享赠币",       --累计分享赠币
            sum(specific_episodes_nodes_coin_give_num) as "累计看特定的剧集赠送币", --累计看特定的剧集赠送币
            sum(specific_episodes_time_coin_give_num) as "累计特定剧集时长赠送币", --累计特定剧集时长赠送币
            sum(gift_code_coin_give_num) as "累计社媒活动赠币" -- 累计社媒赠币
        from (-- 用户在date_date之前的不同类型赠币以及累计使用赠币统计
        -- 13个度量字段
        select
            tmp_uid_date_info.uid,
            tmp_uid_date_info.data_date,
            -- 用户累计充值赠币
            sum(recharge_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row ) as recharge_coin_give_num,
            -- 用户累计签到获得赠币
            sum(sign_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row ) as sign_coin_give_num,
            -- 用户累计赠送币使用数量之和
            sum(use_balance_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as use_balance_give_num,
            -- 用户累计做任务赠币
            sum(total_activity_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as total_activity_coin_give_num,
            -- 用户累计看阶梯广告赠币
            sum(ladder_ad_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as ladder_ad_coin_give_num,
            -- 用户累计看firefly广告赠币
            sum(firefly_ad_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as firefly_ad_coin_give_num,
            -- 用户累计看adcloud广告赠币
            sum(ad_cloud_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as ad_cloud_coin_give_num,
            -- 用户累计看签到广告赠币
            sum(sign_ad_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as sign_ad_coin_give_num,
            -- 用户累计奖励广告赠币
            sum(earn_ad_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row) as earn_ad_coin_give_num,
            -- 用户累计看剧任务赠币
            sum(episode_time_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as episode_time_coin_give_num,
            -- 用户累计分享赠币
            sum(app_share_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as app_share_coin_give_num,
            -- 用户累计看特定的剧集赠送币
            sum(specific_episodes_nodes_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as specific_episodes_nodes_coin_give_num,
            -- 用户累计特定剧集时长赠送币
            sum(specific_episodes_time_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as specific_episodes_time_coin_give_num,
            -- 用户累计社媒活动赠币
            sum(gift_code_coin_give_num) over(partition by tmp_uid_date_info.uid order by data_date rows between unbounded preceding and current row )as gift_code_coin_give_num
        from tmp_uid_date_info
        left join tmp_total_user_consume_record
        on tmp_uid_date_info.uid = tmp_total_user_consume_record.uid
        and tmp_total_user_consume_record.order_date = tmp_uid_date_info.data_date) t
        left join user_info on t.uid = user_info.uid and user_info.register_date <= t.data_date
        left join country_info on upper(user_info.area) = country_info.country_code
        left join recharge_coin_info on t.uid = recharge_coin_info .uid
        and recharge_coin_info.data_date = t.data_date
        group by  t.data_date,
                  coalesce(country_info.area,'未知'),
                  coalesce(country_info.country_name,'未知'),
                  user_info.user_type;
-----------------------------------------------------------
-- 注入cal02
-----------------------------------------------------------
truncate table tmp.dw_coin_consume_cal02;
insert into tmp.dw_coin_consume_cal02
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
            coalesce(k1.area,'未知'),
            coalesce(k1.country_name,'未知'),
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
            k1.user_type;
-----------------------------------------------------------
-- 合并cal01和cal02 到结果dw_coin_consume_1
-----------------------------------------------------------
truncate table public.dw_coin_consume_1;
insert into public.dw_coin_consume_1
with cal01 as (
    select
        *
    from tmp.dw_coin_consume_cal01
), cal02 as (
    select
        *
    from tmp.dw_coin_consume_cal02
)
select
    --
    cal02.data_date as "日期",
    cal02.area as "区域",
    cal02.country_name as "国家",
    cal02.user_type as "用户类型",
    --
    coalesce(balance,0) as "k币余额",
    COALESCE(balance, 0) - COALESCE(lag_1d_balance, 0) AS "K币余额日变动量",
    COALESCE(balance, 0) - COALESCE(balance_give, 0) AS "购买币余额",
    COALESCE(balance_give, 0) AS "赠送币余额",
    COALESCE(balance_give_expired, 0) AS "赠送币回收数量",
    coalesce("赠送币使用数量",0) as "赠送币使用数量",
    (COALESCE(lag_1d_balance, 0) - COALESCE(lag_1d_balance_give, 0))
          + COALESCE("当天充值购买的K币总数", 0) - (COALESCE(balance, 0) - COALESCE(balance_give, 0)
      ) AS "当天消耗的充值K币",
    coalesce("当天充值购买的K币总数", 0) as "当天充值购买的K币总数",
    coalesce(recharge_coin,0) as "累计购买K币",
    coalesce("累计做任务赠币",0)+coalesce("累计充值赠币",0)
          +coalesce("累计签到赠币",0) as "累计赠送K币",
    coalesce(recharge_coin,0)+coalesce("累计做任务赠币",0)+coalesce("累计充值赠币",0)
          +coalesce("累计签到赠币",0) as "累计总k币",
    coalesce("累计充值赠币",0) as "累计充值赠币",
    coalesce("累计签到赠币",0) as "累计签到赠币",
    coalesce("累计看阶梯广告赠币",0) as "累计看阶梯广告赠币",
    coalesce("累计看firefly广告赠币",0) as "累计看firefly广告赠币",
    coalesce("累计看adcloud广告赠币",0) as "累计看adcloud广告赠币",
    coalesce("累计看签到广告赠币",0) as "累计看签到广告赠币",
    coalesce("累计奖励广告赠币",0) as "累计奖励广告赠币",
    coalesce("累计看剧任务赠币",0) as "累计看剧任务赠币",
    coalesce("累计分享赠币",0) as "累计分享赠币",
    coalesce("累计看特定的剧集赠送币",0) as "累计看特定的剧集赠送币",
    coalesce("累计特定剧集时长赠送币",0) as "累计特定剧集时长赠送币",
    coalesce("累计社媒活动赠币",0) as "累计社媒活动赠币",
    coalesce("累计做任务赠币",0)-coalesce("累计看阶梯广告赠币",0)-coalesce("累计看firefly广告赠币",0)
          -coalesce("累计看adcloud广告赠币",0)-coalesce("累计看签到广告赠币",0)
          -coalesce("累计看剧任务赠币",0)-coalesce("累计分享赠币",0)
          -coalesce("累计看特定的剧集赠送币",0)-coalesce("累计特定剧集时长赠送币",0)
          -coalesce("累计社媒活动赠币",0) as "累计其他任务赠币"
from cal02
left join cal01
on cal02.data_date = cal01."日期"
and cal02.area = cal01."区域"
and cal02.country_name = cal01."国家"
and cal02.user_type = cal01."用户类型";