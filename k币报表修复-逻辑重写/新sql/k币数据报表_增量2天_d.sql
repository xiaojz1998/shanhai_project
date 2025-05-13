------------------------------------------
-- file: k币数据报表_增量2天_d.sql
-- author: xiaoj
-- time: 2025/5/12 16:12
-- description:
------------------------------------------

-- 增量更新临时k币状态表
-- 包括字段
--  维度：日期、区域、国家、用户类型
--  计算字段：k币余额，k币余额日变动量，赠送币余额，购买币余额
set timezone = 'UTC-0';
drop table if exists tmp.k_coin_status_tmp;
create table tmp.k_coin_status_tmp as
-- 补全user字段表，从登陆注册表中找到相关属性
with user_info as (
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
-- 补全国家相关字段
country_info as (
        SELECT area,
            country_code,
            country_name
        FROM "v_dim_country_area"
),
-- 目前存在uid、日期 条目
exist_uid_date as (
    select
        uid,balance,recharge_balance,balance_give,order_date
    from (
    select *,row_number() over (partition by uid,order_date order by created_at desc ) rk
    from (
        SELECT uid,balance,recharge_balance,balance_give,created_at,to_timestamp(created_at)::date AS order_date
        FROM "middle_user_consume_record_00"
        union all
        SELECT uid,balance,recharge_balance,balance_give,created_at,to_timestamp(created_at)::date AS order_date
        FROM "middle_user_consume_record_01"
        union all
        SELECT uid,balance,recharge_balance,balance_give,created_at,to_timestamp(created_at)::date AS order_date
        FROM "middle_user_consume_record_02"
        union all
        SELECT uid,balance,recharge_balance,balance_give,created_at,to_timestamp(created_at)::date AS order_date
        FROM "middle_user_consume_record_03"
        union all
        SELECT uid,balance,recharge_balance,balance_give,created_at,to_timestamp(created_at)::date AS order_date
        FROM "middle_user_consume_record_04"
    ) t
    ) t0
    where rk = 1        -- 因为要用状态，所以找到每一天最晚的uid条目
),
-- 当天的k币状态
tmp_today_status as (
    select
        order_date,
        coalesce(country_info.area,'未知') as area,
        coalesce(country_info.country_name,'未知') as country_name,
        coalesce(user_info.user_type,'未知') as user_type,
        sum(balance) as balance,
        sum(balance_give) as balance_give,
        sum(recharge_balance) as recharge_balance
    from
    (select
        uid,
        '2025-05-12'::date as order_date,
        balance,
        recharge_balance,
        balance_give
    from (
    select
        uid,
        order_date,
        lag(balance) over (partition by uid order by order_date) as balance ,
        lag(recharge_balance) over (partition by uid order by order_date) as recharge_balance ,
        lag(balance_give) over (partition by uid order by order_date) as balance_give
    from (
    select
        uid,
        0 as balance,
        0 as recharge_balance,
        0 as balance_give,
        order_date
    from (select uid from exist_uid_date group by uid ) t
    left join (select '9999-12-31'::date as order_date) t0 on 1=1
    union all
    select * from exist_uid_date where order_date <= '2025-05-12'::date
    ) t1
    ) t2
    where order_date = '9999-12-31'::date
    ) t3
    left join user_info on t3.uid = user_info.uid and user_info.register_date <= t3.order_date
    left join country_info on upper(user_info.area) = country_info.country_code
    group by order_date,
            coalesce(country_info.area,'未知'),
            coalesce(country_info.country_name,'未知'),
            coalesce(user_info.user_type,'未知')
),
-- 昨日的k币状态
tmp_yesterday_status as (
    select
        order_date,
        coalesce(country_info.area,'未知') as area,
        coalesce(country_info.country_name,'未知') as country_name,
        coalesce(user_info.user_type,'未知') as user_type,
        sum(balance) as balance,
        sum(balance_give) as balance_give,
        sum(recharge_balance) as recharge_balance
    from
    (select
        uid,
        '2025-05-11'::date as order_date,
        balance,
        recharge_balance,
        balance_give
    from (
    select
        uid,
        order_date,
        lag(balance) over (partition by uid order by order_date) as balance ,
        lag(recharge_balance) over (partition by uid order by order_date) as recharge_balance ,
        lag(balance_give) over (partition by uid order by order_date) as balance_give
    from (
    select
        uid,
        0 as balance,
        0 as recharge_balance,
        0 as balance_give,
        order_date
    from (select uid from exist_uid_date group by uid ) t
    left join (select '9999-12-31'::date as order_date) t0 on 1=1
    union all
    select * from exist_uid_date where order_date <= '2025-05-11'::date
    ) t1
    ) t2
    where order_date = '9999-12-31'::date
    ) t3
    left join user_info on t3.uid = user_info.uid and user_info.register_date <= t3.order_date
    left join country_info on upper(user_info.area) = country_info.country_code
    group by order_date,
            coalesce(country_info.area,'未知'),
            coalesce(country_info.country_name,'未知'),
            coalesce(user_info.user_type,'未知')
),
-- 计算结果，主要用来计算
tmp_result as (
    select
        order_date,
        area,
        country_name,
        user_type,
        balance,
        lag(balance) over (partition by area,country_name,user_type order by order_date) as lag_1d_balance,
        balance_give,
        recharge_balance
    from (select * from tmp_today_status
    union all
    select * from tmp_yesterday_status) t
)
select
    order_date as "日期",
    area as "区域",
    country_name as "国家",
    user_type as "用户类型",
    balance as "k币余额",
    balance-lag_1d_balance as "K币余额日变动量",
    balance_give as  "赠送币余额",
    recharge_balance as "购买币余额"
from tmp_result
where order_date = '2025-05-12'::date;

-- 增量更新 k币累计和当日字段
drop table if exists tmp.k_coin_cumul_value_tmp;
create table tmp.k_coin_cumul_value_tmp as
WITH user_info as (
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
    tmp_completion as (
        select
            order_date,area,country_name,user_type
        from (select generate_series(date '2024-06-24',current_date ,'1 day')::date order_date) t
        left join (select user_type from (select user_type from user_info union all (select '未知' as user_type)) t group by user_type) t1 on 1=1
        left join (select area,country_name from(select area,country_name from country_info union all (select '未知' as area,'未知' as country_name))t group by area,country_name) t2 on 1=1
    ),
    tmp_total_user_consume_record as (
        -- 将middle_user_consume_record_00-04这几个表合并做中间表
        -- 要合并每天的uid和type的值
        select
            t.order_date,
            coalesce(country_info.area,'未知') as area,
            coalesce(country_info.country_name,'未知') as country_name,
            coalesce(user_info.user_type,'未知') as user_type,
            -- 当天充值购买的K币总数
            sum(case when type = 4 and order_id >0 then money else 0 end) as recharge_coin_num,
            -- 当天消耗的充值k币总数
            sum(case when type = 0 then use_recharge_balance else 0 end ) as use_recharge_balance_num,
            -- 赠送币累计回收数量
            sum(case when type = 9 then money else 0 end ) as balance_give_expired_num,
            ------------------------------------------------------------------------
            -- 用户累计充值赠币
            sum(case when type = 7 and order_id >0 then money else 0 end )  as recharge_coin_give_num,
            -- 用户累计签到获得赠币
            sum(case when type = 5 then money else 0 end) as sign_coin_give_num,
            -- 用户累计赠送币使用数量之和
            sum(case when type = 0 then use_balance_give else 0 end) as use_balance_give_num,
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
            -- 用户累计看剧时长任务赠币
            sum(case when type=8 and sub_type='episode_time' then money else 0 end) as episode_time_coin_give_num,
            -- 用户累计看剧任务赠币
            sum(case when type=8 and sub_type='episode_nodes' then money else 0 end) as episode_nodes_coin_give_num,
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
        left join user_info on t.uid = user_info.uid and user_info.register_date <= t.order_date
        left join country_info on upper(user_info.area) = country_info.country_code
        group by t.order_date,
                coalesce(country_info.area,'未知'),
                coalesce(country_info.country_name,'未知'),
                coalesce(user_info.user_type,'未知')
    )
    select
        *
    from (select
        t.order_date as "日期",
        t.area as "区域",
        t.country_name as "国家",
        t.user_type as "用户类型",
        -- 累计充值赠币
        sum(recharge_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row ) as "累计充值赠币",
        -- 当天充值购买的K币总数
        recharge_coin_num as "当天充值购买的K币总数",
        -- 累计赠送币回收数量
        sum(balance_give_expired_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row ) as "赠送币回收数量",
        -- 当天消耗的充值k币
        use_recharge_balance_num as "当天消耗的充值k币",
        -- 累计购买k币
        sum(recharge_coin_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row ) as "累计购买k币",
        -- 累计签到获得赠币
        sum(sign_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row ) as "累计签到赠币",
        -- 累计赠送币使用数量之和
        sum(use_balance_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "赠送币使用数量",
        -- 累计做任务赠币
        sum(total_activity_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计做任务赠币",
        -- 累计看阶梯广告赠币
        sum(ladder_ad_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计看阶梯广告赠币",
        -- 累计看firefly广告赠币
        sum(firefly_ad_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计看firefly广告赠币",
        -- 累计看adcloud广告赠币
        sum(ad_cloud_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计看adcloud广告赠币",
        -- 累计看签到广告赠币
        sum(sign_ad_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计看签到广告赠币",
        -- 累计奖励广告赠币
        sum(earn_ad_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row) as "累计奖励广告赠币",
        -- 累计看剧时长任务赠币
        sum(episode_time_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计看剧时长任务赠币",
        -- 累计看剧任务赠币
        sum(episode_nodes_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计看剧任务赠币",
        -- 累计分享赠币
        sum(app_share_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计分享赠币",
        -- 累计看特定的剧集赠送币
        sum(specific_episodes_nodes_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计看特定的剧集赠送币",
        -- 累计特定剧集时长赠送币
        sum(specific_episodes_time_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计特定剧集时长赠送币",
        -- 累计社媒活动赠币
        sum(gift_code_coin_give_num) over(partition by t.area,t.country_name,t.user_type order by t.order_date rows between unbounded preceding and current row )as "累计社媒活动赠币"
    from tmp_completion t left join tmp_total_user_consume_record t1
    on t.order_date = t1.order_date and t.area = t1.area and t.country_name = t1.country_name and t.user_type = t1.user_type ) t
    where t.日期 = '2025-05-12'::date;

-- 增量更新结果到 public.dw_coin_consume_1
delete from public.dw_coin_consume_1 where 日期 = '2025-05-12'::date;
insert into public.dw_coin_consume_1
with tmp_k_coin_cumul_value as (
    select * from tmp.k_coin_cumul_value_tmp
),
tmp_k_coin_status as (
    select * from tmp.k_coin_status_tmp
)
select
    --
    t1.日期,
    t1.区域,
    t1.国家,
    t1.用户类型,
    --
    coalesce(t2.k币余额,0) as "k币余额",
    COALESCE(t2."K币余额日变动量", 0)  AS "K币余额日变动量",
    COALESCE(t2.购买币余额, 0)  AS "购买币余额",
    COALESCE(t2.赠送币余额, 0) AS "赠送币余额",
    COALESCE(t1.赠送币回收数量, 0) AS "赠送币回收数量",
    coalesce("赠送币使用数量",0) as "赠送币使用数量",
    coalesce(t1.当天消耗的充值k币,0) AS "当天消耗的充值K币",
    coalesce("当天充值购买的K币总数", 0) as "当天充值购买的K币总数",
    coalesce(t1.累计购买k币,0) as "累计购买K币",
    coalesce("累计做任务赠币",0)+coalesce("累计充值赠币",0)
          +coalesce("累计签到赠币",0) as "累计赠送K币",
    coalesce(t1.累计购买k币,0)+coalesce("累计做任务赠币",0)+coalesce("累计充值赠币",0)
          +coalesce("累计签到赠币",0) as "累计总k币",
    coalesce("累计充值赠币",0) as "累计充值赠币",
    coalesce("累计签到赠币",0) as "累计签到赠币",
    coalesce("累计看阶梯广告赠币",0) as "累计看阶梯广告赠币",
    coalesce("累计看firefly广告赠币",0) as "累计看firefly广告赠币",
    coalesce("累计看adcloud广告赠币",0) as "累计看adcloud广告赠币",
    coalesce("累计看签到广告赠币",0) as "累计看签到广告赠币",
    coalesce("累计奖励广告赠币",0) as "累计奖励广告赠币",
    coalesce("累计看剧时长任务赠币",0) as "累计看剧时长任务赠币",
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
from tmp_k_coin_cumul_value t1
left join tmp_k_coin_status t2
on t1."日期" = t2."日期"
and t1."区域" = t2."区域"
and t1."国家" = t2."国家"
and t1."用户类型" = t2."用户类型";