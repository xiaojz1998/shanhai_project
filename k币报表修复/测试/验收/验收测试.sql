-- 验收sql
select type,sub_type,sum(money) money from
(select type,sub_type,sum(money) money  from "middle_user_consume_record_00" where to_timestamp(created_at)::date<='2025-03-30'  group by type,sub_type
union all
select type,sub_type,sum(money) money  from "middle_user_consume_record_01" where to_timestamp(created_at)::date<='2025-03-30'  group by type,sub_type
union all
select type,sub_type,sum(money) money from "middle_user_consume_record_02" where to_timestamp(created_at)::date<='2025-03-30' group by type,sub_type
union all
select type,sub_type,sum(money) money from "middle_user_consume_record_03" where to_timestamp(created_at)::date<='2025-03-30' group by type,sub_type
union all
select type,sub_type,sum(money) money from "middle_user_consume_record_04" where to_timestamp(created_at)::date<='2025-03-30'  group by type,sub_type)tt
group by type,sub_type;

-- 我的测试sql
-- bi自动聚合没有问题
select
    sum(累计奖励广告赠币)
from dw_coin_consume_1
where 日期 = '2025-03-30';

-- 找到consume table 最小时间 2024-06-24
select
    min(order_date)
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
FROM "middle_user_consume_record_02"
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
where type=5 or type=8 or type = 0 or type = 7) t;

-- 从earn_ad 入手看看是什么问题
with user_info as (
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
    )
--  select * from tmp_completion where  area = '未知' and country_name = '未知' and user_type is null
-- 436554970 为何少了一个亿？
-- 因为少union了一张表
-- TODO:
-- 532104140 少1千万 正确结果：545059320
-- 原因：left join 出错

-- tmp_completion 维度条数   202911
--  select count(*) from tmp_completion
-- left join前   维度条数    83675
-- 交集          维度条数     83296
-- 差集          维度条数     0
-- left join后  只有83019个被join上了

-- select
--     count(*)
-- from (select
--     t.order_date,
--     coalesce(country_info.area,'未知') as area ,
--     coalesce(country_info.country_name,'未知') as country_name,
--     user_info.user_type
-- from (SELECT uid,
--     order_id,
--     money,
--     to_timestamp(created_at)::date AS order_date,
--     type,
--     sub_type
-- FROM "middle_user_consume_record_00"
-- where type=5 or type=8 or type = 0 or type = 7
-- union all
-- SELECT uid,
--     order_id,
--     money,
--     to_timestamp(created_at)::date AS order_date,
--     type,
--     sub_type
-- FROM "middle_user_consume_record_01"
-- where type=5 or type=8 or type = 0 or type = 7
-- union all
-- SELECT uid,
--     order_id,
--     money,
--     to_timestamp(created_at)::date AS order_date,
--     type,
--     sub_type
-- FROM "middle_user_consume_record_02"
-- where type=5 or type=8 or type = 0 or type = 7
-- union all
-- SELECT uid,
--     order_id,
--     money,
--     to_timestamp(created_at)::date AS order_date,
--     type,
--     sub_type
-- FROM "middle_user_consume_record_03"
-- where type=5 or type=8 or type = 0 or type = 7
-- union all
-- SELECT uid,
--     order_id,
--     money,
--     to_timestamp(created_at)::date AS order_date,
--     type,
--     sub_type
-- FROM "middle_user_consume_record_04"
-- where type=5 or type=8 or type = 0 or type = 7) t
-- left join user_info on t.uid = user_info.uid and user_info.register_date <= t.order_date
-- left join country_info on upper(user_info.area) = country_info.country_code
-- group by t.order_date,coalesce(country_info.area,'未知'),coalesce(country_info.country_name,'未知'),user_info.user_type
-- except
-- select * from tmp_completion) t

-- 测试left join 前条数的sql
-- select
--     count(*)
-- from (select
--     t.order_date,
--     coalesce(country_info.area,'未知') as area ,
--     coalesce(country_info.country_name,'未知') as country_name,
--     coalesce(user_info.user_type,'未知') as user_type
-- from (SELECT uid,
--     order_id,
--     money,
--     to_timestamp(created_at)::date AS order_date,
--     type,
--     sub_type
-- FROM "middle_user_consume_record_00"
-- where type=5 or type=8 or type = 0 or type = 7
-- union all
-- SELECT uid,
--     order_id,
--     money,
--     to_timestamp(created_at)::date AS order_date,
--     type,
--     sub_type
-- FROM "middle_user_consume_record_01"
-- where type=5 or type=8 or type = 0 or type = 7
-- union all
-- SELECT uid,
--     order_id,
--     money,
--     to_timestamp(created_at)::date AS order_date,
--     type,
--     sub_type
-- FROM "middle_user_consume_record_02"
-- where type=5 or type=8 or type = 0 or type = 7
-- union all
-- SELECT uid,
--     order_id,
--     money,
--     to_timestamp(created_at)::date AS order_date,
--     type,
--     sub_type
-- FROM "middle_user_consume_record_03"
-- where type=5 or type=8 or type = 0 or type = 7
-- union all
-- SELECT uid,
--     order_id,
--     money,
--     to_timestamp(created_at)::date AS order_date,
--     type,
--     sub_type
-- FROM "middle_user_consume_record_04"
-- where type=5 or type=8 or type = 0 or type = 7) t
-- left join user_info on t.uid = user_info.uid and user_info.register_date <= t.order_date
-- left join country_info on upper(user_info.area) = country_info.country_code
-- group by t.order_date,coalesce(country_info.area,'未知'),coalesce(country_info.country_name,'未知'),coalesce(user_info.user_type,'未知')) t1;

-- 最终代码测试成功
select
    sum(earn_ad_coin_give_num)
from (select
    tmp_completion.order_date,
    tmp_completion.area ,
    tmp_completion.country_name,
    tmp_completion.user_type,
    sum(earn_ad_coin_give_num) over (partition by tmp_completion.area,tmp_completion.country_name,tmp_completion.user_type order by tmp_completion.order_date rows between unbounded preceding and current row) as earn_ad_coin_give_num
from tmp_completion left join (
select
    t.order_date,
    coalesce(country_info.area,'未知') as area ,
    coalesce(country_info.country_name,'未知') as country_name,
    coalesce(user_info.user_type,'未知') as user_type,
    sum(case when type=8 and sub_type='earn_ad' then money else 0 end ) as earn_ad_coin_give_num
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
FROM "middle_user_consume_record_02"
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
where type=5 or type=8 or type = 0 or type = 7) t
left join user_info on t.uid = user_info.uid and user_info.register_date <= t.order_date
left join country_info on upper(user_info.area) = country_info.country_code
group by t.order_date,coalesce(country_info.area,'未知'),coalesce(country_info.country_name,'未知'),coalesce(user_info.user_type,'未知')) t1
on t1.order_date = tmp_completion.order_date and t1.area = tmp_completion.area and t1.country_name = tmp_completion.country_name and t1.user_type = tmp_completion.user_type ) t2
where order_date = '2025-03-30'


