---------------------------------------------
-- File: 分语言收入转化和留存数据.sql
-- Time: 2025/6/20 16:52
-- User: xiaoj
-- Description:  
---------------------------------------------

with t27 as (
    SELECT
        uid
        , min("created_date") as first_pay_date
    FROM  public.all_order_log --找出付费用户第一个付费日
    where "environment" = 1 --生产环境
        and "status"  = 1 --充值成功（没减去退款用户
        AND created_date BETWEEN 20000101 AND 20991231
    GROUP BY uid
)
, t28 as (
    SELECT
        uid
        , CASE WHEN (CURRENT_DATE - INTERVAL '1 day') = TO_DATE(first_pay_date::text, 'YYYYMMDD') THEN 1
               ELSE 0
            END AS is_first_pay  -- 计算是否为昨天的首充
    FROM t27
)
, t29 as (
    SELECT
        uid
        , min("created_date") as first_k_pay_date
    FROM  public.all_order_log --找出K币付费用户第一个付费日
    where "environment" = 1 --生产环境
        and "status"  = 1 --充值成功（没减去退款用户）
        and "coin" > 0
        AND created_date BETWEEN 20000101 AND 20991231
    GROUP BY uid
)
, t30 as (
    SELECT uid,
        CASE WHEN (CURRENT_DATE - INTERVAL '1 day') = TO_DATE(first_k_pay_date::text, 'YYYYMMDD') THEN 1
            ELSE 0
            END AS is_first_k_pay  -- 计算是否为昨天的首充
    FROM t29
)
, t31 as (
    SELECT
        uid
        , min("created_date") as first_sub_pay_date
    FROM  public.all_order_log --找出订阅付费用户第一个付费日
    where "environment" = 1 --生产环境
        and "status"  = 1 --充值成功（没减去退款用户）
        and order_type in (4,5,6)
        AND created_date BETWEEN 20000101 AND 20991231
        --and "coin" = 0
    GROUP BY uid
)
, t32 as (
    SELECT
        uid
        , CASE WHEN (CURRENT_DATE - INTERVAL '1 day') = TO_DATE(first_sub_pay_date::text, 'YYYYMMDD') THEN 1
            ELSE 0
            END AS is_first_sub_pay  -- 计算是否为昨天的订阅首充
    FROM t31
)
, t1 as (
    select
        distinct uid , created_date
    from public.all_order_log  -- 选取昨天只充值K币的首充用户
    where "environment" = 1 --生产环境
        and "coin"  > 0 --充值金币数大于0
        and "status"  = 1 --充值成功（没减去退款用户）
        and TO_DATE(created_date::text, 'YYYYMMDD') = CURRENT_DATE - INTERVAL '1 day'
        AND created_date BETWEEN 20000101 AND 20991231
        and uid in (select uid from t30 where is_first_k_pay = 1)
)
, t2 as (
    select
        COUNT(*) as new_k_pay_users
        , CASE WHEN lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE lang_name
            END AS lang_name
    from t1  -- 合并繁体和简体中文 lang_name from t1  --首充K币用户数量按语言分类
    INNER JOIN public.dwd_user_info u on t1.uid = u.uid::bigint
    GROUP BY CASE WHEN lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE lang_name
                END
)
, t3 as (
    select
        sum(dau) as dau
        , CASE WHEN lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE lang_name
            END AS lang_name
    from  public.dw_operate_view a
    where  TO_DATE(a.d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY CASE WHEN lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE lang_name
                END
)
, t4 as (
    select
        new_k_pay_users
        , dau
        , CASE WHEN t2.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE t2.lang_name
            END AS lang_name
    from t2 -- 全连接DAU表和首充K币表
    FULL OUTER JOIN  t3 on t2.lang_name = t3.lang_name
)
, t5 as (
    select distinct uid, created_date
    from public.all_order_log --首充订阅会员用户
    where "environment" = 1 --生产环境
        and "status"  = 1 --充值成功（没减去退款用户）
        and order_type in (4,5,6) --是订阅会员用户
        AND created_date BETWEEN 20000101 AND 20991231
        and TO_DATE(created_date::text, 'YYYYMMDD') = CURRENT_DATE - INTERVAL '1 day'
        and uid in (select uid from t32 where is_first_sub_pay = 1) --第一次充值在昨日
)
, t6 as (
    select
        COUNT(*) as new_vip_pay_users
        , CASE WHEN u.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE u.lang_name
            END AS lang_name
    from t5  --首充订阅会员用户数量按语言分类
    INNER JOIN public.dwd_user_info u on t5.uid = u.uid::bigint
    GROUP BY CASE WHEN u.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE u.lang_name
            END
)
, t7 as (
    select
        sum(pay_amt+ad_income_amt) as "总收入(含商广)"
        , CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE a.lang_name
            END AS lang_name  --总充值金额=(充值+商业广告收入)
    from public.dw_operate_view a
    where  TO_DATE(d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE a.lang_name
            END
)
, t8 as (
    select
        CASE WHEN SUM((all_pay_user))  = 0 THEN 0
            ELSE COALESCE(SUM((pay_user*1.0)) / SUM(all_pay_user * 1.0),0)
            END as "订单转化率：UV"
        , CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE a.lang_name
            END AS lang_name
    from public.dw_operate_view a
    where  TO_DATE(a.d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE a.lang_name
                END
)
, t9 as (
    select
        case when sum(due_user)=0 then 0.00
            else 1.0*sum(repay_user)/sum(due_user)
            end as "续订率"
        , CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE a.lang_name
            END AS lang_name
    from public.dw_operate_view a
    where  TO_DATE(a.d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE a.lang_name
                END
)
, t10 as (
    select
        CASE WHEN  SUM(pay_user_dau) = 0 THEN 0
            ELSE SUM(pay_user_2login) * 1.0 / SUM(pay_user_dau) * 1.0
            END as "总付费次留"
        , CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE a.lang_name
            END AS lang_name
    from public.dw_operate_view a
    where  TO_DATE(a.d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '2 day' --注意昨日次留数据需完整取自前天，即-2日
    GROUP BY CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE a.lang_name
                END
)
, t11 as (
    select
        CASE WHEN  SUM(new_pay_user_dau) = 0 THEN 0
            ELSE SUM(new_pay_user_2login) * 1.0 / SUM(new_pay_user_dau) * 1.0
            END as "新用户付费次留"
        , CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE a.lang_name
            END AS lang_name
    from public.dw_operate_view a
    where  TO_DATE(a.d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '2 day'--注意昨日次留数据需完整取自前天，即-2日
    GROUP BY CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE a.lang_name
                END
)
, t12 as (
    select
        CASE WHEN  SUM(old_pay_user_dau) = 0 THEN 0
            ELSE SUM(old_pay_user_2login) * 1.0 / SUM(old_pay_user_dau) * 1.0
            END as "老用户付费次留"
        , CASE  WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE a.lang_name
            END AS lang_name
    from public.dw_operate_view a
    where  TO_DATE(a.d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '2 day'--注意昨日次留数据需完整取自前天，即-2日
    GROUP BY (CASE
                WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE a.lang_name
            END)
)
, t13 as (
    select
        CASE WHEN  SUM(dau) = 0 THEN 0
            ELSE SUM(dau_2login) * 1.0 / SUM(dau) * 1.0
            END as "总次留"
        , CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE a.lang_name
            END AS lang_name
    from public.dw_operate_view a
    where  TO_DATE(a.d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '2 day'--注意昨日次留数据需完整取自前天，即-2日
    GROUP BY (CASE
                WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE a.lang_name
            END)
)
, t14 as (
    select
        CASE WHEN  SUM(new_dau) = 0 THEN 0
            ELSE SUM(new_dau_2login) * 1.0 / SUM(new_dau) * 1.0
            END as "新用户次留"
        , CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE a.lang_name
            END AS lang_name
    from public.dw_operate_view a
    where  TO_DATE(a.d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '2 day'--注意昨日次留数据需完整取自前天，即-2日
    GROUP BY (CASE
                WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE a.lang_name
            END)
)
, t15 as (
    select
        CASE WHEN  SUM(old_dau) = 0 THEN 0
            ELSE SUM(old_dau_2login) * 1.0 / SUM(old_dau) * 1.0
            END as "老用户次留"
        , CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE a.lang_name
                END AS lang_name
    from public.dw_operate_view a
    where  TO_DATE(a.d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '2 day'--注意昨日次留数据需完整取自前天，即-2日
    GROUP BY (CASE
                WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE a.lang_name
            END)
)
, t16 as (
    select
        CASE WHEN SUM((all_pay_order))  = 0 THEN 0
            ELSE COALESCE(SUM((pay_order*1.0)) / SUM(all_pay_order * 1.0),0)
            END as "订单转化率"
        , CASE WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE a.lang_name
            END AS lang_name
    from public.dw_operate_view a
    where  TO_DATE(a.d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY (CASE
                WHEN a.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE a.lang_name
            END)
)
, t17 as (
    select
        *   -- 注册
    from (
        select
            TO_TIMESTAMP(created_at) :: date register_date
            -- , case when upper(area) = 'ID' then '印尼'
            --     when upper(area)  = 'KR' then '韩国'
            --     when upper(area)  = 'JP' then '日本'
            --     when upper(area)  = 'ES' then '西班牙'
            --     when upper(area)  = 'CN' then '中国(简体)'
            --   else '英语国家' end as area
            , b.area as area
            , b.country_name as 国家
            , if(os = 'ios', 'IOS', 'Android') as 系统
            , campaign_id
            , uid
            , event
            , row_number() over(partition by uid order by created_at) rk  -- 可能存在多次归因，以第一次为准
        FROM "user_log"  a left join v_dim_country_area b on upper(a.area)= b.country_code
        where 1 =1
            and event = 1
            and created_date >= 20240701
    ) a
    where rk = 1
)
, t18 as (
    select
        TO_TIMESTAMP(created_at) :: date active_date
        , uid  -- 活跃表
    from app_user_track_log
    where true
        and event in (1,16)
        and created_date >= 20240904
        group by TO_TIMESTAMP(created_at) :: date,uid

    union

    select
        TO_TIMESTAMP(created_at) :: date active_date
        , uid
    from user_log
    where true
        and created_date >= 20240801
    group by TO_TIMESTAMP(created_at) :: date,uid
)
, t19 as (
    select
        active_date
        ,COALESCE(register_date,'2024-07-01') register_date
        , COALESCE(area,'未知') area
        , COALESCE(国家,'未知') 国家
        , COALESCE(系统,'Android') 系统
        , COALESCE(campaign_id,'0') campaign_id
        , t18.uid
    from t18 left join t17 on t17.uid = t18.uid
)
, t20 as (
    select
        CASE WHEN u.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE u.lang_name
            END AS lang_name
        ,a.active_date
        , count(distinct a.uid) DAU
        , count(distinct case when b.active_date - a.active_date = 3 then b.uid else null end) 总3日留存
        , concat(cast(coalesce(round((count(distinct case when b.active_date - a.active_date = 3 then b.uid else null end) * 1.0 /count(distinct a.uid) * 1.0 *100), 2), 0.00) as varchar), '%') as "总3留率"
    FROM t19 a
    left join t19 b on a.uid = b.uid and b.active_date > a.active_date
    INNER JOIN public.dwd_user_info u on a.uid = u.uid::bigint
    where a.active_date = CURRENT_DATE - INTERVAL '4 day'
    group by a.active_date
           , (CASE
                WHEN u.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE u.lang_name
            END)
)
, t21 as (
    select
        CASE WHEN u.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE u.lang_name
            END as lang_name
        , a.active_date
        , count(distinct a.uid) DAU
        , count(distinct case when b.active_date - a.active_date = 7 then b.uid else null end) 总7日留存
        , concat(cast(coalesce(round((count(distinct case when b.active_date - a.active_date = 7 then b.uid else null end) * 1.0 /count(distinct a.uid) * 1.0 *100), 2), 0.00) as varchar), '%') as "总7留率"
    FROM t19 a
    left join t19 b on a.uid = b.uid and b.active_date > a.active_date
    INNER JOIN public.dwd_user_info u on a.uid = u.uid::bigint
    where a.active_date = CURRENT_DATE - INTERVAL '8 day'
    group by a.active_date
           , (CASE
                WHEN u.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE u.lang_name
            END)
)
, t22 as (
    select
        CASE WHEN u.lang_name IN ('简体中文', '繁体中文') THEN '中文'
            ELSE u.lang_name
            END "lang_name"
        , a.active_date
        , count(distinct a.uid) DAU
        , count(distinct case when b.active_date - a.active_date = 30 then b.uid else null end) 总30日留存
        , concat(cast(coalesce(round((count(distinct case when b.active_date - a.active_date = 30 then b.uid else null end) * 1.0 /count(distinct a.uid) * 1.0 *100), 2), 0.00) as varchar), '%') as "总30留率"
    FROM t19 a
    left join t19 b on a.uid = b.uid and b.active_date > a.active_date
    INNER JOIN public.dwd_user_info u on a.uid = u.uid::bigint
    where a.active_date = CURRENT_DATE - INTERVAL '31 day'
    group by a.active_date
           , (CASE
                WHEN u.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE u.lang_name
            END)
)
, t23 as (
    select
        t20.lang_name
        , "总3留率"
        , "总7留率"
        , "总30留率"
    from t20
    inner join t21 on t20.lang_name = t21.lang_name
    inner join t22 on t20.lang_name = t22.lang_name
)
, t24 as (
    select
        distinct created_date , uid
    from public.all_order_log o
    where uid in (select uid from t28 where is_first_pay = 1) --昨日是第一天充值的用户
        and "environment"  = 1
        and "status"  = 1
        and TO_DATE(o."created_date"::TEXT, 'YYYYMMDD') = CURRENT_DATE - INTERVAL '1 day'
)
, t25 as (
    select
        COUNT(*) as pay_again_users_num
        , (CASE
                WHEN u.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE u.lang_name
            END) as lang_name
    from t24  --复充会员用户数量按语言分类（第二次及以上）
    INNER JOIN public.dwd_user_info u on t24.uid = u.uid::bigint
    GROUP BY (CASE
                WHEN u.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE u.lang_name
            END)
)
, t26 as (
    select
        sum(pay_user) as pay_user_num
        , (CASE
                WHEN lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE lang_name
            END) as lang_name
    from public.dw_operate_view
    where TO_DATE(d_date, 'YYYY-MM-DD') = CURRENT_DATE - INTERVAL '1 day'
    group by (CASE
                WHEN lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE lang_name
            END)
)
, t33 as (
    SELECT
        COUNT(DISTINCT a.uid) as pay_again_users_num
        , (CASE       -----老充值用户在昨日复充人数
                WHEN c.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE c.lang_name
            END) AS lang_name
    FROM all_order_log a
    INNER JOIN (
        SELECT
            uid
        FROM all_order_log
        WHERE environment = 1
            AND status = 1
        GROUP BY uid
        HAVING COUNT(order_num) >= 2  -- 复充用户
    ) b ON a.uid = b.uid
    INNER JOIN (
        SELECT DISTINCT uid, lang_name
        FROM public.dwd_user_info
    ) c ON b.uid = c.uid::BIGINT  -- 确保数据类型匹配
    WHERE a.environment = 1
        AND a.status = 1
        AND a.created_date = TO_CHAR(CURRENT_DATE - INTERVAL '1 day', 'YYYYMMDD')::INTEGER -- 限制订单日期
    GROUP BY (CASE
                WHEN c.lang_name IN ('简体中文', '繁体中文') THEN '中文'
                ELSE c.lang_name
            END)
)
select
    t3.lang_name as 语言,
    t3.dau as DAU, --DAU
    t7."总收入(含商广)" as  总收入,
    concat(cast(coalesce(round(t16."订单转化率" *100, 2), 0.00) as varchar), '%') as 订单转化率,
    --concat(cast(coalesce(round(t8."订单转化率：UV" *100, 2), 0.00) as varchar), '%') as 订单转化率：UV, --无需UV订单转化率
    concat(cast(coalesce(round((t2.new_k_pay_users*1.0)/t3.dau*100, 2), 0.00) as varchar), '%') as 首次充值率K币, --首次充值率(K币)
    concat(cast(coalesce(round((t6.new_vip_pay_users*1.0)/t3.dau*100, 2), 0.00) as varchar), '%') as 首次订阅率,--首次订阅率(用户注册以来在昨天第一次充值K币的用户数/昨天的活跃用户数)
    concat(cast(coalesce(round((t33.pay_again_users_num*1.0)/t26.pay_user_num*100, 2), 0.00) as varchar), '%') as 复充率,--复充率(用户注册以来在昨天再次充值的用户数/昨天总充值用户数)
    concat(cast(coalesce(round(t9."续订率" *100, 2), 0.00) as varchar), '%') as 续订率,
    concat(cast(coalesce(round(t10."总付费次留" *100, 2), 0.00) as varchar), '%') as 总付费次留,
    concat(cast(coalesce(round(t11."新用户付费次留" *100, 2), 0.00) as varchar), '%') as 新用户付费次留,
    concat(cast(coalesce(round(t12."老用户付费次留" *100, 2), 0.00) as varchar), '%') as 老用户付费次留,
    concat(cast(coalesce(round(t13."总次留" *100, 2), 0.00) as varchar), '%') as 总次留,
    concat(cast(coalesce(round(t14."新用户次留" *100, 2), 0.00) as varchar), '%') as 新用户次留,
    concat(cast(coalesce(round(t15."老用户次留" *100, 2), 0.00) as varchar), '%') as 老用户次留,
    t23."总3留率" as 总3留率,
    t23."总7留率" as 总7留率,
    t23."总30留率" as 总30留率
from t4
left JOIN t3
on t3.lang_name = t4.lang_name
FULL OUTER JOIN t2
ON t2.lang_name = t4.lang_name
FULL OUTER JOIN t6
on t6.lang_name = t4.lang_name
left JOIN t7
on t7.lang_name = t4.lang_name
--left JOIN t8
--on t8.lang_name = t4.lang_name
left JOIN t9
on t9.lang_name = t4.lang_name
LEFT JOIN t10
on t10.lang_name = t4.lang_name
LEFT JOIN t11
on t11.lang_name = t4.lang_name
LEFT JOIN t12
on t12.lang_name = t4.lang_name
LEFT JOIN t13
on t13.lang_name = t4.lang_name
LEFT JOIN t14
on t14.lang_name = t4.lang_name
LEFT JOIN t15
on t15.lang_name = t4.lang_name
LEFT JOIN t16
on t16.lang_name = t4.lang_name
inner JOIN t23
on t23.lang_name = t4.lang_name
LEFT JOIN t33
on t33.lang_name = t4.lang_name
LEFT JOIN t26
on t26.lang_name = t4.lang_name
where t3.lang_name is not null