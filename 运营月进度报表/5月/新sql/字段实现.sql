------------------------------------------
-- file: 字段实现.sql
-- author: xiaoj
-- time: 2025/5/9 15:52
-- description:
------------------------------------------

-- 实现 新用户累计充值 用于新用户累计roi计算
,tmp_pay as (
    select
        d_date,
        country_code,
        lang,
        sum(case when d_date::date between (p_date::date+interval'- 7 d')::date and p_date::date  and (d_date::date+interval' 7 d')<=(current_date+interval'-1 d') then new_pay_amt else null end)::decimal(20,2) as pay_7
    from (
    select
        t1.d_date,
        t2.d_date as p_date,
        coalesce(t1.country_code,'UNKNOWN') as country_code,
        coalesce(t1.lang,'UNKNOWN') as lang,
        sum(t2.pay_amt)::decimal(20,2) as new_pay_amt
    from new_reg_users t1
    left join (
        -- 每日用户充值金额
        select
            d_date::date as d_date
             ,uid
            ,sum(pay_amt) as pay_amt -- 新用户充值金额（未减退款，与指标概览保持一致）
            from(
                -- 每日用户订单统计
                select
                to_char( to_timestamp(created_at),'YYYY-MM-DD')as d_date
                ,uid
                ,sum(case when o.status = 1 then o.money*0.01 else 0 end) as  pay_amt  -- 成功充值金额
                from public.all_order_log o                                 -- 用户订单表
                where o.environment = 1 and o.os in('android','ios')
                and created_date>=20240701
                group by
                to_char( to_timestamp(created_at),'YYYY-MM-DD')
                ,uid
            )a
            group by d_date,uid
    ) t2 on t1.uid = t2.uid and t1.d_date <= t2.d_date
    group by t1.d_date,t2.d_date, coalesce(t1.country_code,'UNKNOWN'), coalesce(t1.lang,'UNKNOWN')) t3
    group by d_date,country_code, lang
)