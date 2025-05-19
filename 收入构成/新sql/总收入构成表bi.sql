------------------------------------------
-- file: 总收入构成表bi.sql
-- author: xiaoj
-- time: 2025/5/16 11:19
-- description:
------------------------------------------

-- 已经完成，直接用在bi上
with tmp_total as (
    select
        t.d_date
        , sum(pay_amt) as "当日总充值金额"
        , sum(ad_income_amt) as "当日广告收入"
        , sum(pay_refund_amt) as "当日退款金额"
    from public.dw_operate_view t
    group by t.d_date
)
, tmp_t0 as (
    select
    -- 分组维度
        t.d_date
        , t.country_code
        , t.country_name
        , t.area
        , coalesce(country_grade,'未分类') as  country_grade
        -- 计算字段
        , sum(pay_amt) + sum(ad_income_amt) as "总收入"
        , sum(pay_amt) as "总充值金额"
        , sum(ad_income_amt) as "广告收入"
        , sum(pay_refund_amt) as "退款金额"
    from public.dw_operate_view t
    left join "v_dim_country_area" t0 on t.country_code = t0.country_code
    where t.d_date::date  <= current_date - 1
    group by t.d_date,t.country_code,t.country_name,t.area,coalesce(country_grade,'未分类')
)
select
    t.d_date
    , t.country_code
    , t.country_name
    , t.area
    , country_grade
    -- 计算字段
    , "总收入"
    , "总充值金额"
    , "广告收入"
    , "退款金额"
    , "当日总充值金额"
    , "当日广告收入"
    , "当日退款金额"
from tmp_t0 t
left join tmp_total t0 on t.d_date = t0.d_date