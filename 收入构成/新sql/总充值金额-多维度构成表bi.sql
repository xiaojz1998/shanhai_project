------------------------------------------
-- file: 总充值金额-多维度构成表bi.sql
-- author: xiaoj
-- time: 2025/5/19 15:04
-- description:
------------------------------------------

with tmp_total as (
    select
        t.d_date
        , sum(money) as "当日总充值金额"
    from public.dws_order_recharge_all_dimension_stat_di t
    group by t.d_date
)
select
    t.d_date as "日期"
    , area as "用户区域"
    , country_name as "用户国家"
    , country_level as "用户T级国家"
    , u_lang_name as "用户语言"
    , reg_date as "用户注册日期"
    , case when reg_days = 0 then '当天注册'
           when reg_days >= 1 and reg_days<= 2 then '注册1-2天'
           when reg_days >= 3 and reg_days<= 4 then '注册3-4天'
           when reg_days >= 5 and reg_days<= 7 then '注册5-7天'
           when reg_days >= 8 and reg_days<= 14 then '注册8-14天'
           when reg_days >= 15 and reg_days<= 30 then '注册15-30天'
           when reg_days >= 31 and reg_days<= 90 then '注册31-90天'
           when reg_days >= 91 and reg_days<= 180 then '注册91-180天'
           when reg_days >= 181 and reg_days<= 365 then '注册181-365天'
           else '注册365天以上'
               end as "用户注册天数"
    , ad_channel as "用户来源渠道"
    , vid as "剧id"
    , vname as "剧名称"
    , display_date as "剧首次上架日期"
    , substr(display_date,1,7) as "剧首次上架月份"
    , v_lang_name as "剧语言"
    , v_source_name as "剧目来源"
    , v_category_name as "剧目类别"
    , v_complex_name as "综合"
    , v_type_name as "作品分类"
    , order_type as "订单类型"
    , entrance as "下单入口"
    , popup_entrance as "支付入口"
    , os as "下单应用平台"
    , money as "总充值金额"
    ,  t1."当日总充值金额" as "当日总充值金额"
from dws_order_recharge_all_dimension_stat_di t
left join tmp_total t1 on t.d_date = t1.d_date