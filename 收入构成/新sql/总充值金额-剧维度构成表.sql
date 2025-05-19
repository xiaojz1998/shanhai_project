------------------------------------------
-- file: 总充值金额-剧维度构成表.sql
-- author: xiaoj
-- time: 2025/5/19 18:33
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
    , vid as "剧id"
    , vname as "剧名称"
    , display_date as "剧首次上架日期"
    , substr(display_date,1,7) as "剧首次上架月份"
    , v_lang_name as "剧语言"
    , v_source_name as "剧目来源"
    , v_category_name as "剧目类别"
    , v_complex_name as "综合"
    , v_type_name as "作品分类"
    , sum(money) as "总充值金额"
    , max(t1."当日总充值金额")as "当日总充值金额"
from dws_order_recharge_all_dimension_stat_di t
left join tmp_total t1 on t.d_date = t1.d_date
group by t.d_date,vid, vname, display_date, display_date, v_lang_name, v_source_name, v_category_name, v_complex_name, v_type_name