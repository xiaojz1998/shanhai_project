-- 广告汇总数据
select
    d_date as 日期,
    area as 区域,
    country_name as 国家,
    lang_name as 语言,
    os as 应用平台,
    total_ad_click_uv as 广告总观看人数,
    total_ad_click_pv as 广告总观看次数,
    total_checkin_ad_click_uv as 签到广告观看人数,
    total_checkin_ad_click_pv as 签到广告观看次数,
    total_ladder_ad_click_uv as 阶梯广告观看人数,
    total_ladder_ad_click_pv as 阶梯广告观看次数,
    total_unlock_ad_click_uv as 剧集解锁广告观看人数,
    total_unlock_ad_click_pv as 剧集解锁广告观看次数,
    total_ad_watch_finish_uv as "开屏/插屏广告观看人数",
    total_ad_watch_finish_pv as "开屏/插屏广告观看次数",
    total_firefly_ad_click_uv as "firefly广告观看人数",
    total_firefly_ad_click_pv as "firefly广告观看次数",
    total_adcloud_ad_click_uv as "adcloud广告观看人数",
    total_adcloud_ad_click_pv as "adcloud广告观看次数",
    total_ads_load_succeed_uv as "广告填充成功人数",
    total_ads_load_succeed_pv as "广告填充成功次数",
    total_ads_load_fail_uv as "广告填充失败人数",
    total_ads_load_fail_pv as "广告填充失败次数",
    ads_load_fail_rate as "广告填充失败率",
    total_ads_show_succeed_uv as "广告展示成功人数",
    total_ads_show_succeed_pv as "广告展示成功次数",
    total_ads_show_fail_uv as "广告展示失败人数",
    total_ads_show_fail_pv as "广告展示失败次数",
    ads_show_fail_rate as "广告展示失败率"
from public.ads_traffic_ad_statistics;

-- 分广告场景数据
select
    d_date as 日期,
    area as 区域,
    country_name as 国家,
    lang_name as 语言,
    os as 应用平台,
    ad_type as 广告场景,
    total_ad_click_uv as 广告观看人数,
    total_ad_click_pv as 广告观看次数,
    total_ad_click_new_uv as 新用户广告观看人数,
    total_ad_click_new_pv as 新用户广告观看次数,
    total_ad_click_old_uv as 老用户广告观看人数,
    total_ad_click_old_pv as 老用户广告观看次数,
    total_ad_click_pay_uv as 付费用户广告观看人数,
    total_ad_click_pay_pv as 付费用户广告观看次数,
    total_ad_click_no_pay_uv as 未付费用户广告观看人数,
    total_ad_click_no_pay_pv as 未付费用户广告观看次数,
    total_ads_load_succeed_uv as 广告填充成功人数,
    total_ads_load_succeed_pv as 广告填充成功次数,
    total_ads_load_fail_uv as 广告填充失败人数,
    total_ads_load_fail_pv as 广告填充失败次数,
    ads_load_fail_rate as 广告填充失败率,
    total_ads_show_succeed_uv as 广告展示成功人数,
    total_ads_show_succeed_pv as 广告展示成功次数,
    total_ads_show_fail_uv as 广告展示失败人数,
    total_ads_show_fail_pv as 广告展示失败次数,
    ads_show_fail_rate as 广告展示失败率
from public.ads_traffic_ad_type_statistics
