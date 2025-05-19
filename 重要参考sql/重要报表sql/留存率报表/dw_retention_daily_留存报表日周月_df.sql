------------------------------------------
-- file: dw_retention_daily_留存报表日周月_df.新sql_v1
-- author: xiaoj
-- time: 2025/4/24 11:38
-- description:
------------------------------------------

SET timezone ='UTC';

INSERT INTO public.dw_retention_daily
SELECT * from(
SELECT md5(CONCAT(a.active_date,a.area,a.country_name,a.os,a.lang_name,a.is_paid)) as id
    ,a.active_date
    ,a.area AS 区域
	,a.country_name as 国家
	,a.os as 系统
    ,COUNT(DISTINCT a.uid) dau
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 1 THEN b.uid ELSE NULL END) 总次日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 3 THEN b.uid ELSE NULL END) 总3日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 7 THEN b.uid ELSE NULL END) 总7日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 14 THEN b.uid ELSE NULL END) 总14日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 30 THEN b.uid ELSE NULL END) 总30日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date THEN a.uid ELSE NULL END) 新用户数
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 1 THEN b.uid ELSE NULL END ) 新用户次日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 3 THEN b.uid ELSE NULL END ) 新用户3日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 7 THEN b.uid ELSE NULL END ) 新用户7日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 14 THEN b.uid ELSE NULL END ) 新用户14日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 30 THEN b.uid ELSE NULL END ) 新用户30日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=1 THEN a.uid ELSE NULL END) 新推广用户
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=1 AND b.active_date - a.active_date = 1 THEN b.uid ELSE NULL END ) 新推广用户次日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=1 AND b.active_date - a.active_date = 3 THEN b.uid ELSE NULL END) 新推广用户3日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=1 AND b.active_date - a.active_date = 7 THEN b.uid ELSE NULL END) 新推广用户7日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=1 AND b.active_date - a.active_date = 14 THEN b.uid ELSE NULL END) 新推广用户14日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=1 AND b.active_date - a.active_date = 30 THEN b.uid ELSE NULL END) 新推广用户30日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=0 THEN a.uid ELSE NULL END) 新自然用户数
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=0 AND b.active_date - a.active_date = 1 THEN b.uid ELSE NULL END) 新自然用户次日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=0 AND b.active_date - a.active_date = 3 THEN b.uid ELSE NULL END) 新自然用户3日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=0 AND b.active_date - a.active_date = 7 THEN b.uid ELSE NULL END) 新自然用户7日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=0 AND b.active_date - a.active_date = 14 THEN b.uid ELSE NULL END) 新自然用户14日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=0 AND b.active_date - a.active_date = 30 THEN b.uid ELSE NULL END) 新自然用户30日留存
    ,COUNT(DISTINCT CASE WHEN (a.active_date <> a.register_date or a.register_date is null) THEN a.uid ELSE NULL END) 老用户数
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 1 AND (a.active_date <> a.register_date or a.register_date is null) THEN b.uid ELSE NULL END ) 老用户次日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 3 AND (a.active_date <> a.register_date or a.register_date is null) THEN b.uid ELSE NULL END ) 老用户3日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 7 AND (a.active_date <> a.register_date or a.register_date is null) THEN b.uid ELSE NULL END ) 老用户7日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 14 AND (a.active_date <> a.register_date or a.register_date is null) THEN b.uid ELSE NULL END ) 老用户14日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 30 AND (a.active_date <> a.register_date or a.register_date is null) THEN b.uid ELSE NULL END ) 老用户30日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 60 THEN b.uid ELSE NULL END) dau_60login
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 120 THEN b.uid ELSE NULL END) dau_120login
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 60 THEN b.uid ELSE NULL END ) new_dau_60login
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 120 THEN b.uid ELSE NULL END ) new_dau_120login
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=1 AND b.active_date - a.active_date = 60 THEN b.uid ELSE NULL END) new_dau_60login_campaign
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=1 AND b.active_date - a.active_date = 120 THEN b.uid ELSE NULL END) new_dau_120login_campaign
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=0 AND b.active_date - a.active_date = 60 THEN b.uid ELSE NULL END) new_dau_60login_natural
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.is_campaign=0 AND b.active_date - a.active_date = 120 THEN b.uid ELSE NULL END) new_dau_120login_natural
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 60 AND (a.active_date <> a.register_date or a.register_date is null) THEN b.uid ELSE NULL END ) old_dau_60login
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 120 AND (a.active_date <> a.register_date or a.register_date is null) THEN b.uid ELSE NULL END ) old_dau_120login
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 90 THEN b.uid ELSE NULL END) dau_90login
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 180 THEN b.uid ELSE NULL END) dau_180login
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 360 THEN b.uid ELSE NULL END) dau_360login
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 90 THEN b.uid ELSE NULL END ) new_dau_90login
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 180 THEN b.uid ELSE NULL END ) new_dau_180login
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 360 THEN b.uid ELSE NULL END ) new_dau_360login
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 90 AND (a.active_date <> a.register_date or a.register_date is null) THEN b.uid ELSE NULL END ) old_dau_90login
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 180 AND (a.active_date <> a.register_date or a.register_date is null) THEN b.uid ELSE NULL END ) old_dau_180login
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 360 AND (a.active_date <> a.register_date or a.register_date is null) THEN b.uid ELSE NULL END ) old_dau_360login
,a.lang_name
,a.is_paid
FROM tmp.dw_retention_detail_tmp01 a
LEFT JOIN tmp.dw_retention_detail_tmp01 b ON a.uid = b.uid AND b.active_date > a.active_date
WHERE a.active_date <= '${dt}'
GROUP BY
a.active_date, a.area, a.country_name, a.os
,a.lang_name
,a.is_paid
)a
ON CONFLICT(id)
DO UPDATE SET
active_date = excluded.active_date
,区域 = excluded.区域
,国家 = excluded.国家
,系统 = excluded.系统
,dau = excluded.dau
,总次日留存 = excluded.总次日留存
,总3日留存 = excluded.总3日留存
,总7日留存 = excluded.总7日留存
,总14日留存 = excluded.总14日留存
,总30日留存 = excluded.总30日留存
,新用户数 = excluded.新用户数
,新用户次日留存 = excluded.新用户次日留存
,新用户3日留存 = excluded.新用户3日留存
,新用户7日留存 = excluded.新用户7日留存
,新用户14日留存 = excluded.新用户14日留存
,新用户30日留存 = excluded.新用户30日留存
,新推广用户 = excluded.新推广用户
,新推广用户次日留存 = excluded.新推广用户次日留存
,新推广用户3日留存 = excluded.新推广用户3日留存
,新推广用户7日留存 = excluded.新推广用户7日留存
,新推广用户14日留存 = excluded.新推广用户14日留存
,新推广用户30日留存 = excluded.新推广用户30日留存
,新自然用户数 = excluded.新自然用户数
,新自然用户次日留存 = excluded.新自然用户次日留存
,新自然用户3日留存 = excluded.新自然用户3日留存
,新自然用户7日留存 = excluded.新自然用户7日留存
,新自然用户14日留存 = excluded.新自然用户14日留存
,新自然用户30日留存 = excluded.新自然用户30日留存
,老用户数 = excluded.老用户数
,老用户次日留存 = excluded.老用户次日留存
,老用户3日留存 = excluded.老用户3日留存
,老用户7日留存 = excluded.老用户7日留存
,老用户14日留存 = excluded.老用户14日留存
,老用户30日留存 = excluded.老用户30日留存
,dau_60login = excluded.dau_60login
,dau_120login = excluded.dau_120login
,new_dau_60login = excluded.new_dau_60login
,new_dau_120login = excluded.new_dau_120login
,new_dau_60login_campaign = excluded.new_dau_60login_campaign
,new_dau_120login_campaign = excluded.new_dau_120login_campaign
,new_dau_60login_natural = excluded.new_dau_60login_natural
,new_dau_120login_natural = excluded.new_dau_120login_natural
,old_dau_60login = excluded.old_dau_60login
,old_dau_120login = excluded.old_dau_120login
,dau_90login = excluded.dau_90login
,dau_180login = excluded.dau_180login
,dau_360login = excluded.dau_360login
,new_dau_90login = excluded.new_dau_90login
,new_dau_180login = excluded.new_dau_180login
,new_dau_360login = excluded.new_dau_360login
,old_dau_90login = excluded.old_dau_90login
,old_dau_180login = excluded.old_dau_180login
,old_dau_360login = excluded.old_dau_360login
,lang_name = excluded.lang_name
,is_paid = excluded.is_paid
;


INSERT INTO public.dw_retention_week
select * from(
SELECT md5(CONCAT(a.week_day,a.active_week,a.area,a.country_name,a.os,a.lang_name)) as id
    ,a.week_day
    ,a.active_week
    ,a.area AS 区域
    ,a.country_name as 国家
    ,a.os as 系统
    ,COUNT(DISTINCT a.uid) wau
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date BETWEEN 7 AND 13 THEN b.uid  ELSE NULL END) 总次周留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date BETWEEN 14 AND 20 THEN b.uid ELSE NULL END) 总2周留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date BETWEEN 21 AND 27 THEN b.uid ELSE NULL END) 总3周留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date BETWEEN 28 AND 34 THEN b.uid ELSE NULL END) 总4周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week THEN a.uid ELSE NULL END) 新用户数
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 7 AND 13 THEN b.uid ELSE NULL END) 新用户次周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 14 AND 20 THEN b.uid ELSE NULL END) 新用户2周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 21 AND 27 THEN b.uid ELSE NULL END) 新用户3周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 28 AND 34 THEN b.uid ELSE NULL END) 新用户4周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND a.is_campaign = 1  THEN a.uid ELSE NULL END) 新推广用户数
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 7 AND 13  AND a.is_campaign = 1 THEN b.uid ELSE NULL END) 新推广用户次周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 14 AND 20 AND a.is_campaign = 1 THEN b.uid ELSE NULL END) 新推广用户2周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 21 AND 27 AND a.is_campaign = 1 THEN b.uid ELSE NULL END) 新推广用户3周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 28 AND 34 AND a.is_campaign = 1 THEN b.uid ELSE NULL END) 新推广用户4周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND a.is_campaign = 0  THEN a.uid ELSE NULL END) 新自然用户数
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 7 AND 13  AND a.is_campaign = 0 THEN b.uid ELSE NULL END) 新自然用户次周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 14 AND 20 AND a.is_campaign = 0 THEN b.uid ELSE NULL END) 新自然用户2周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 21 AND 27 AND a.is_campaign = 0 THEN b.uid ELSE NULL END) 新自然用户3周留存
    ,COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND b.active_date - a.active_date BETWEEN 28 AND 34 AND a.is_campaign = 0 THEN b.uid ELSE NULL END) 新自然用户4周留存
    ,COUNT(DISTINCT CASE WHEN (a.active_week <> a.register_week or a.register_week is null) THEN a.uid ELSE NULL END) 老用户数
    ,COUNT(DISTINCT CASE WHEN (a.active_week <> a.register_week or a.register_week is null) AND b.active_date - a.active_date BETWEEN 7 AND 13 THEN b.uid ELSE NULL END) 老用户次周留存
    ,COUNT(DISTINCT CASE WHEN (a.active_week <> a.register_week or a.register_week is null) AND b.active_date - a.active_date BETWEEN 14 AND 20  THEN b.uid ELSE NULL END) 老用户2周留存
    ,COUNT(DISTINCT CASE WHEN (a.active_week <> a.register_week or a.register_week is null) AND b.active_date - a.active_date BETWEEN 21 AND 27 THEN b.uid ELSE NULL END) 老用户3周留存
    ,COUNT(DISTINCT CASE WHEN (a.active_week <> a.register_week or a.register_week is null) AND b.active_date - a.active_date BETWEEN 28 AND 34 THEN b.uid ELSE NULL END) 老用户4周留存
    ,a.lang_name
    ,max(a.is_paid) as is_paid
FROM tmp.dw_retention_detail_tmp01 a
LEFT JOIN tmp.dw_retention_detail_tmp01 b ON a.uid = b.uid AND b.week_end > a.week_end
WHERE a.active_date <= '${dt}'
GROUP BY
a.week_day
,a.active_week
,a.area
,a.country_name
,a.os
,a.lang_name
)a
ON CONFLICT(id)
DO UPDATE SET
week_day = excluded.week_day
,active_week = excluded.active_week
,区域 = excluded.区域
,国家 = excluded.国家
,系统 = excluded.系统
,WAU = excluded.WAU
,总次周留存 = excluded.总次周留存
,总2周留存 = excluded.总2周留存
,总3周留存 = excluded.总3周留存
,总4周留存 = excluded.总4周留存
,新用户数 = excluded.新用户数
,新用户次周留存 = excluded.新用户次周留存
,新用户2周留存 = excluded.新用户2周留存
,新用户3周留存 = excluded.新用户3周留存
,新用户4周留存 = excluded.新用户4周留存
,新推广用户数 = excluded.新推广用户数
,新推广用户次周留存 = excluded.新推广用户次周留存
,新推广用户2周留存 = excluded.新推广用户2周留存
,新推广用户3周留存 = excluded.新推广用户3周留存
,新推广用户4周留存 = excluded.新推广用户4周留存
,新自然用户数 = excluded.新自然用户数
,新自然用户次周留存 = excluded.新自然用户次周留存
,新自然用户2周留存 = excluded.新自然用户2周留存
,新自然用户3周留存 = excluded.新自然用户3周留存
,新自然用户4周留存 = excluded.新自然用户4周留存
,老用户数 = excluded.老用户数
,老用户次周留存 = excluded.老用户次周留存
,老用户2周留存 = excluded.老用户2周留存
,老用户3周留存 = excluded.老用户3周留存
,老用户4周留存 = excluded.老用户4周留存
,lang_name = excluded.lang_name
,is_paid = excluded.is_paid
;


insert into public.dw_retention_month
select * from(
SELECT md5(CONCAT(a.active_month,a.area,a.country_name,a.os,a.lang_name)) as id
    ,a.active_month
    ,a.area AS 区域
    ,a.country_name as 国家
    ,a.os as 系统
    ,COUNT(DISTINCT a.uid) AS mau
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date BETWEEN 30 AND 59 THEN b.uid ELSE NULL END) 总次月留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date BETWEEN 60 AND 89 THEN b.uid ELSE NULL END) 总2月留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date BETWEEN 90 AND 119  THEN b.uid ELSE NULL END) 总3月留存
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month THEN a.uid ELSE NULL END) 新用户数
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND b.active_date - a.active_date BETWEEN 30 AND 59 THEN b.uid ELSE NULL END) 新用户次月留存
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND b.active_date - a.active_date BETWEEN 60 AND 89 THEN b.uid ELSE NULL END) 新用户2月留存
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND b.active_date - a.active_date BETWEEN 90 AND 119  THEN b.uid ELSE NULL END) 新用户3月留存
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND a.is_campaign = 1 THEN a.uid ELSE NULL END) 新推广用户数
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND b.active_date - a.active_date BETWEEN 30 AND 59 AND a.is_campaign = 1  THEN b.uid ELSE NULL END) 新推广用户次月留存
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND b.active_date - a.active_date BETWEEN 60 AND 89 AND a.is_campaign = 1 THEN b.uid ELSE NULL END) 新推广用户2月留存
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND b.active_date - a.active_date BETWEEN 90 AND 119 AND a.is_campaign = 1  THEN b.uid ELSE NULL END) 新推广用户3月留存
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND a.is_campaign = 0 THEN a.uid ELSE NULL END) 新自然用户数
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND b.active_date - a.active_date BETWEEN 30 AND 59 AND a.is_campaign = 0  THEN b.uid ELSE NULL END) 新自然用户次月留存
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND b.active_date - a.active_date BETWEEN 60 AND 89 AND a.is_campaign = 0 THEN b.uid ELSE NULL END) 新自然用户2月留存
    ,COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND b.active_date - a.active_date BETWEEN 90 AND 119 AND a.is_campaign = 0  THEN b.uid ELSE NULL END) 新自然用户3月留存
    ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) THEN a.uid ELSE NULL END) 老用户数
    ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) AND b.active_date - a.active_date BETWEEN 30 AND 59 THEN b.uid ELSE NULL END) 老用户次月留存
    ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) AND b.active_date - a.active_date BETWEEN 60 AND 89 THEN b.uid ELSE NULL END) 老用户2月留存
    ,COUNT(DISTINCT CASE WHEN (a.active_month <> a.register_month or a.register_month is null) AND b.active_date - a.active_date BETWEEN 90 AND 119 THEN b.uid ELSE NULL END) 老用户3月留存
    ,a.lang_name
    ,max(a.is_paid) as is_paid
FROM tmp.dw_retention_detail_tmp01 a
LEFT JOIN tmp.dw_retention_detail_tmp01 b ON a.uid = b.uid AND b.week_end > a.week_end
WHERE a.active_date <= '${dt}'
GROUP BY
a.active_month
,a.area
,a.country_name
,a.os
,a.lang_name
)a
ON CONFLICT(id)
DO UPDATE SET
active_month = excluded.active_month
,区域 = excluded.区域
,国家 = excluded.国家
,系统 = excluded.系统
,MAU = excluded.MAU
,总次月留存 = excluded.总次月留存
,总2月留存 = excluded.总2月留存
,总3月留存 = excluded.总3月留存
,新用户数 = excluded.新用户数
,新用户次月留存 = excluded.新用户次月留存
,新用户2月留存 = excluded.新用户2月留存
,新用户3月留存 = excluded.新用户3月留存
,新推广用户数 = excluded.新推广用户数
,新推广用户次月留存 = excluded.新推广用户次月留存
,新推广用户2月留存 = excluded.新推广用户2月留存
,新推广用户3月留存 = excluded.新推广用户3月留存
,新自然用户数 = excluded.新自然用户数
,新自然用户次月留存 = excluded.新自然用户次月留存
,新自然用户2月留存 = excluded.新自然用户2月留存
,新自然用户3月留存 = excluded.新自然用户3月留存
,老用户数 = excluded.老用户数
,老用户次月留存 = excluded.老用户次月留存
,老用户2月留存 = excluded.老用户2月留存
,老用户3月留存 = excluded.老用户3月留存
,lang_name = excluded.lang_name
,is_paid = excluded.is_paid
;
