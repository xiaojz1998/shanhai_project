------ 圈定新用户：归因新用户
with dnu_user as (
    select
      created_date as  enter_date,
      case when ad_channel is null then '自然流' else ad_channel end as ad_channel,
      case when os=1 then 'android' when os=2 then 'iOS' else '其他' end as os,
      device_id,
      link_id
    from dw.dwd_user_info
    where ad_channel !='归因缺失'
      and guiyin_date is not null
      and ad_channel='巨量(抖音)'
    group by
        created_date ,
        case when ad_channel is null then '自然流' else ad_channel end ,
        case when os=1 then 'android' when os=2 then 'iOS' else '其他' end ,
        device_id,
        link_id
)
-------付费收入
,pay_user as(
    SELECT
        date(to_timestamp(c_time)) analysis_date,
        device_id,
        sum(amount)*1.0/100 yuan
    FROM duanju_vip_order
    where status=2 and amount>0
    GROUP BY date(to_timestamp(c_time)),device_id
)

-------广告收入
,ad_revenue as(
    select DISTINCT
        analysis_date,
        device_id,
        ad_show_pv,
        imp_cnt,
        revenue,
        revenue/imp_cnt*ad_show_pv ad_revenue
    from
    (
        SELECT
            created_date analysis_date,
            device_id,
            os,
            count(distinct created_at) ad_show_pv
        FROM user_track_log
        WHERE event_name in ('ad_show')
            and created_date is not null
        group by
            analysis_date,
            device_id,
            os
    ) t1
    left join
    (
        SELECT
            TO_DATE(date, 'YYYY-MM-DD') date,
            case when site_id='5564670' then 'android' when site_id='5574041' then 'iOS' else '其他' end as os,
            sum(imp_cnt::numeric) imp_cnt,
            sum(revenue::numeric) revenue
        from csj_income_data_log
        where site_id in ('5564670','5574041')
        group by date,site_id
    ) t2
    on t1.analysis_date=t2.date and t1.os=t2.os
)

-----总收入
,total_revenue as(
    SELECT
      COALESCE(p.analysis_date, a.analysis_date) AS analysis_date,
      COALESCE(p.device_id, a.device_id) AS device_id,
      COALESCE(p.yuan, 0) + COALESCE(a.ad_revenue, 0) AS total_revenue
    FROM pay_user p
    FULL OUTER JOIN ad_revenue a
    ON p.analysis_date = a.analysis_date AND p.device_id = a.device_id
)

-- select * from t0

--------------------------- 消耗数据
,cost as(
SELECT
  date,
  -- link_id,
  ad_channel,
  case when platform='android' then 'android' when platform='ios' then 'iOS' else '其他' end as os,
  sum(stat_cost_yuan) stat_cost_yuan
from(
SELECT
  TO_DATE(TO_CHAR(stat_time_day, '99999999'), 'YYYYMMDD') AS date,
  link_id,
  sum(show_cnt) show_cnt,
  sum(click_cnt) click_cnt,
  sum(stat_cost)*1.0/100 stat_cost_yuan
FROM ad_channel_cost_data
GROUP BY
    date
    ,link_id
)t1
left join (
select DISTINCT
  id link_id,
  case when kind=1 then '巨量(抖音)'
    when kind=2 then '快手'
    when kind=3 then '腾讯'
    when kind=4 then '百度'
    when kind=5 then '小米'
    when kind=6 then 'vivo'
    when kind=7 then 'oppo'
  else '其他' end as ad_channel,
  platform
from duanju_promotion_links
)t2 on t1.link_id=t2.link_id
GROUP BY
  date,
  -- link_id,
  ad_channel,
  platform
having sum(stat_cost_yuan)>0
)

,t as(
-- 汇总：按天，新增用户累计总收入、DNU、消耗
select
    t1.enter_date,
    -- t1.ad_channel,
    t1.os,
    t3.stat_cost_yuan,
    count(DISTINCT t1.device_id) DNU
        ,sum(case when enter_date=analysis_date and enter_date<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_0
        ,sum(case when enter_date between (analysis_date+interval'- 1 d') and analysis_date and (enter_date+interval' 1 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_1
        ,sum(case when enter_date between (analysis_date+interval'- 2 d') and analysis_date and (enter_date+interval' 2 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_2
        ,sum(case when enter_date between (analysis_date+interval'- 3 d') and analysis_date and (enter_date+interval' 3 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_3
        ,sum(case when enter_date between (analysis_date+interval'- 4 d') and analysis_date and (enter_date+interval' 4 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_4
        ,sum(case when enter_date between (analysis_date+interval'- 5 d') and analysis_date and (enter_date+interval' 5 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_5
        ,sum(case when enter_date between (analysis_date+interval'- 6 d') and analysis_date and (enter_date+interval' 6 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_6
        ,sum(case when enter_date between (analysis_date+interval'- 7 d') and analysis_date and (enter_date+interval' 7 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_7
        ,sum(case when enter_date between (analysis_date+interval'- 8 d') and analysis_date and (enter_date+interval' 8 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_8
        ,sum(case when enter_date between (analysis_date+interval'- 9 d') and analysis_date and (enter_date+interval' 9 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_9
        ,sum(case when enter_date between (analysis_date+interval'-10 d') and analysis_date and (enter_date+interval'10 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_10
        ,sum(case when enter_date between (analysis_date+interval'-11 d') and analysis_date and (enter_date+interval'11 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_11
        ,sum(case when enter_date between (analysis_date+interval'-12 d') and analysis_date and (enter_date+interval'12 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_12
        ,sum(case when enter_date between (analysis_date+interval'-13 d') and analysis_date and (enter_date+interval'13 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_13
        ,sum(case when enter_date between (analysis_date+interval'-14 d') and analysis_date and (enter_date+interval'14 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_14
        ,sum(case when enter_date between (analysis_date+interval'-15 d') and analysis_date and (enter_date+interval'15 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_15
        ,sum(case when enter_date between (analysis_date+interval'-16 d') and analysis_date and (enter_date+interval'16 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_16
        ,sum(case when enter_date between (analysis_date+interval'-17 d') and analysis_date and (enter_date+interval'17 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_17
        ,sum(case when enter_date between (analysis_date+interval'-18 d') and analysis_date and (enter_date+interval'18 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_18
        ,sum(case when enter_date between (analysis_date+interval'-19 d') and analysis_date and (enter_date+interval'19 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_19
        ,sum(case when enter_date between (analysis_date+interval'-20 d') and analysis_date and (enter_date+interval'20 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_20
        ,sum(case when enter_date between (analysis_date+interval'-21 d') and analysis_date and (enter_date+interval'21 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_21
        ,sum(case when enter_date between (analysis_date+interval'-22 d') and analysis_date and (enter_date+interval'22 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_22
        ,sum(case when enter_date between (analysis_date+interval'-23 d') and analysis_date and (enter_date+interval'23 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_23
        ,sum(case when enter_date between (analysis_date+interval'-24 d') and analysis_date and (enter_date+interval'24 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_24
        ,sum(case when enter_date between (analysis_date+interval'-25 d') and analysis_date and (enter_date+interval'25 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_25
        ,sum(case when enter_date between (analysis_date+interval'-26 d') and analysis_date and (enter_date+interval'26 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_26
        ,sum(case when enter_date between (analysis_date+interval'-27 d') and analysis_date and (enter_date+interval'27 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_27
        ,sum(case when enter_date between (analysis_date+interval'-28 d') and analysis_date and (enter_date+interval'28 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_28
        ,sum(case when enter_date between (analysis_date+interval'-29 d') and analysis_date and (enter_date+interval'29 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_29
        ,sum(case when enter_date between (analysis_date+interval'-30 d') and analysis_date and (enter_date+interval'30 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_30
        ,sum(case when enter_date between (analysis_date+interval'-31 d') and analysis_date and (enter_date+interval'31 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_31
        ,sum(case when enter_date between (analysis_date+interval'-32 d') and analysis_date and (enter_date+interval'32 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_32
        ,sum(case when enter_date between (analysis_date+interval'-33 d') and analysis_date and (enter_date+interval'33 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_33
        ,sum(case when enter_date between (analysis_date+interval'-34 d') and analysis_date and (enter_date+interval'34 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_34
        ,sum(case when enter_date between (analysis_date+interval'-35 d') and analysis_date and (enter_date+interval'35 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_35
        ,sum(case when enter_date between (analysis_date+interval'-36 d') and analysis_date and (enter_date+interval'36 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_36
        ,sum(case when enter_date between (analysis_date+interval'-37 d') and analysis_date and (enter_date+interval'37 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_37
        ,sum(case when enter_date between (analysis_date+interval'-38 d') and analysis_date and (enter_date+interval'38 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_38
        ,sum(case when enter_date between (analysis_date+interval'-39 d') and analysis_date and (enter_date+interval'39 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_39
        ,sum(case when enter_date between (analysis_date+interval'-40 d') and analysis_date and (enter_date+interval'40 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_40
        ,sum(case when enter_date between (analysis_date+interval'-41 d') and analysis_date and (enter_date+interval'41 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_41
        ,sum(case when enter_date between (analysis_date+interval'-42 d') and analysis_date and (enter_date+interval'42 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_42
        ,sum(case when enter_date between (analysis_date+interval'-43 d') and analysis_date and (enter_date+interval'43 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_43
        ,sum(case when enter_date between (analysis_date+interval'-44 d') and analysis_date and (enter_date+interval'44 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_44
        ,sum(case when enter_date between (analysis_date+interval'-45 d') and analysis_date and (enter_date+interval'45 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_45
        ,sum(case when enter_date between (analysis_date+interval'-46 d') and analysis_date and (enter_date+interval'46 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_46
        ,sum(case when enter_date between (analysis_date+interval'-47 d') and analysis_date and (enter_date+interval'47 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_47
        ,sum(case when enter_date between (analysis_date+interval'-48 d') and analysis_date and (enter_date+interval'48 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_48
        ,sum(case when enter_date between (analysis_date+interval'-49 d') and analysis_date and (enter_date+interval'49 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_49
        ,sum(case when enter_date between (analysis_date+interval'-50 d') and analysis_date and (enter_date+interval'50 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_50
        ,sum(case when enter_date between (analysis_date+interval'-51 d') and analysis_date and (enter_date+interval'51 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_51
        ,sum(case when enter_date between (analysis_date+interval'-52 d') and analysis_date and (enter_date+interval'52 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_52
        ,sum(case when enter_date between (analysis_date+interval'-53 d') and analysis_date and (enter_date+interval'53 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_53
        ,sum(case when enter_date between (analysis_date+interval'-54 d') and analysis_date and (enter_date+interval'54 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_54
        ,sum(case when enter_date between (analysis_date+interval'-55 d') and analysis_date and (enter_date+interval'55 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_55
        ,sum(case when enter_date between (analysis_date+interval'-56 d') and analysis_date and (enter_date+interval'56 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_56
        ,sum(case when enter_date between (analysis_date+interval'-57 d') and analysis_date and (enter_date+interval'57 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_57
        ,sum(case when enter_date between (analysis_date+interval'-58 d') and analysis_date and (enter_date+interval'58 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_58
        ,sum(case when enter_date between (analysis_date+interval'-59 d') and analysis_date and (enter_date+interval'59 d')<=(current_date+interval'0 d')::date then total_revenue else null end) as pay_59

from dnu_user t1
left join total_revenue t2 on t1.enter_date<=t2.analysis_date and t1.device_id=t2.device_id
left join cost t3 on t1.enter_date=t3.date and t1.ad_channel=t3.ad_channel and t1.os=t3.os
-- where enter_date='2025-01-16'
GROUP BY
    t1.enter_date,
    -- t1.ad_channel,
    t1.os,
    t3.stat_cost_yuan
)

,roi_table as(
select
    enter_date date,
    -- ad_channel,
    os,
    -- stat_cost_yuan,
    -- DNU,
    pay_0/stat_cost_yuan ROI0,
    pay_1/stat_cost_yuan ROI1,
    pay_2/stat_cost_yuan ROI2,
    pay_3/stat_cost_yuan ROI3,
    pay_4/stat_cost_yuan ROI4,
    pay_5/stat_cost_yuan ROI5,
    pay_6/stat_cost_yuan ROI6,
    pay_7/stat_cost_yuan ROI7,
    pay_8/stat_cost_yuan ROI8,
    pay_9/stat_cost_yuan ROI9,
    pay_10/stat_cost_yuan ROI10,
    pay_11/stat_cost_yuan ROI11,
    pay_12/stat_cost_yuan ROI12,
    pay_13/stat_cost_yuan ROI13,
    pay_14/stat_cost_yuan ROI14,
    pay_15/stat_cost_yuan ROI15,
    pay_16/stat_cost_yuan ROI16,
    pay_17/stat_cost_yuan ROI17,
    pay_18/stat_cost_yuan ROI18,
    pay_19/stat_cost_yuan ROI19,
    pay_20/stat_cost_yuan ROI20,
    pay_21/stat_cost_yuan ROI21,
    pay_22/stat_cost_yuan ROI22,
    pay_23/stat_cost_yuan ROI23,
    pay_24/stat_cost_yuan ROI24,
    pay_25/stat_cost_yuan ROI25,
    pay_26/stat_cost_yuan ROI26,
    pay_27/stat_cost_yuan ROI27,
    pay_28/stat_cost_yuan ROI28,
    pay_29/stat_cost_yuan ROI29,
    pay_30/stat_cost_yuan ROI30,
    pay_31/stat_cost_yuan ROI31,
    pay_32/stat_cost_yuan ROI32,
    pay_33/stat_cost_yuan ROI33,
    pay_34/stat_cost_yuan ROI34,
    pay_35/stat_cost_yuan ROI35,
    pay_36/stat_cost_yuan ROI36,
    pay_37/stat_cost_yuan ROI37,
    pay_38/stat_cost_yuan ROI38,
    pay_39/stat_cost_yuan ROI39,
    pay_40/stat_cost_yuan ROI40,
    pay_41/stat_cost_yuan ROI41,
    pay_42/stat_cost_yuan ROI42,
    pay_43/stat_cost_yuan ROI43,
    pay_44/stat_cost_yuan ROI44,
    pay_45/stat_cost_yuan ROI45,
    pay_46/stat_cost_yuan ROI46,
    pay_47/stat_cost_yuan ROI47,
    pay_48/stat_cost_yuan ROI48,
    pay_49/stat_cost_yuan ROI49,
    pay_50/stat_cost_yuan ROI50,
    pay_51/stat_cost_yuan ROI51,
    pay_52/stat_cost_yuan ROI52,
    pay_53/stat_cost_yuan ROI53,
    pay_54/stat_cost_yuan ROI54,
    pay_55/stat_cost_yuan ROI55,
    pay_56/stat_cost_yuan ROI56,
    pay_57/stat_cost_yuan ROI57,
    pay_58/stat_cost_yuan ROI58,
    pay_59/stat_cost_yuan ROI59
from t
WHERE stat_cost_yuan>0
)

-- 将宽表中的多列ROI拆解为长格式
SELECT
    date,
    os,
    0 AS roi_type,
    ROI0 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    1 AS roi_type,
    ROI1 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    2 AS roi_type,
    ROI2 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    3 AS roi_type,
    ROI3 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    4 AS roi_type,
    ROI4 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    5 AS roi_type,
    ROI5 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    6 AS roi_type,
    ROI6 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    7 AS roi_type,
    ROI7 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    8 AS roi_type,
    ROI8 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    9 AS roi_type,
    ROI9 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    10 AS roi_type,
    ROI10 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    11 AS roi_type,
    ROI11 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    12 AS roi_type,
    ROI12 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    13 AS roi_type,
    ROI13 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    14 AS roi_type,
    ROI14 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    15 AS roi_type,
    ROI15 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    16 AS roi_type,
    ROI16 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    17 AS roi_type,
    ROI17 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    18 AS roi_type,
    ROI18 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    19 AS roi_type,
    ROI19 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    20 AS roi_type,
    ROI20 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    21 AS roi_type,
    ROI21 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    22 AS roi_type,
    ROI22 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    23 AS roi_type,
    ROI23 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    24 AS roi_type,
    ROI24 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    25 AS roi_type,
    ROI25 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    26 AS roi_type,
    ROI26 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    27 AS roi_type,
    ROI27 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    28 AS roi_type,
    ROI28 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    29 AS roi_type,
    ROI29 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    30 AS roi_type,
    ROI30 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    31 AS roi_type,
    ROI31 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    32 AS roi_type,
    ROI32 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    33 AS roi_type,
    ROI33 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    34 AS roi_type,
    ROI34 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    35 AS roi_type,
    ROI35 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    36 AS roi_type,
    ROI36 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    37 AS roi_type,
    ROI37 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    38 AS roi_type,
    ROI38 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    39 AS roi_type,
    ROI39 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    40 AS roi_type,
    ROI40 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    41 AS roi_type,
    ROI41 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    42 AS roi_type,
    ROI42 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    43 AS roi_type,
    ROI43 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    44 AS roi_type,
    ROI44 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    45 AS roi_type,
    ROI45 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    46 AS roi_type,
    ROI46 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    47 AS roi_type,
    ROI47 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    48 AS roi_type,
    ROI48 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    49 AS roi_type,
    ROI49 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    50 AS roi_type,
    ROI50 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    51 AS roi_type,
    ROI51 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    52 AS roi_type,
    ROI52 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    53 AS roi_type,
    ROI53 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    54 AS roi_type,
    ROI54 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    55 AS roi_type,
    ROI55 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    56 AS roi_type,
    ROI56 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    57 AS roi_type,
    ROI57 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    58 AS roi_type,
    ROI58 AS roi_value
FROM
    roi_table
UNION ALL
SELECT
    date,
    os,
    59 AS roi_type,
    ROI59 AS roi_value
FROM
    roi_table
-- ORDER BY  date, os, roi_type;