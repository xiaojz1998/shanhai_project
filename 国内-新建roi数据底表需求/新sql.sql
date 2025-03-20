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
---- 消耗数据
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
        TO_DATE(TO_CHAR(stat_time_day, '99999999'), 'YYYYMMDD'),
        link_id
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
, tmp_complete as (
    -- 补全条目表
    select
        t1.enter_date,
        t1.analysis_date,
        t1.os,
        t3.stat_cost_yuan,
        pay
    from (select
        t1.d_date as enter_date,
        t3.os,
        t4.ad_channel,
        t2.d_date as analysis_date,
        0 as pay
    from (select generate_series(date '2024-12-17',current_date-1 ,'1 day')::date d_date) t1            -- 生成进入日期
    left join (select generate_series(date '2024-12-17',current_date-1 ,'1 day')::date d_date) t2       -- 生成分析日期
        on t1.d_date<= t2.d_date and t2.d_date - t1.d_date <60                                          -- 60天roi
    left join (select unnest(array['android','iOS','其他']) as os)t3 on 1=1                              -- 生成os
    left join (select unnest(array['巨量(抖音)']) as ad_channel) t4 on 1=1 ) t1                           -- 生成渠道
    left join cost t3 on t1.enter_date=t3.date and t1.ad_channel=t3.ad_channel and t1.os=t3.os          -- 补全cost
)
-- enter_date 当天新用户，analysis_date那天的支付金额
, tmp_pay as (
    select
        t.enter_date,
        analysis_date,
        t.os,
        stat_cost_yuan,
        dnu,
        analysis_date- t.enter_date as day_num,
        sum(pay) over (partition by t.enter_date,stat_cost_yuan,t.os order by analysis_date rows between unbounded preceding and current row) as total_pay
    from (
        select
            enter_date,
            analysis_date,
            os,
            stat_cost_yuan,
            sum(pay) as pay
        from (select
            t1.enter_date,
            t2.analysis_date,
            t1.os,
            t3.stat_cost_yuan,
            sum(total_revenue) as pay
        from dnu_user t1
        join total_revenue t2 on t1.enter_date<=t2.analysis_date and t1.device_id=t2.device_id
        left join cost t3 on t1.enter_date=t3.date and t1.ad_channel=t3.ad_channel and t1.os=t3.os
        group by t1.enter_date,t2.analysis_date,t1.os,t3.stat_cost_yuan
        union all
        select * from tmp_complete ) tmp_complete_t
        group by tmp_complete_t.enter_date,tmp_complete_t.analysis_date,tmp_complete_t.os,tmp_complete_t.stat_cost_yuan
    ) t
    left join (
        select
            enter_date,
            os,
            count(distinct device_id) as dnu
        from dnu_user
        group by enter_date,os
    ) t1 on t.enter_date=t1.enter_date and t.os=t1.os
)
    select
        enter_date,
        os,
        stat_cost_yuan as cost,
        dnu,
        day_num as days,
        analysis_date,
        total_pay as pay_value,
        case when stat_cost_yuan=0 then null else total_pay/stat_cost_yuan end  as roi_value
    from tmp_pay
    order by enter_date ,days;




