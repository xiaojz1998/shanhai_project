with newuser_tb as(
    select
        *
    from
(
            select
                to_timestamp(created_at) :: date as p_date,
case
                    when (
                        area = ''
                        or area is null
                    ) then 'UNKNOWN'
                    else upper(area)
                end as country_code,
                upper(
                    case
                        when campaign_id = '0'
                        or campaign_id = '' then '自然'
                        when ad_channel = 'tt' then 'tt'
                        when ad_channel in ('fb', 'metaweb') then 'fb'
                        when ad_channel = 'apple' then 'apple'
                        when ad_channel = 'moloco_int' then 'moloco_int'
                        when substr(ad_channel, 1, 12) = 'social_media' then '社媒'
                        else '未知'
                    end
                ) as ad_channel,
                upper(put_system) as put_system,
                v_type,
                ad_format,
                uid,
                campaign_id,
                campaign_name,
                row_number() over(
                    partition by uid
                    order by
                        created_at
                ) rk -- 可能存在多次归因，以第一次为准
            from
                public.user_log
            where
                event = 1
                and created_date >= 20240701
        ) tmp
    where
        rk = 1 -- and p_date = current_date - 1
),
tmp_user_pay as(
    select
        p_date,
        uid,
        sum(pay_amt) as money
    from
(
            select
                to_timestamp(o.created_at) :: date as p_date,
                o.uid,
                sum(o.money) * 0.01 as pay_amt
            from
                public.all_order_log o
            where
                environment = 1
                and status = 1 -- and to_timestamp(o.created_at)::date = current_date - 1
            group by
                to_timestamp(o.created_at) :: date,
                o.uid
            union
            all
            select
                to_timestamp(r.refund_time) :: date as p_date,
                uid,
                - sum(r.money) * 0.01 as refund_amt -- 减去退款
            from
                public.all_refund_order_log r
            where
                r.environment = 1
                and r.status = 1 -- and to_timestamp(r.refund_time)::date = current_date - 1
            group by
                to_timestamp(r.refund_time) :: date,
                r.uid
        ) a
    group by
        p_date,
        uid
),
total_cost as (
    select
        日期,
        语言,
        投放组,
        sum(广告消耗) over(
            partition by 语言,
            投放组,
            to_char(日期, 'YYYY-MM')
            order by
                日期
        ) 本月累计消耗,
        sum(广告消耗) over(
            partition by 语言,
            投放组,
            to_char(日期, 'IYYY"-"IW')
            order by
                日期
        ) 本周累计消耗
    from
        (
            select
                TO_TIMESTAMP(t1.created_at) :: date 日期,
                if(
                    language_tb.name in (
                        '中文简体',
                        '繁体中文'
                    ),
                    '中文',
                    language_tb.name
                ) as 语言,
    CASE 
        WHEN  UPPER(optimize_group) LIKE 'G%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'G1' THEN '广州一组'
                WHEN  UPPER(optimize_group) = 'G2' THEN '广州二组'
                WHEN  UPPER(optimize_group) = 'G3' THEN '广州三组'
                WHEN  UPPER(optimize_group) = 'G4' THEN '广州四组'
                WHEN  UPPER(optimize_group) = 'G5' THEN '广州五组'
                WHEN  UPPER(optimize_group) = 'G6' THEN '广州六组'
                WHEN  UPPER(optimize_group) = 'G7' THEN '广州七组'
                WHEN  UPPER(optimize_group) = 'G8' THEN '广州八组'
                WHEN  UPPER(optimize_group) = 'G9' THEN '广州九组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'Z%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'Z1' THEN '深圳一组'
                WHEN  UPPER(optimize_group) = 'Z2' THEN '深圳二组'
                WHEN  UPPER(optimize_group) = 'Z3' THEN '深圳三组'
                WHEN  UPPER(optimize_group) = 'Z4' THEN '深圳四组'
                WHEN  UPPER(optimize_group) = 'Z5' THEN '深圳五组'
                WHEN  UPPER(optimize_group) = 'Z6' THEN '深圳六组'
                WHEN  UPPER(optimize_group) = 'Z88' THEN '深圳88组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'N%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'N1' THEN '南宁一组'
                WHEN  UPPER(optimize_group) = 'N2' THEN '南宁二组'
                WHEN  UPPER(optimize_group) = 'N3' THEN '南宁三组'
                WHEN  UPPER(optimize_group) = 'N4' THEN '南宁四组'
                WHEN  UPPER(optimize_group) = 'N5' THEN '南宁五组'
                WHEN  UPPER(optimize_group) = 'N6' THEN '南宁六组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) = 'SNS' THEN 'SNS'
        WHEN  UPPER(optimize_group) = 'CPS' THEN 'CPS'
        WHEN  UPPER(optimize_group) = 'MJTF' THEN '媒介投放组'
        ELSE '其他'
    END as 投放组,
                round(sum(cost_amount) * 1.0 / 10000, 2) 广告消耗
            from
                ad_cost_data_log t1
                left join v_dim_ad_campaign_info b on t1.campaign_id = b.campaign_id
                left join "oversea-api_osd_videos" vid_tb on cast(split_part(t1.campaign_name, '_', 5) as varchar) = cast(vid_tb.id as varchar)
                left join "oversea-api_osd_categories" language_tb on cast(vid_tb.category_id as varchar) = cast(language_tb.id as varchar)
            where
                TO_TIMESTAMP(t1.created_at) :: date >= current_date - 40
                and split_part(b.campaign_name, '_', 1) not in (
                    '飞书',
                    'beijingdilcn808',
                    'huntmobi'
                )
                and "cost_amount" > 0
            group by
                TO_TIMESTAMP(t1.created_at) :: date,
                if(
                    language_tb.name in (
                        '中文简体',
                        '繁体中文'
                    ),
                    '中文',
                    language_tb.name
                ),
    CASE 
        WHEN  UPPER(optimize_group) LIKE 'G%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'G1' THEN '广州一组'
                WHEN  UPPER(optimize_group) = 'G2' THEN '广州二组'
                WHEN  UPPER(optimize_group) = 'G3' THEN '广州三组'
                WHEN  UPPER(optimize_group) = 'G4' THEN '广州四组'
                WHEN  UPPER(optimize_group) = 'G5' THEN '广州五组'
                WHEN  UPPER(optimize_group) = 'G6' THEN '广州六组'
                WHEN  UPPER(optimize_group) = 'G7' THEN '广州七组'
                WHEN  UPPER(optimize_group) = 'G8' THEN '广州八组'
                WHEN  UPPER(optimize_group) = 'G9' THEN '广州九组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'Z%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'Z1' THEN '深圳一组'
                WHEN  UPPER(optimize_group) = 'Z2' THEN '深圳二组'
                WHEN  UPPER(optimize_group) = 'Z3' THEN '深圳三组'
                WHEN  UPPER(optimize_group) = 'Z4' THEN '深圳四组'
                WHEN  UPPER(optimize_group) = 'Z5' THEN '深圳五组'
                WHEN  UPPER(optimize_group) = 'Z6' THEN '深圳六组'
                WHEN  UPPER(optimize_group) = 'Z88' THEN '深圳88组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'N%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'N1' THEN '南宁一组'
                WHEN  UPPER(optimize_group) = 'N2' THEN '南宁二组'
                WHEN  UPPER(optimize_group) = 'N3' THEN '南宁三组'
                WHEN  UPPER(optimize_group) = 'N4' THEN '南宁四组'
                WHEN  UPPER(optimize_group) = 'N5' THEN '南宁五组'
                WHEN  UPPER(optimize_group) = 'N6' THEN '南宁六组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) = 'SNS' THEN 'SNS'
        WHEN  UPPER(optimize_group) = 'CPS' THEN 'CPS'
        WHEN  UPPER(optimize_group) = 'MJTF' THEN '媒介投放组'
        ELSE '其他'
    END
        ) t1
),
t1 as (
    select
        TO_TIMESTAMP(t1.created_at) :: date 日期,
        if(
            language_tb.name in (
                '中文简体',
                '繁体中文'
            ),
            '中文',
            language_tb.name
        ) as 语言,
    CASE 
        WHEN  UPPER(optimize_group) LIKE 'G%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'G1' THEN '广州一组'
                WHEN  UPPER(optimize_group) = 'G2' THEN '广州二组'
                WHEN  UPPER(optimize_group) = 'G3' THEN '广州三组'
                WHEN  UPPER(optimize_group) = 'G4' THEN '广州四组'
                WHEN  UPPER(optimize_group) = 'G5' THEN '广州五组'
                WHEN  UPPER(optimize_group) = 'G6' THEN '广州六组'
                WHEN  UPPER(optimize_group) = 'G7' THEN '广州七组'
                WHEN  UPPER(optimize_group) = 'G8' THEN '广州八组'
                WHEN  UPPER(optimize_group) = 'G9' THEN '广州九组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'Z%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'Z1' THEN '深圳一组'
                WHEN  UPPER(optimize_group) = 'Z2' THEN '深圳二组'
                WHEN  UPPER(optimize_group) = 'Z3' THEN '深圳三组'
                WHEN  UPPER(optimize_group) = 'Z4' THEN '深圳四组'
                WHEN  UPPER(optimize_group) = 'Z5' THEN '深圳五组'
                WHEN  UPPER(optimize_group) = 'Z6' THEN '深圳六组'
                WHEN  UPPER(optimize_group) = 'Z88' THEN '深圳88组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'N%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'N1' THEN '南宁一组'
                WHEN  UPPER(optimize_group) = 'N2' THEN '南宁二组'
                WHEN  UPPER(optimize_group) = 'N3' THEN '南宁三组'
                WHEN  UPPER(optimize_group) = 'N4' THEN '南宁四组'
                WHEN  UPPER(optimize_group) = 'N5' THEN '南宁五组'
                WHEN  UPPER(optimize_group) = 'N6' THEN '南宁六组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) = 'SNS' THEN 'SNS'
        WHEN  UPPER(optimize_group) = 'CPS' THEN 'CPS'
        WHEN  UPPER(optimize_group) = 'MJTF' THEN '媒介投放组'
        ELSE '其他'
    END as 投放组,
        t1.campaign_id,
        round(sum(cost_amount) * 1.0 / 10000, 2) 广告消耗,
        sum(impression) as impression,
        sum(click) as click
    from
        ad_cost_data_log t1
        left join v_dim_ad_campaign_info b on t1.campaign_id = b.campaign_id
        left join "oversea-api_osd_videos" vid_tb on cast(split_part(t1.campaign_name, '_', 5) as varchar) = cast(vid_tb.id as varchar)
        left join "oversea-api_osd_categories" language_tb on cast(vid_tb.category_id as varchar) = cast(language_tb.id as varchar)
    where
        TO_TIMESTAMP(t1.created_at) :: date <= current_date - 1
        and TO_TIMESTAMP(t1.created_at) :: date >= current_date - 7
        and split_part(b.campaign_name, '_', 1) not in (
            '飞书',
            'beijingdilcn808',
            'huntmobi'
        ) -- and "cost_amount" >0
    group by
        TO_TIMESTAMP(t1.created_at) :: date,
        if(
            language_tb.name in (
                '中文简体',
                '繁体中文'
            ),
            '中文',
            language_tb.name
        ),
    CASE 
        WHEN  UPPER(optimize_group) LIKE 'G%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'G1' THEN '广州一组'
                WHEN  UPPER(optimize_group) = 'G2' THEN '广州二组'
                WHEN  UPPER(optimize_group) = 'G3' THEN '广州三组'
                WHEN  UPPER(optimize_group) = 'G4' THEN '广州四组'
                WHEN  UPPER(optimize_group) = 'G5' THEN '广州五组'
                WHEN  UPPER(optimize_group) = 'G6' THEN '广州六组'
                WHEN  UPPER(optimize_group) = 'G7' THEN '广州七组'
                WHEN  UPPER(optimize_group) = 'G8' THEN '广州八组'
                WHEN  UPPER(optimize_group) = 'G9' THEN '广州九组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'Z%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'Z1' THEN '深圳一组'
                WHEN  UPPER(optimize_group) = 'Z2' THEN '深圳二组'
                WHEN  UPPER(optimize_group) = 'Z3' THEN '深圳三组'
                WHEN  UPPER(optimize_group) = 'Z4' THEN '深圳四组'
                WHEN  UPPER(optimize_group) = 'Z5' THEN '深圳五组'
                WHEN  UPPER(optimize_group) = 'Z6' THEN '深圳六组'
                WHEN  UPPER(optimize_group) = 'Z88' THEN '深圳88组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'N%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'N1' THEN '南宁一组'
                WHEN  UPPER(optimize_group) = 'N2' THEN '南宁二组'
                WHEN  UPPER(optimize_group) = 'N3' THEN '南宁三组'
                WHEN  UPPER(optimize_group) = 'N4' THEN '南宁四组'
                WHEN  UPPER(optimize_group) = 'N5' THEN '南宁五组'
                WHEN  UPPER(optimize_group) = 'N6' THEN '南宁六组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) = 'SNS' THEN 'SNS'
        WHEN  UPPER(optimize_group) = 'CPS' THEN 'CPS'
        WHEN  UPPER(optimize_group) = 'MJTF' THEN '媒介投放组'
        ELSE '其他'
    END,
        t1.campaign_id
),
t2 as (
    select
        newuser_tb.p_date,
        newuser_tb.campaign_id,
        count(distinct newuser_tb.uid) as newuser_uv,
        count(
            distinct if(
                newuser_tb.uid = tmp_user_pay.uid
                and newuser_tb.p_date = tmp_user_pay.p_date,
                newuser_tb.uid,
                null
            )
        ) as new_pay_uv,
        sum(
            if(
                newuser_tb.uid = tmp_user_pay.uid
                and tmp_user_pay.p_date = newuser_tb.p_date,
                tmp_user_pay.money,
                0.0
            )
        ) as 新用户收入
    from
        newuser_tb
        left join tmp_user_pay on cast(newuser_tb.uid as varchar) = cast(tmp_user_pay.uid as varchar)
    where
        newuser_tb.p_date <= current_date - 1
        and newuser_tb.p_date >= current_date - 7
    group by
        newuser_tb.p_date,
        newuser_tb.campaign_id
),
total_recharge as (
    select
        if(
            language_tb.name in (
                '中文简体',
                '繁体中文'
            ),
            '中文',
            language_tb.name
        ) as 语言,
    CASE 
        WHEN  UPPER(optimize_group) LIKE 'G%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'G1' THEN '广州一组'
                WHEN  UPPER(optimize_group) = 'G2' THEN '广州二组'
                WHEN  UPPER(optimize_group) = 'G3' THEN '广州三组'
                WHEN  UPPER(optimize_group) = 'G4' THEN '广州四组'
                WHEN  UPPER(optimize_group) = 'G5' THEN '广州五组'
                WHEN  UPPER(optimize_group) = 'G6' THEN '广州六组'
                WHEN  UPPER(optimize_group) = 'G7' THEN '广州七组'
                WHEN  UPPER(optimize_group) = 'G8' THEN '广州八组'
                WHEN  UPPER(optimize_group) = 'G9' THEN '广州九组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'Z%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'Z1' THEN '深圳一组'
                WHEN  UPPER(optimize_group) = 'Z2' THEN '深圳二组'
                WHEN  UPPER(optimize_group) = 'Z3' THEN '深圳三组'
                WHEN  UPPER(optimize_group) = 'Z4' THEN '深圳四组'
                WHEN  UPPER(optimize_group) = 'Z5' THEN '深圳五组'
                WHEN  UPPER(optimize_group) = 'Z6' THEN '深圳六组'
                WHEN  UPPER(optimize_group) = 'Z88' THEN '深圳88组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'N%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'N1' THEN '南宁一组'
                WHEN  UPPER(optimize_group) = 'N2' THEN '南宁二组'
                WHEN  UPPER(optimize_group) = 'N3' THEN '南宁三组'
                WHEN  UPPER(optimize_group) = 'N4' THEN '南宁四组'
                WHEN  UPPER(optimize_group) = 'N5' THEN '南宁五组'
                WHEN  UPPER(optimize_group) = 'N6' THEN '南宁六组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) = 'SNS' THEN 'SNS'
        WHEN  UPPER(optimize_group) = 'CPS' THEN 'CPS'
        WHEN  UPPER(optimize_group) = 'MJTF' THEN '媒介投放组'
        ELSE '其他'
    END as 投放组,
        sum(money) 总充值金额
    from
        newuser_tb
        inner join v_dim_ad_campaign_info b on newuser_tb.campaign_id = b.campaign_id
        inner join tmp_user_pay on cast(newuser_tb.uid as varchar) = cast(tmp_user_pay.uid as varchar)
        left join "oversea-api_osd_videos" vid_tb on cast(
            split_part(newuser_tb.campaign_name, '_', 5) as varchar
        ) = cast(vid_tb.id as varchar)
        left join "oversea-api_osd_categories" language_tb on cast(vid_tb.category_id as varchar) = cast(language_tb.id as varchar)
    where
        tmp_user_pay.p_date <= current_date - 1
        and tmp_user_pay.p_date >= current_date - 7
    group by
        if(
            language_tb.name in (
                '中文简体',
                '繁体中文'
            ),
            '中文',
            language_tb.name
        ),
    CASE 
        WHEN  UPPER(optimize_group) LIKE 'G%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'G1' THEN '广州一组'
                WHEN  UPPER(optimize_group) = 'G2' THEN '广州二组'
                WHEN  UPPER(optimize_group) = 'G3' THEN '广州三组'
                WHEN  UPPER(optimize_group) = 'G4' THEN '广州四组'
                WHEN  UPPER(optimize_group) = 'G5' THEN '广州五组'
                WHEN  UPPER(optimize_group) = 'G6' THEN '广州六组'
                WHEN  UPPER(optimize_group) = 'G7' THEN '广州七组'
                WHEN  UPPER(optimize_group) = 'G8' THEN '广州八组'
                WHEN  UPPER(optimize_group) = 'G9' THEN '广州九组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'Z%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'Z1' THEN '深圳一组'
                WHEN  UPPER(optimize_group) = 'Z2' THEN '深圳二组'
                WHEN  UPPER(optimize_group) = 'Z3' THEN '深圳三组'
                WHEN  UPPER(optimize_group) = 'Z4' THEN '深圳四组'
                WHEN  UPPER(optimize_group) = 'Z5' THEN '深圳五组'
                WHEN  UPPER(optimize_group) = 'Z6' THEN '深圳六组'
                WHEN  UPPER(optimize_group) = 'Z88' THEN '深圳88组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) LIKE 'N%' THEN 
            CASE 
                WHEN  UPPER(optimize_group) = 'N1' THEN '南宁一组'
                WHEN  UPPER(optimize_group) = 'N2' THEN '南宁二组'
                WHEN  UPPER(optimize_group) = 'N3' THEN '南宁三组'
                WHEN  UPPER(optimize_group) = 'N4' THEN '南宁四组'
                WHEN  UPPER(optimize_group) = 'N5' THEN '南宁五组'
                WHEN  UPPER(optimize_group) = 'N6' THEN '南宁六组'
                ELSE '其他'
            END
        WHEN  UPPER(optimize_group) = 'SNS' THEN 'SNS'
        WHEN  UPPER(optimize_group) = 'CPS' THEN 'CPS'
        WHEN  UPPER(optimize_group) = 'MJTF' THEN '媒介投放组'
        ELSE '其他'
    END
),
total_all as (
    select
        (current_date -7) :: varchar || '~' || (current_date -1) :: varchar as 日期,
        a.语言,
        a.投放组,
        COALESCE(广告消耗, 0.00) 广告消耗,
        COALESCE(新用户收入, 0.00) 新用户收入,
        concat(
            cast(
                COALESCE(新用户ROI_0, 0.00) as varchar
            ),
            '%'
        ) 新用户ROI,
        COALESCE(总充值金额, 0.00) 总充值金额,
        case
            when 广告消耗 = 0 then '0.00%'
            else concat(
                cast(
                    round(
                        总充值金额 * 100.0 / 广告消耗,
                        2
                    ) as varchar
                ),
                '%'
            )
        end 总ROI,
        COALESCE(展示量, 0) 展示量,
        "CPM(千次展现成本:$)",
        COALESCE(点击数, 0) 点击数,
        concat(
            cast(
                COALESCE(点击率, 0.00) as varchar
            ),
            '%'
        ) 点击率,
        COALESCE(拉新量, 0) as 激活量,
        COALESCE("CPA(激活成本:$)", 0.00) AS "CPA(激活成本:$)",
        COALESCE(新增当日付费用户数, 0) 新增当日付费用户数,
        concat(
            cast(
                COALESCE(付费率, 0.00) as varchar
            ),
            '%'
        ) as 新增付费率,
        COALESCE(付费成本, 0.00) 付费成本
    from
        (
            select
                语言,
                投放组,
                sum(广告消耗) 广告消耗,
                sum(新用户收入) 新用户收入,
                case
                    when sum(广告消耗) = 0 then 0.00
                    else round(
                        sum(新用户收入) * 100.0 / sum(广告消耗),
                        2
                    )
                end 新用户ROI_0 --   , sum(总充值金额) 总充值金额
                --   , case when sum(广告消耗) = 0 then 0
                --   else round(sum(总充值金额)*100.0/sum(广告消耗),2) end 总ROI
,
                sum(impression) 展示量,
                case
                    when sum(impression) = 0 then 0.00
                    else round(
                        sum(广告消耗) / sum(impression) * 1000.0,
                        2
                    )
                end as "CPM(千次展现成本:$)",
                sum(click) 点击数,
                case
                    when sum(impression) = 0 then 0.00
                    else round(sum(click) * 100.0 / sum(impression), 2)
                end as 点击率,
                sum(newuser_uv) as 拉新量,
                case
                    when sum(newuser_uv) = 0 then 0.00
                    else round(
                        sum(广告消耗) * 1.0 / sum(newuser_uv),
                        2
                    )
                end as "CPA(激活成本:$)",
                sum(new_pay_uv) 新增当日付费用户数,
                case
                    when sum(newuser_uv) = 0 then 0.00
                    else round(sum(new_pay_uv) * 100.0 / sum(newuser_uv), 2)
                end as 付费率,
                case
                    when sum(new_pay_uv) = 0 then 0.00
                    else round(
                        sum(广告消耗) * 1.0 / sum(new_pay_uv),
                        2
                    )
                end as 付费成本
            from
                t1
                left join t2 on t1.campaign_id = t2.campaign_id
                and t1.日期 = t2.p_date
            group by
                语言,
                投放组 -- having sum(广告消耗) >0
        ) a
        left join total_recharge on a.语言 = total_recharge.语言
        and a.投放组 = total_recharge.投放组
    union
    all
    select
        (current_date -7) :: varchar || '~' || (current_date -1) :: varchar as 日期,
        b.语言,
        '汇总' as 投放组,
        COALESCE(广告消耗, 0.00) 广告消耗,
        COALESCE(新用户收入, 0.00) 新用户收入,
        concat(
            cast(
                COALESCE(新用户ROI_0, 0.00) as varchar
            ),
            '%'
        ) 新用户ROI,
        COALESCE(总充值金额, 0.00) 总充值金额,
        case
            when 广告消耗 = 0 then '0.00%'
            else concat(
                cast(
                    round(
                        总充值金额 * 100.0 / 广告消耗,
                        2
                    ) as varchar
                ),
                '%'
            )
        end 总ROI,
        COALESCE(展示量, 0) 展示量,
        "CPM(千次展现成本:$)",
        COALESCE(点击数, 0) 点击数,
        concat(
            cast(
                COALESCE(点击率, 0.00) as varchar
            ),
            '%'
        ) 点击率,
        COALESCE(拉新量, 0) as 激活量,
        COALESCE("CPA(激活成本:$)", 0.00) AS "CPA(激活成本:$)",
        COALESCE(新增当日付费用户数, 0) 新增当日付费用户数,
        concat(
            cast(
                COALESCE(付费率, 0.00) as varchar
            ),
            '%'
        ) as 新增付费率,
        COALESCE(付费成本, 0.00) 付费成本
    from
        (
            select
                a.语言,
                sum(广告消耗) 广告消耗,
                sum(新用户收入) 新用户收入,
                case
                    when sum(广告消耗) = 0 then 0.00
                    else round(
                        sum(新用户收入) * 100.0 / sum(广告消耗),
                        2
                    )
                end 新用户ROI_0 --   , sum(总充值金额) 总充值金额
                --   , case when sum(广告消耗) = 0 then 0
                --   else round(sum(总充值金额)*100.0/sum(广告消耗),2) end 总ROI
,
                sum(总充值金额) as 总充值金额,
                sum(impression) 展示量,
                case
                    when sum(impression) = 0 then 0.00
                    else round(
                        sum(广告消耗) / sum(impression) * 1000.0,
                        2
                    )
                end as "CPM(千次展现成本:$)",
                sum(click) 点击数,
                case
                    when sum(impression) = 0 then 0.00
                    else round(sum(click) * 100.0 / sum(impression), 2)
                end as 点击率,
                sum(newuser_uv) as 拉新量,
                case
                    when sum(newuser_uv) = 0 then 0.00
                    else round(
                        sum(广告消耗) * 1.0 / sum(newuser_uv),
                        2
                    )
                end as "CPA(激活成本:$)",
                sum(new_pay_uv) 新增当日付费用户数,
                case
                    when sum(newuser_uv) = 0 then 0.00
                    else round(sum(new_pay_uv) * 100.0 / sum(newuser_uv), 2)
                end as 付费率,
                case
                    when sum(new_pay_uv) = 0 then 0.00
                    else round(
                        sum(广告消耗) * 1.0 / sum(new_pay_uv),
                        2
                    )
                end as 付费成本
            from
                (
                    select
                        语言,
                        投放组,
                        sum(广告消耗) 广告消耗,
                        sum(新用户收入) 新用户收入,
                        sum(impression) as impression,
                        sum(click) as click,
                        sum(newuser_uv) as newuser_uv,
                        sum(new_pay_uv) as new_pay_uv
                    from
                        t1
                        left join t2 on t1.campaign_id = t2.campaign_id
                        and t1.日期 = t2.p_date
                    group by
                        语言,
                        投放组 -- having sum(广告消耗) >0
                ) a
                left join total_recharge on a.语言 = total_recharge.语言
                and a.投放组 = total_recharge.投放组
            group by
                a.语言
        ) b
) --英语
select
    *
from
    total_all a
where
    语言 = '英语'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc,
     --日语
select
  *
from
  total_all a
where
  语言 = '日语'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc 
  --韩语
select
  *
from
  total_all a
where
  语言 = '韩语'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc 
  --泰语
select
  *
from
  total_all a
where
  语言 = '泰语'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc 
  --印尼语
select
  *
from
  total_all a
where
  语言 = '印尼语'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc 
  --法语
select
  *
from
  total_all a
where
  语言 = '法语'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc 
  --德语
select
  *
from
  total_all a
where
  语言 = '德语'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc 
  --西班牙语
select
  *
from
  total_all a
where
  语言 = '西班牙语'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc 
  --葡萄牙语
select
  *
from
  total_all a
where
  语言 = '葡萄牙语'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc 
  --中文
select
  *
from
  total_all a
where
  语言 = '中文'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc 
  --阿拉伯语
select
  *
from
  total_all a
where
  语言 = '阿拉伯语'
order by
  case
    when a.投放组 = '汇总' then 1
    else 99 end ,
  广告消耗 DESC
  --越南语
select
  *
from
  total_all a
where
  语言 = '越南语'
order by
  case
    when a.投放组 = '汇总' then 1
    else 99 end ,
  广告消耗 DESC
  --土耳其语
select
  *
from
  total_all a
where
  语言 = '土耳其语'
order by
    case
        when a.投放组 = '汇总' then 1
        when a.投放组 = '广州一组' then 2
        when a.投放组 = '广州二组' then 3
        when a.投放组 = '广州三组' then 4
        when a.投放组 = '广州四组' then 5
        when a.投放组 = '广州五组' then 6
        when a.投放组 = '广州六组' then 7
        when a.投放组 = '广州七组' then 8
        when a.投放组 = '广州八组' then 9
        when a.投放组 = '广州九组' then 10
        when a.投放组 = '深圳一组' then 11
        when a.投放组 = '深圳二组' then 12
        when a.投放组 = '深圳三组' then 13
        when a.投放组 = '深圳四组' then 14
        when a.投放组 = '深圳五组' then 15
        when a.投放组 = '深圳六组' then 16
        when a.投放组 = '深圳88组' then 17
        when a.投放组 = '杭州一组' then 18
        when a.投放组 = '杭州二组' then 19
        when a.投放组 = '南宁一组' then 20
        when a.投放组 = '南宁二组' then 21
        when a.投放组 = '南宁三组' then 22
        when a.投放组 = '南宁四组' then 23
        when a.投放组 = '南宁五组' then 24
        when a.投放组 = '南宁六组' then 25
        when a.投放组 = '南宁七组' then 26
        when a.投放组 = '南宁八组' then 27
        when a.投放组 = '南宁九组' then 28
        else 999
    end,
    广告消耗 desc