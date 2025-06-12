---------------------------------------------
-- File: 本土首发测试剧目.sql
-- Time: 2025/6/11 10:44
-- User: xiaoj
-- Description:  
---------------------------------------------
set timezone = 'UTC-0';

TRUNCATE TABLE public.tmp_dw_video_test_mainland;
INSERT INTO public.tmp_dw_video_test_mainland
WITH newuser_tb AS (
    SELECT uid, d_date, v_date, country_code, ad_channel
    FROM public.dwd_user_info
),
--找出在测试范围内，且在6个小时整的阶段的剧
test_drama as (
    SELECT *,
        (current_timestamp::timestamp with time zone AT TIME ZONE 'GMT') end_time,
        concat('第', (hours / 6), '阶段')  time_segment
    FROM (
        SELECT drama_id  AS vid,
            test_time,
            dim_test_mainland_drama.test_time::timestamp with time zone AT TIME ZONE 'GMT+8' AS test_0_time,
            (DATE_PART('day',(current_timestamp::timestamp + interval '-1 HOUR')::timestamp with time zone AT TIME ZONE 'GMT' -test_time::timestamp with time zone AT TIME ZONE 'GMT+8') * 24 +
                        date_part('hour',(current_timestamp::timestamp + interval '-1 HOUR')::timestamp with time zone AT TIME ZONE 'GMT' - test_time::timestamp with time zone AT TIME ZONE 'GMT+8'))::int  hours
        from public.dim_test_mainland_drama
    ) t
    where hours % 6 = 0
        and hours > 0
        AND hours <= 48
),
tb_tack as (
    SELECT to_timestamp(a.created_at)::date AS d_date,
        a.vid,
        a.uid,
        COUNT(CASE WHEN event IN (1, 2, 13, 14) AND eid > 0 THEN uid ELSE NULL END) AS watch_pv
    FROM public.app_user_track_log a
    iNNER JOIN "oversea-api_osd_video_episodes" b ON a.eid = b.id
    LEFT JOIN public."oversea-api_osd_videos" v1 ON a.vid = v1.id
    inner join test_drama td on a.vid = td.vid --只统计在测试范围的剧
    WHERE
--      to_timestamp(a.created_at) >= td.test_0_time AND
        to_timestamp(a.created_at) < td.end_time
        AND event IN (1, 2, 13, 14, 6)
        AND a.vid = b.vid
        AND to_timestamp(a.created_at)::date >= (current_date + interval '-3 day')::date
        AND to_timestamp(a.created_at) < date_trunc('hour', now() - interval '1 hour')
        GROUP BY to_timestamp(a.created_at)::date,a.vid,a.uid
),
tmp_vid_log AS (
    SELECT d.end_time::date d_date,
        COALESCE(nn.ad_channel, '未知') AS ad_channel,
        n1.vid::text AS vid
    FROM tb_tack n1
    LEFT JOIN newuser_tb nn ON n1.uid::text = nn.uid
    inner join test_drama d on n1.vid=d.vid
    GROUP BY d.end_time::date, COALESCE(nn.ad_channel, '未知'), n1.vid
),
tb_uv as (
    select t.vid,
        sum(watch_uv)     watch_uv,
        sum(new_watch_uv) new_watch_uv,
        sum(old_watch_uv) old_watch_uv
    FROM (
        SELECT n1.d_date d_date,
            n1.vid::text AS vid,
            COUNT(DISTINCT case when n1.watch_pv > 0 then n1.uid end)       watch_uv,
            COUNT(DISTINCT CASE WHEN nu.uid IS NOT NULL THEN n1.uid END) AS new_watch_uv,
            COUNT(DISTINCT CASE WHEN nu.uid IS NULL THEN n1.uid END)     AS old_watch_uv
        FROM tb_tack n1
        LEFT JOIN newuser_tb nu ON n1.uid::bigint = nu.uid::bigint AND n1.d_date = nu.d_date::date
        GROUP BY n1.d_date, n1.vid
    ) t
    inner join test_drama d on t.vid = d.vid::text
    group by t.vid
),
tb_order as (
    SELECT to_timestamp(a.created_at)::date o_date,
        td.end_time::date AS d_date,
        goods_id          AS vid,
        uid,
        SUM(money) * 0.01 AS pay_amt
    FROM public.all_order_log a
    inner join test_drama td on a.goods_id = td.vid
    WHERE status = 1
        AND environment = 1
--      and to_timestamp(created_at) > td.test_0_time
        AND to_timestamp(a.created_at) < td.end_time
        AND to_timestamp(created_at)::date >= '2025-03-04'
        AND to_timestamp(created_at) < date_trunc('hour', now() - interval '1 hour')
    GROUP BY td.end_time::date,
        to_timestamp(a.created_at)::date,
        goods_id,
        uid
),
    --  tmp_vid_pay AS (SELECT p1.d_date,
    --                         COALESCE(nn.ad_channel, '未知')                                        AS ad_channel,
    --                         p1.vid::text                                                           AS vid,
    --                         -- COUNT(DISTINCT p1.uid)                                                 AS pay_uv,
    --                         -- COUNT(DISTINCT CASE WHEN nu.uid IS NOT NULL THEN p1.uid ELSE NULL END) AS new_pay_uv,
    --                         -- COUNT(DISTINCT CASE WHEN nu.uid IS NULL THEN p1.uid ELSE NULL END)     AS old_pay_uv,
    --                         SUM(p1.pay_amt)                                                        AS pay_amt,
    --                         SUM(CASE WHEN nu.uid IS NOT NULL THEN p1.pay_amt ELSE 0 END)           AS new_pay_amt,
    --                         SUM(CASE WHEN nu.uid IS NULL THEN p1.pay_amt ELSE 0 END)               AS old_pay_amt
    --                  FROM tb_order p1
    --                           LEFT JOIN newuser_tb nn ON p1.uid::text = nn.uid
    --                           LEFT JOIN newuser_tb nu ON p1.uid::text = nu.uid AND p1.o_date = nu.d_date::date
    --                  GROUP BY p1.d_date,
    --                           COALESCE(nn.ad_channel, '未知'), p1.vid),
tb_pay_uv as (
        SELECT t.vid,
            sum(pay_amt)    pay_amt,
            sum(new_pay_amt)    new_pay_amt,
            sum(old_pay_amt)    old_pay_amt,
            sum(pay_uv)     pay_uv,
            sum(new_pay_uv) new_pay_uv,
            sum(old_pay_uv) old_pay_uv
        from (
            SELECT p1.o_date,
                p1.vid::text AS vid,
                SUM(p1.pay_amt) AS pay_amt,
                SUM(CASE WHEN nu.uid IS NOT NULL THEN p1.pay_amt ELSE 0 END) AS new_pay_amt,
                SUM(CASE WHEN nu.uid IS NULL THEN p1.pay_amt ELSE 0 END) AS old_pay_amt,
                COUNT(DISTINCT p1.uid) AS pay_uv,
                COUNT(DISTINCT CASE WHEN nu.uid IS NOT NULL THEN p1.uid ELSE NULL END) AS new_pay_uv,
                COUNT(DISTINCT CASE WHEN nu.uid IS NULL THEN p1.uid ELSE NULL END) AS old_pay_uv
            FROM tb_order p1
            LEFT JOIN newuser_tb nn ON p1.uid::text = nn.uid
            LEFT JOIN newuser_tb nu ON p1.uid::text = nu.uid AND p1.o_date = nu.d_date::date
            GROUP BY p1.o_date,
                    p1.vid::text
        ) t
        inner join test_drama td on t.vid = td.vid::text
        GROUP BY t.vid
),
tmp_camp_cost AS (
        SELECT td.end_time::date AS d_date,
            UPPER(ad_channel) AS ad_channel,
            COALESCE(tc.vid, 'UNKNOWN') AS vid,
            SUM(impression) AS impression,
            SUM(click) AS click,
            SUM(cost_amount) * 0.0001 AS ad_cost
        FROM public.ad_cost_data_log tb
        LEFT JOIN public.v_dim_ad_campaign_info tc ON tb.campaign_id = tc.campaign_id
        INNER JOIN test_drama td on tc.vid = td.vid::varchar
--      WHERE created_date >= '20240701'
        where
--      TO_TIMESTAMP(tb.created_at) >= td.test_0_time AND
            TO_TIMESTAMP(tb.created_at) < td.end_time
        GROUP BY td.end_time::date,
                UPPER(ad_channel),
                tc.vid
),
optimize_count AS (
        SELECT vid,
            optimize_group,
            COUNT(DISTINCT LOWER(optimize)) AS optimizer_count
        FROM public.v_dim_ad_campaign_info
        WHERE vid ~ '^[0-9]+$'
        GROUP BY vid, optimize_group
),
creative_indicators AS (
        SELECT td.end_time::date  AS d_date,
            UPPER(ad_channel)  AS ad_channel,
            COALESCE(tc.vid, 'UNKNOWN') AS vid,
            SUM(cost) / 100.0           AS cost,
            SUM(af_conversion)    AS af_conversion,
            SUM(af_revenue_1) / 100.0  AS af_revenue_1
        FROM creative_indicators_data_xmp_log tb
        LEFT JOIN public.v_dim_ad_campaign_info tc ON tb.campaign_id = tc.campaign_id
        INNER JOIN test_drama td on tc.vid = td.vid::varchar
--      WHERE TO_TIMESTAMP(created_at)::date >= '2024-07-01'
        where
--      TO_TIMESTAMP(created_at) >= td.test_0_time AND
            TO_TIMESTAMP(created_at) < td.end_time
        GROUP BY td.end_time::date,
                UPPER(ad_channel),
                tc.vid
)
SELECT t.*,
       td.test_time,
       time_segment
FROM (
    SELECT tvl.d_date,
        tvl.ad_channel,
        tvl.vid,
        v1.name  AS "剧名称",
        CASE
            WHEN v1.source = '1' THEN '国内'
            WHEN v1.source = '2' THEN '外采'
            WHEN v1.source = '3' THEN '对投'
            WHEN v1.source = '4' THEN '自制'
            ELSE '其他'END  AS "剧目来源",
        CASE
            WHEN v1.type = '1' THEN '翻译剧'
            WHEN v1.type = '2' THEN '本土剧'
            WHEN v1.type = '3' THEN '中文剧'
            ELSE '其他'END   AS "剧目类别",
        CASE
            WHEN v1.complex = 1 THEN '男频'
            WHEN v1.complex = 2 THEN '女频'
            ELSE '其他'END  AS "综合",
        CASE
            WHEN v1.category = 1 THEN '现代言情'
            WHEN v1.category = 2 THEN '奇幻言情'
            WHEN v1.category = 3 THEN '青春校园'
            WHEN v1.category = 4 THEN '玄幻/仙侠'
            WHEN v1.category = 5 THEN '悬疑/惊悚'
            WHEN v1.category = 6 THEN '科幻'
            WHEN v1.category = 7 THEN 'LGBT+'
            WHEN v1.category = 8 THEN '萌宝/奶爸'
            WHEN v1.category = 9 THEN '古代言情'
            WHEN v1.category = 10 THEN '年代言情'
            WHEN v1.category = 11 THEN '家庭伦理'
            WHEN v1.category = 12 THEN '情色'
            WHEN v1.category = 13 THEN '其他'
            ELSE '未知' END  AS "作品分类",
        uv.watch_uv,
        uv.new_watch_uv,
        uv.old_watch_uv,
        puv.pay_uv,
        puv.new_pay_uv,
        puv.old_pay_uv,
        puv.pay_amt,
        puv.new_pay_amt,
        puv.old_pay_amt,
        tcc.impression,
        tcc.click,
        tcc.ad_cost,
        ci.cost,
        ci.af_conversion,
        ci.af_revenue_1,
        STRING_AGG(oc.optimize_group, ', ') AS 测试组,
        STRING_AGG(oc.optimizer_count::text, ', ') AS 测试人数
    FROM (
        SELECT *
        FROM tmp_vid_log
        WHERE ad_channel in ('FB', 'TT')
    ) tvl
            --    LEFT JOIN tmp_vid_pay tvp  --付款不分渠道
            --              ON
            --                  tvl.d_date = tvp.d_date
            --                  AND tvl.ad_channel = tvp.ad_channel
            --                  AND tvl.vid = tvp.vid
    LEFT JOIN tmp_camp_cost tcc ON tvl.d_date = tcc.d_date AND tvl.ad_channel = tcc.ad_channel AND tvl.vid = tcc.vid
    LEFT JOIN optimize_count oc ON tvl.vid = oc.vid
    LEFT JOIN public."oversea-api_osd_videos" v1 ON tvl.vid = v1.id::text
    LEFT JOIN public."oversea-api_osd_categories" v2 ON v1.category_id = v2.id
    LEFT JOIN creative_indicators ci ON tvl.d_date = ci.d_date AND tvl.ad_channel = ci.ad_channel AND tvl.vid = ci.vid
    LEFT JOIN tb_uv uv on tvl.vid = uv.vid::text
    LEFT JOIN tb_pay_uv puv on tvl.vid = puv.vid::text

      WHERE 1 = 1
        -- AND tvl.vid IN ('4145', '4184', '3925', '3868')
        -- AND tvl.ad_channel in ('FB', 'TT')
    GROUP BY tvl.d_date,
             tvl.ad_channel,
             tvl.vid,
             v1.name,
             v1.source,
             v1.type,
             v1.complex,
             v1.category,
             uv.watch_uv,
             uv.new_watch_uv,
             uv.old_watch_uv,
             puv.pay_uv,
             puv.new_pay_uv,
             puv.old_pay_uv,
             puv.pay_amt,
             puv.new_pay_amt,
             puv.old_pay_amt,
             tcc.impression,
             tcc.click,
             tcc.ad_cost,
             ci.cost,
             ci.af_conversion,
             ci.af_revenue_1
    ORDER BY uv.watch_uv DESC
    ) t
left join test_drama td on t.vid = td.vid::varchar;


delete from public.dw_video_test_mainland_new e1
USING public.tmp_dw_video_test_mainland e2
WHERE e1.d_date = e2.d_date
  AND e1.vid = e2.vid
  and e1.ad_channel = e2.ad_channel
  and e1.time_segment = e2.time_segment;

insert into public.dw_video_test_mainland_new
select d_date,
       ad_channel,
       vid,
       vid_name,
       vid_source,
       vid_category,
       vid_complex,
       vid_style,
       wathc_uv,
       new_watch_uv,
       old_watch_uv,
       pay_uv,
       new_pay_uv,
       old_pay_uv,
       pay_amt,
       new_pay_amt,
       old_pay_amt,
       impression,
       click,
       ad_cost,
       xmp_cost,
       af_conversion,
       af_revenue_1,
       test_group,
       test_num,
       test_time,
       time_segment
from (
    select *,
        row_number() over (PARTITION BY vid,time_segment,ad_channel order by d_date DESC ) r
    from public.tmp_dw_video_test_mainland
) t
where r = 1;