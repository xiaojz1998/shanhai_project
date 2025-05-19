------------------------------------------
-- file: v_dim_ad_campaign_info.sql
-- author: xiaoj
-- time: 2025/5/14 18:48
-- description:
------------------------------------------
with tmp_a as (
     SELECT ad_cost_data_log.campaign_id,
            ad_cost_data_log.campaign_name,
            row_number() OVER (PARTITION BY ad_cost_data_log.campaign_id ORDER BY ad_cost_data_log.created_at DESC) AS rn
    FROM ad_cost_data_log
    WHERE 1 = 1 AND ad_cost_data_log.created_date >= 20240701
),
tmp_a as (
    SELECT a.campaign_id,
           a.campaign_name,
           split_part(a.campaign_name::text, '_'::text, 5) AS vid,
           split_part(a.campaign_name::text, '_'::text, 8) AS optimize_group,
           split_part(a.campaign_name::text, '_'::text, 9) AS optimize
    FROM tmp_a
    WHERE a.rn = 1
)
SELECT
    b.campaign_id,
    b.campaign_name,
    b.vid,
    b.optimize_group,
    b.optimize,
    b1.name AS vid_name,
    b2.name AS lang_name,
    c1.lang_code AS lang
FROM  tmp_b
    LEFT JOIN "oversea-api_osd_videos" b1 ON b.vid = b1.id::text
    LEFT JOIN "oversea-api_osd_categories" b2 ON b1.category_id = b2.id
    LEFT JOIN "oversea-api_osd_lang" c1 ON
                CASE
                    WHEN b2.name = '印尼语'::text THEN '印度尼西亚语'::text
                    WHEN b2.name = '中文简体'::text THEN '简体中文'::text
                    ELSE b2.name
                END = c1.name;