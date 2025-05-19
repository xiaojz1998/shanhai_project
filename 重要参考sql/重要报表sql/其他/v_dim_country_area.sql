------------------------------------------
-- file: v_dim_country_area.sql
-- author: xiaoj
-- time: 2025/5/15 10:15
-- description:
------------------------------------------
SELECT
    COALESCE(c.id, a.id::bigint) AS id,
    COALESCE(c.name, a.country_name) AS country_name,
    COALESCE(c.code, a.country_code) AS country_code,
    COALESCE(c.sort, a.sort::bigint) AS sort,
    CASE
        WHEN c.code IS NULL THEN '补充'::text
        ELSE '正常'::text
    END AS dcomment,
    a.area,
    a.area2,
    a.country_grade,
    COALESCE(o.label, 0::bigint) AS label
FROM middle_country c
FULL JOIN tmp_country_input a ON c.code = a.country_code
FULL JOIN "oversea-api_osd_country" o ON c.code = o.code;