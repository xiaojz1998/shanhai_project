---------------------------------------------
-- File: 测试.sql
-- Time: 2025/6/10 16:36
-- User: xiaoj
-- Description:  
---------------------------------------------
-- 查看数据格式
select
    week_end - week_start
from tmp.dw_retention_detail_tmp01
limit 100;

-- 转换时间格式
select to_char(current_timestamp, 'IYYY"-"IW')

-- 月相减
select to_char(to_date('2025-05','yyyy-mm') - interval '1 month','yyyy-mm')
