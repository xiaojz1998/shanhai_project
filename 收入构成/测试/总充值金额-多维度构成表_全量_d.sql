------------------------------------------
-- file: 总充值金额-多维度构成表_全量_d.sql
-- author: xiaoj
-- time: 2025/5/16 14:50
-- description:
------------------------------------------

-- 未完成，因为已经从holo转向mc开发

set timezone ='UTC-0';
with tmp_pay as (
    select
        
    from all_order_log
    where environment = 1                   -- 生产
        and created_date>=20240701          -- 日期过滤
        and (os ='android' or os = 'ios')   -- 操作系统
        and status = 1                      -- 支付成功
)