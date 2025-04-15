-- 增加一个字段
ALTER TABLE dw_area_month_budget
ADD COLUMN bud_dau3login_rate numeric(20,2);

-- 添加数据
-- 查看总数量
select count(*) from dw_area_month_budget where d_month='2025-03';

-- update 数据
select * from  dw_area_month_budget where d_month='2025-03' and bud_area = '土耳其语';

UPDATE dw_area_month_budget
SET bud_dau3login_rate = 0.18
WHERE d_month='2025-03' and bud_area = '土耳其语';

-- 看总体
select
    bud_area,bud_dau3login_rate
    from  dw_area_month_budget where d_month='2025-03';