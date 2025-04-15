-------------------------------------------------------------
-- dw_area_month_budget 表中字段
--     d_month text NOT NULL,
--     area_tag text NOT NULL,
--     bud_area text NOT NULL,
--     bud_payamt numeric(20,2),
--     bud_dau integer,
--     bud_newdau2login_rate numeric(20,2),
--     bud_dau7login_rate numeric(20,2),
--     bud_roi numeric(20,2),
--     bud_arppu numeric(20,2),
--     bud_payuser_rate numeric(20,2),
--     bud_oldpayuser_avgorder numeric(20,2),
--     bud_business_rete numeric(20,2),
--     bud_act_rate numeric(20,2),
--     bud_duration_avguser numeric(20,2),
--     bud_eid_avguser numeric(20,2),
--     bud_pushview_rate numeric(20,2),
--     bud_eidfree_rate numeric(20,2),
--     bud_eidpay_rate numeric(20,2),
--     bud_dau2login_rate numeric(20,2),
--     cost_target numeric(20,2),
--     bud_roi7 numeric(20,2),
--     bud_payuser_avgorder numeric(20,2),
--     bud_subsrepay_rate numeric(20,2),
--     bud_pushclick_rate numeric(20,4),
--     bud_dau3login_rate numeric(20,2)
-------------------------------------------------------------
-- 录入字段
-- d_month
-- area_tag
-- bud_area

-- 收入           bud_payamt
-- 总ROI         bud_roi
-- roi7         bud_roi7
-- 总付费率       bud_payuser_rate
-- ARPPU        bud_arppu
-- ARPU         新增字段 bud_arpu
-- 总人均付费次数  bud_payuser_avgorder
-- 订阅会员续订率  bud_subsrepay_rate
-- 总次留        bud_dau2login_rate
-- 总7留         bud_dau7login_rate
-- push点击率    bud_pushclick_rate
-- 人均播放时长   bud_duration_avguser
-- 人均每日观看集数bud_eid_avguser
-------------------------------------------------------------

-- 查看数据
select * from dw_area_month_budget where d_month = '2025-04';

-- 增加字段 bud_arpu
ALTER TABLE dw_area_month_budget
ADD COLUMN bud_arpu numeric(20,2);

-- 删除错误数据
delete from dw_area_month_budget where d_month = '2025-04';

-- 注入4月份数据
-- 英语
insert into dw_area_month_budget(d_month,area_tag,bud_area,bud_payamt,bud_roi,bud_roi7,bud_payuser_rate,bud_arppu,bud_arpu,bud_payuser_avgorder,
                                 bud_subsrepay_rate,bud_dau2login_rate,bud_dau7login_rate,bud_pushclick_rate,bud_duration_avguser,bud_eid_avguser)
values ('2025-04','语言','英语',5095000,1.26,0.6,0.06,28,1.1,
        1.35,0.49,0.45,0.33,0.01,19,18);

-- 其他
insert into dw_area_month_budget(d_month,area_tag,bud_area,bud_payamt,bud_roi,bud_roi7,bud_payuser_rate,bud_arppu,bud_arpu,bud_payuser_avgorder,
                                 bud_subsrepay_rate,bud_dau2login_rate,bud_dau7login_rate,bud_pushclick_rate,bud_duration_avguser,bud_eid_avguser)
values ('2025-04','语言','日语',960000,1.16,0.6,0.07,28,1.3,
        1.7,0.65,0.6,0.53,0.01,38,33),
      ('2025-04','语言','韩语',530000,1.12,0.6,0.07,28,1.43,
       1.6,0.43,0.55,0.43,0.01,46,43),
      ('2025-04','语言','泰语',260000,1.15,0.6,0.05,18,0.72,
       1.25,0.47,0.37,0.24,0.01,22,22),
      ('2025-04','语言','印尼语',150000,1.12,0.6,0.03,16,0.4,
       1.3,0.35,0.36,0.25,0.01,22,18),
      ('2025-04','语言','法语',160000,1.8,0.6,0.035,24,0.65,
       1.35,0.5,0.45,0.40,0.01,20,18),
      ('2025-04','语言','西班牙语',260000,1.2,0.6,0.025,23,0.5,
       1.35,0.6,0.4,0.3,0.01,20,18),
      ('2025-04','语言','德语',176000,2,0.6,0.065,27,1.35,
       1.45,0.45,0.6,0.4,0.01,30,27),
      ('2025-04','语言','葡语',372000,1.05,0.6,0.035,18,0.45,
       1.40,0.5,0.4,0.25,0.01,20,18),
      ('2025-04','语言','中文',207000,1,0.6,0.09,25,2.1,
       2.4,0.54,0.32,0.16,0.01,22,18),
      ('2025-04','语言','阿拉伯语',150000,1,0.6,0.025,24,0.45,
       1.25,0.52,0.3,0.2,0.01,20,18),
      ('2025-04','语言','越南语',290000,1.2,0.6,0.045,17,0.65,
       1.3,0.5,0.3,0.25,0.01,20,10),
      ('2025-04','语言','土耳其语',190000,1,0.6,0.035,16,0.45,
       1.35,0.45,0.30,0.25,0.01,20,10);

-- 加入总体数据
insert into dw_area_month_budget(d_month,area_tag,bud_area,bud_payamt,bud_roi,bud_roi7,bud_payuser_rate,bud_arppu,bud_arpu,bud_payuser_avgorder,
                                 bud_subsrepay_rate,bud_dau2login_rate,bud_dau7login_rate,bud_pushclick_rate,bud_duration_avguser,bud_eid_avguser)
values ('2025-04','语言','总体',8800000,1.1,0.6,0.05,25,0.9,
        1.5,0.48,0.45,0.30,0.01,25,17);

-- 加入消耗数据
UPDATE dw_area_month_budget SET cost_target = 6250000 WHERE d_month = '2025-04' and bud_area = '总体';
UPDATE dw_area_month_budget SET cost_target = 1800000 WHERE d_month = '2025-04' and bud_area = '英语';
UPDATE dw_area_month_budget SET cost_target = 690000 WHERE d_month = '2025-04' and bud_area = '日语';
UPDATE dw_area_month_budget SET cost_target = 390000 WHERE d_month = '2025-04' and bud_area = '韩语';
UPDATE dw_area_month_budget SET cost_target = 330000 WHERE d_month = '2025-04' and bud_area = '泰语';
UPDATE dw_area_month_budget SET cost_target = 320000 WHERE d_month = '2025-04' and bud_area = '印尼语';
UPDATE dw_area_month_budget SET cost_target = 280000 WHERE d_month = '2025-04' and bud_area = '法语';
UPDATE dw_area_month_budget SET cost_target = 410000 WHERE d_month = '2025-04' and bud_area = '西班牙语';
UPDATE dw_area_month_budget SET cost_target = 280000 WHERE d_month = '2025-04' and bud_area = '德语';
UPDATE dw_area_month_budget SET cost_target = 420000 WHERE d_month = '2025-04' and bud_area = '葡语';
UPDATE dw_area_month_budget SET cost_target = 400000 WHERE d_month = '2025-04' and bud_area = '中文';
UPDATE dw_area_month_budget SET cost_target = 290000 WHERE d_month = '2025-04' and bud_area = '阿拉伯语';
UPDATE dw_area_month_budget SET cost_target = 330000 WHERE d_month = '2025-04' and bud_area = '越南语';
UPDATE dw_area_month_budget SET cost_target = 310000 WHERE d_month = '2025-04' and bud_area = '土耳其语';


-- 修改 bud_payuser_rate 的精度
-- 错误方法 不能通过临时列
-- ALTER TABLE dw_area_month_budget ALTER COLUMN bud_payuser_rate TYPE numeric(20,4); 目前不支持这么做

-- 添加一个具有新类型的新字段：tmp_dw_area_month_budget
-- ALTER TABLE dw_area_month_budget ADD COLUMN tmp_bud_payuser_rate NUMERIC(20,4);

-- 将原始字段的数据转换并存储到临时字段中。
-- UPDATE dw_area_month_budget SET tmp_bud_payuser_rate = bud_payuser_rate::NUMERIC(20,4);

-- 移除旧字段并重命名临时字段：
--ALTER TABLE "DROP COLUMN" is not supported now
--  ALTER TABLE dw_area_month_budget DROP COLUMN bud_payuser_rate;
--  ALTER TABLE dw_area_month_budget RENAME COLUMN bud_payuser_rate TO bud_payuser_rate_old;
--  ALTER TABLE dw_area_month_budget RENAME COLUMN tmp_bud_payuser_rate TO bud_payuser_rate;
--