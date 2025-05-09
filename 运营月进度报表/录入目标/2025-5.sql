------------------------------------------
-- file: 2025-5.sql
-- author: xiaoj
-- time: 2025/5/7 16:02
-- description:
------------------------------------------


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
-- 新用户收入      bud_newuser_payamt
-- 老用户收入      bud_olduser_payamt
-- 总ROI         bud_roi
-- 新用户累计roi   bud_newuser_roi
-- 总付费率       bud_payuser_rate
-- ARPPU        bud_arppu
-- ARPU         bud_arpu
-- 总人均付费次数  bud_payuser_avgorder
-- 订阅会员续订率  bud_subsrepay_rate
-- 新次留              bud_newdau2login_rate
-- 总7留              bud_dau7login_rate
-- push点击率          bud_pushclick_rate
-- 人均播放时长       bud_duration_avguser
-- 人均每日观看集数     bud_eid_avguser
-- 消耗              cost_target
-------------------------------------------------------------

--加入新字段
-- 新用户收入            bud_newuser_payamt              numeric(20,4)
-- 老用户收入            bud_olduser_payamt              numeric(20,4)
-- 新用户累计roi         bud_newuser_roi                 numeric(20,4)

ALTER TABLE dw_area_month_budget ADD COLUMN bud_newuser_payamt numeric(20,2);
ALTER TABLE dw_area_month_budget ADD COLUMN bud_olduser_payamt numeric(20,2);
ALTER TABLE dw_area_month_budget ADD COLUMN bud_newuser_roi numeric(20,4);

COMMENT ON COLUMN dw_area_month_budget.bud_newuser_payamt IS '新用户收入';
COMMENT ON COLUMN dw_area_month_budget.bud_olduser_payamt IS '老用户收入';
COMMENT ON COLUMN dw_area_month_budget.bud_newuser_roi IS '新用户累计roi';

-- 查看数据
select * from dw_area_month_budget where d_month = '2025-05';

-- 录入5月数据
insert into dw_area_month_budget(d_month,area_tag,bud_area,bud_payamt,bud_newuser_payamt,bud_olduser_payamt,bud_roi,bud_newuser_roi,bud_payuser_rate,
                                 bud_arppu,bud_arpu,bud_payuser_avgorder,bud_subsrepay_rate,bud_newdau2login_rate,bud_dau7login_rate,bud_pushclick_rate,
                                 bud_duration_avguser,bud_eid_avguser,cost_target)
values ('2025-05','语言','总体',8300000,4000000,4300000,1.1,0.58,0.05,
        25,0.69,1.5,0.48,0.4,0.3,0.01,25,18,5450000),
       ('2025-05','语言','英语',2000000,963855 ,1036145 ,1.5,0.55,0.06,
        28,1.05,1.4,0.5,0.35,0.33,0.01,21,20,1200000),
       ('2025-05','语言','日语',700000,337349 ,362651 ,1.72,0.55,0.07,
        27,1.3,1.5,0.56,0.4,0.33,0.01,40,35,530000),
       ('2025-05','语言','韩语',420000,202410 ,217590 ,1.3,0.45,0.075,
        27,1.41,1.5,0.43,0.4,0.35,0.01,60,58,290000),
       ('2025-05','语言','泰语',590000,284337 ,305663 ,1.1,0.57,0.05,
        17,0.6,1.35,0.45,0.40,0.3,0.01,21,22,360000),
       ('2025-05','语言','印尼语',765000,368675,396325 ,1,0.57,0.03	,
        16,0.3,1.3,0.32,0.4,0.3	,0.01,	20,18,380000),
       ('2025-05','语言','法语',382500,184337 ,198163 ,1.25,0.6,0.035,
        22,0.45,1.45,0.46,0.3,0.4,0.01,	17,16,370000),
       ('2025-05','语言','西班牙语',765000,368675 ,396325 ,1.26,0.6,0.03,
        21,0.43,1.35,0.54,0.30,0.25,0.01,15,14,500000),
       ('2025-05','语言','德语',297500,143373 ,154127 ,2,0.6,0.06,
        25,1.26,1.40,0.44,0.35,0.25,0.01,33,32,200000),
       ('2025-05','语言','葡语',382500,184337 ,198163 ,1.8,0.6,0.035,
        22,0.42,1.35,0.55,0.40,0.25,0.01,20,18,200000),
       ('2025-05','语言','中文',467500,225301 ,242199 ,1,0.55,0.085,
        28,1.9 ,1.4 ,0.50,0.25,0.20,0.01,25,24,360000),
      ('2025-05','语言','阿拉伯语',722500,348193 ,374307 ,1,0.7,0.025,
       25,0.40,1.40,0.5,0.3,0.2,0.01,15,16,500000),
      ('2025-05','语言','越南语',425000,204819 ,220181 ,1.8,0.55,0.06,
       17,0.75,1.35,0.58,0.40,0.25,0.01,22,21,270000),
      ('2025-05','语言','土耳其语',382500,184337 ,198163 ,1.2,0.6,0.035,
       18,0.40,1.30,0.5,0.3,0.25,0.01,18,16,290000);




