---------------------------------------------
-- File: 2025-6.sql
-- Time: 2025/6/9 11:55
-- User: xiaoj
-- Description:  
---------------------------------------------

-------------------------------------------------------------
-- dw_area_month_budget 表中字段
--  d_month text NOT NULL,
--  area_tag text NOT NULL,
--  bud_area text NOT NULL,
--  bud_payamt numeric(20,2),
--  bud_dau integer,
--  bud_newdau2login_rate numeric(20,2),
--  bud_dau7login_rate numeric(20,2),
--  bud_roi numeric(20,2),
--  bud_arppu numeric(20,2),
--  bud_payuser_rate_old numeric(20,2),
--  bud_oldpayuser_avgorder numeric(20,2),
--  bud_business_rete numeric(20,2),
--  bud_act_rate numeric(20,2),
--  bud_duration_avguser numeric(20,2),
--  bud_eid_avguser numeric(20,2),
--  bud_pushview_rate numeric(20,2),
--  bud_eidfree_rate numeric(20,2),
--  bud_eidpay_rate numeric(20,2),
--  bud_dau2login_rate numeric(20,2),
--  cost_target numeric(20,2),
--  bud_roi7 numeric(20,2),
--  bud_payuser_avgorder numeric(20,2),
--  bud_subsrepay_rate numeric(20,2),
--  bud_pushclick_rate numeric(20,4),
--  bud_dau3login_rate numeric(20,2),
--  bud_arpu numeric(20,2),
--  bud_payuser_rate numeric(20,4),
--  bud_newuser_payamt numeric(20,2),
--  bud_olduser_payamt numeric(20,2),
--  bud_newuser_roi numeric(20,4)
-------------------------------------------------------------

-- 录入字段
-- 总收入              bud_payamt
-- 新用户收入          bud_newuser_payamt
-- 老用户收入          bud_olduser_payamt
-- DAU               bud_dau
-- 总roi             bud_roi
-- 新用户累计ROI      bud_newuser_roi
-- 总付费率          bud_payuser_rate
-- ARPPU            bud_arppu
-- ARPU             bud_arpu
-- 总人均付费次数      bud_payuser_avgorder
-- 订阅会员续订率      bud_subsrepay_rate
-- 新用户次留         bud_newdau2login_rate
-- 总7留              bud_dau7login_rate
-- PUSH点击率         bud_pushclick_rate
-- 人均播放时长(分钟)   bud_duration_avguser
-- 人均每日观看集数     bud_eid_avguser
-- 消耗               cost_target

-------------------------------------------------------------
-- 查看数据
select * from dw_area_month_budget where d_month = '2025-06';

insert into dw_area_month_budget(d_month,area_tag,bud_area,bud_payamt,bud_newuser_payamt,bud_olduser_payamt,bud_dau,bud_roi,bud_newuser_roi,bud_payuser_rate,bud_arppu,bud_arpu,
                                 bud_payuser_avgorder,bud_subsrepay_rate,bud_newdau2login_rate,bud_dau7login_rate,bud_pushclick_rate,bud_duration_avguser,bud_eid_avguser,cost_target)
values ('2025-06','语言','总体',10000000,3600000,6400000,400000,1.25,0.55,0.05,
        22,0.7,1.5,0.5,0.25,0.27,0.01,25,21,8400000),
       ('2025-06','语言','英语',3200000,1152000,2048000,104000,1.85,0.8,0.06,
        25,1.05,1.4,0.53,0.25,0.27,0.01,21,20,1990000),
       ('2025-06','语言','日语',1000000,360000,640000,23000 ,1.35,0.65,0.065,
        26,1.25,1.5,0.56,0.30,0.45,0.01,42,37,920000),
       ('2025-06','语言','韩语',600000,216000,384000,11000,1.2,0.5,0.065,
        26,1.32,1.5,0.38,0.28,0.38,0.01,61,62,780000),
       ('2025-06','语言','泰语',400000,144000,256000,20000,1.25,0.5,0.055,
        17,0.65,1.35,0.46,0.25,0.22,0.01,23,23,560000),
       ('2025-06','语言','印尼语',500000,180000,320000,50500,1,0.4,0.02,
        15,0.2,1.3,0.31,0.25,0.16,0.01,16,16,510000),
       ('2025-06','语言','法语',400000,144000,256000,20000,1.25,0.6,0.025,
        25,0.45,1.40,0.5,0.25,0.25,0.01,15,16,190000),
       ('2025-06','语言','西班牙语',1000000,360000,640000,51000,1.05,0.6,0.03,
        21,0.50,1.35,0.54,0.25,0.20,0.01,16,15,660000),
       ('2025-06','语言','德语',400000,144000,256000,4000,1.15,0.6,0.06,
        25,1.38,1.40,0.44,0.25,0.40,0.01,31,32,190000),
       ('2025-06','语言','葡语',400000,144000,256000,24000,1.25,0.6,0.04,
        16,0.48,1.35,0.56,0.25,0.25,0.01,17,18,440000),
       ('2025-06','语言','中文',500000,180000,320000,4500,1,0.45,0.085,
        26,2.0,1.4,0.55,0.20,0.30,0.01,30,26,340000),
       ('2025-06','语言','阿拉伯语',700000 ,252000,448000,45000 ,1.21,0.45,0.025,
        20,0.38,1.40,0.55,0.25,0.20,0.01,16,18,580000),
       ('2025-06','语言','越南语',500000,180000,320000,28000,1.5,0.5,0.05,
        16,0.50,1.35,0.58,0.20,0.20,0.01,17,19,540000),
       ('2025-06','语言','土耳其语',400000,144000,256000,15000,1.3,0.5,0.04,
        18,0.45,1.30,0.57,0.20,0.20,0.01,17,20,500000)

