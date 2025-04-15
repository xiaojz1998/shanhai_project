-------------------------------------------------------------
--  配合最新全量表和增量表
-------------------------------------------------------------
-- cal01表，中间表存赠币数据和当天充值购买的K币总数
drop table if exists tmp.dw_coin_consume_cal01;
CREATE TABLE tmp.dw_coin_consume_cal01 (
    "日期" date,
    "区域" character varying(100),
    "国家" character varying(100),
    "用户类型" character varying(50),
--
    "累计充值赠币" numeric(20,2), --充值赠送币
    "当天充值购买的K币总数" numeric(20,2),
    "累计签到赠币"  numeric(20,2),        -- 签到赠送币数量
    "赠送币使用数量" numeric(20,2),     --赠送币使用数量
    "累计做任务赠币" numeric(20,2), --做任务赠币总数
    "累计看阶梯广告赠币" numeric(20,2), -- 阶梯广告赠币
    "累计看firefly广告赠币" numeric(20,2),
    "累计看adcloud广告赠币" numeric(20,2),
    "累计看签到广告赠币" numeric(20,2),
    "累计奖励广告赠币" numeric(20,2),
    "累计看剧时长任务赠币"numeric(20,2), --累计看剧任务赠币
    "累计看剧任务赠币" numeric(20,2),
    "累计分享赠币" numeric(20,2),        --累计分享赠币
    "累计看特定的剧集赠送币" numeric(20,2), --累计看特定的剧集赠送币
    "累计特定剧集时长赠送币" numeric(20,2), --累计特定剧集时长赠送币
    "累计社媒活动赠币" numeric(20,2)-- 累计社媒赠币
);
-- cal01 存bal_info数据
drop table if exists tmp.dw_coin_consume_cal02;
create table if not exists tmp.dw_coin_consume_cal02 (
    data_date date,
    area character varying(100),
    country_name character varying(100),
    user_type character varying(50),
--
    balance numeric(20,2),
    lag_1d_balance numeric(20,2),
    recharge_coin numeric(20,2),
    lag_1d_recharge_coin numeric(20,2),
    balance_give_expired numeric(20,2),
    total_balance_give numeric(20,2),
    balance_give numeric(20,2),
    lag_1d_balance_give numeric(20,2)
);

--------------------------------------------------------
-- 建表 dw_coin_consume_1.0
--------------------------------------------------------
drop table if exists public.dw_coin_consume_1;
CREATE TABLE if not exists public.dw_coin_consume_1 (
    "日期" date,
    "区域" character varying(100),
    "国家" character varying(100),
    "用户类型" character varying(50),
    --
    "k币余额" numeric(20,2),
    "K币余额日变动量" numeric(20,2),
    "购买币余额" numeric(20,2),
    "赠送币余额" numeric(20,2),
    "赠送币回收数量" numeric(20,2),
    "赠送币使用数量" numeric(20,2),
    "当天消耗的充值K币" numeric(20,2),
    "当天充值购买的K币总数" numeric(20,2),
    "累计购买K币" numeric(20,2),
    "累计赠送K币" numeric(20,2),
    "累计总k币" numeric(20,2),
    "累计充值赠币" numeric(20,2),
    "累计签到赠币" numeric(20,2),
    "累计看阶梯广告赠币" numeric(20,2),
    "累计看firefly广告赠币" numeric(20,2),
    "累计看adcloud广告赠币" numeric(20,2),
    "累计看签到广告赠币" numeric(20,2),
    "累计奖励广告赠币" numeric(20,2),
    "累计看剧时长任务赠币" numeric(20,2),
    "累计看剧任务赠币" numeric(20,2),
    "累计分享赠币" numeric(20,2),
    "累计看特定的剧集赠送币" numeric(20,2),
    "累计特定剧集时长赠送币" numeric(20,2),
    "累计社媒活动赠币" numeric(20,2),
    "累计其他任务赠币" numeric(20,2)
);