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
    "累计看剧任务赠币" numeric(20,2),
    "累计分享赠币" numeric(20,2),
    "累计看特定的剧集赠送币" numeric(20,2),
    "累计特定剧集时长赠送币" numeric(20,2),
    "累计社媒活动赠币" numeric(20,2),
    "累计其他任务赠币" numeric(20,2)
);
--------------------------------------------------------
-- 注入 dw_coin_consume_1
--------------------------------------------------------
truncate table public.dw_coin_consume_1;
insert into public.dw_coin_consume_1
with cal01 as (
    select
        *
    from tmp.dw_coin_consume_cal01
), cal02 as (
    select
        *
    from tmp.dw_coin_consume_cal02
)
select
    --
    cal02.data_date as "日期",
    cal02.area as "区域",
    cal02.country_name as "国家",
    cal02.user_type as "用户类型",
    --
    coalesce(balance,0) as "k币余额",
    COALESCE(balance, 0) - COALESCE(lag_1d_balance, 0) AS "K币余额日变动量",
    COALESCE(balance, 0) - COALESCE(balance_give, 0) AS "购买币余额",
    COALESCE(balance_give, 0) AS "赠送币余额",
    COALESCE(balance_give_expired, 0) AS "赠送币回收数量",
    coalesce("赠送币使用数量",0) as "赠送币使用数量",
    (COALESCE(lag_1d_balance, 0) - COALESCE(lag_1d_balance_give, 0))
          + COALESCE("当天充值购买的K币总数", 0) - (COALESCE(balance, 0) - COALESCE(balance_give, 0)
      ) AS "当天消耗的充值K币",
    coalesce("当天充值购买的K币总数", 0) as "当天充值购买的K币总数",
    coalesce(recharge_coin,0) as "累计购买K币",
    coalesce("累计做任务赠币",0)+coalesce("累计充值赠币",0)
          +coalesce("累计签到赠币",0) as "累计赠送K币",
    coalesce(recharge_coin,0)+coalesce("累计做任务赠币",0)+coalesce("累计充值赠币",0)
          +coalesce("累计签到赠币",0) as "累计总k币",
    coalesce("累计充值赠币",0) as "累计充值赠币",
    coalesce("累计签到赠币",0) as "累计签到赠币",
    coalesce("累计看阶梯广告赠币",0) as "累计看阶梯广告赠币",
    coalesce("累计看firefly广告赠币",0) as "累计看firefly广告赠币",
    coalesce("累计看adcloud广告赠币",0) as "累计看adcloud广告赠币",
    coalesce("累计看签到广告赠币",0) as "累计看签到广告赠币",
    coalesce("累计看剧任务赠币",0) as "累计看剧任务赠币",
    coalesce("累计分享赠币",0) as "累计分享赠币",
    coalesce("累计看特定的剧集赠送币",0) as "累计看特定的剧集赠送币",
    coalesce("累计特定剧集时长赠送币",0) as "累计特定剧集时长赠送币",
    coalesce("累计社媒活动赠币",0) as "累计社媒活动赠币",
    coalesce("累计做任务赠币",0)-coalesce("累计看阶梯广告赠币",0)-coalesce("累计看firefly广告赠币",0)
          -coalesce("累计看adcloud广告赠币",0)-coalesce("累计看签到广告赠币",0)
          -coalesce("累计看剧任务赠币",0)-coalesce("累计分享赠币",0)
          -coalesce("累计看特定的剧集赠送币",0)-coalesce("累计特定剧集时长赠送币",0)
          -coalesce("累计社媒活动赠币",0) as "累计其他任务赠币"
from cal02
left join cal01
on cal02.data_date = cal01."日期"
and cal02.area = cal01."区域"
and cal02.country_name = cal01."国家"
and cal02.user_type = cal01."用户类型";