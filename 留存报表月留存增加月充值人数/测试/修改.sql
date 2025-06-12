---------------------------------------------
-- File: 修改.sql
-- Time: 2025/6/12 11:44
-- User: xiaoj
-- Description:  
---------------------------------------------
SET timezone ='UTC';
---------------------------------------------
-- 建表
---------------------------------------------

-- drop table if exists public.dw_retention_month;
CREATE TABLE if not exists public.dw_retention_month (
    id text NOT NULL,
    active_month text,
    "区域" text,
    "国家" text,
    "系统" text,
    mau bigint,
    "总次月留存" bigint,
    "总2月留存" bigint,
    "总3月留存" bigint,
    "新用户数" bigint,
    "新用户次月留存" bigint,
    "新用户2月留存" bigint,
    "新用户3月留存" bigint,
    "新推广用户数" bigint,
    "新推广用户次月留存" bigint,
    "新推广用户2月留存" bigint,
    "新推广用户3月留存" bigint,
    "新自然用户数" bigint,
    "新自然用户次月留存" bigint,
    "新自然用户2月留存" bigint,
    "新自然用户3月留存" bigint,
    "老用户数" bigint,
    "老用户次月留存" bigint,
    "老用户2月留存" bigint,
    "老用户3月留存" bigint,
    "月充值人数" bigint,
    lang_name text,
    is_paid integer
    ,PRIMARY KEY (id)
);


---------------------------------------------
-- 更新
---------------------------------------------






-- 月留存

