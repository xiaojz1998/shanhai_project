-----------------------------------------------------
-- 数据对比，利用会员过期时间
-----------------------------------------------------
set timezone ='UTC-0';
with tmp_all_order_log as (
    select
       order_num,
       platform_order_id,
       app_id,
       os,
       uid,
       order_type,
       order_date,
       created_date,
       created_at,
       vip_days,
       to_timestamp(vip_expires_time) as end_time,
       case when vip_days = 7 then (to_timestamp(vip_expires_time)::date - interval '7 day')::date
           when vip_days = 30 then (to_timestamp(vip_expires_time)::date - interval '1 month')::date
           when vip_days = 90 then (to_timestamp(vip_expires_time)::date - interval '3 month')::date
           when vip_days = 365 then (to_timestamp(vip_expires_time)::date - interval '1 year')::date
           else null end as begin_date,
       to_timestamp(vip_expires_time)::date as end_date,
       row_number() over (partition by order_num order by created_at) as rn
    from all_order_log
    where environment =1 and status =1
      --and app_id<>'osd13469466'
      -- subscribe_type 不能用因为这是后加入的字段
),
    new_reg_users as (
	-- 每日新增人数
	select
	    v_date as created_date
        ,d_date::date as d_date
        ,uid::int8 as uid
        ,country_code
        ,lang
        ,lang_name
        ,is_campaign
        ,os
    from public.dwd_user_info
),
    tmp_subscription as   (
    select t1.*
        ,row_number() over(partition by t1.uid,t1.vip_days,t1.order_type2 order by t1.begin_date ) as rn -- 周卡第n次订阅/第n次续订
        ,dense_rank() over(partition by t1.uid,t1.vip_days order by t1.order_id  ) as odrn               -- 第n次周卡
        ,coalesce(nn.country_code,'UNKNOWN') as country_code
        ,coalesce(nn.lang) as lang
        ,coalesce(nn.lang_name) as lang_name
        ,coalesce(nn.os) as os
	from(
        select t1.cs_order_id,t1.order_num,t1.vip_days,t1.status,t1.pay_type ,t1.money,platform_order_id
              ,t2.*
              ,case when row_number() over(partition by t2.uid,t2.order_id order by t2.out_order_id)=1 then 4 else 5 end as order_type2
              -- ,case when  t2.out_origin_order_id<>t2.out_order_id then 5 else 4 end as order_type
              -- ,case when t1.vip_days is null then (case when product_id like '%week%' then 7 when product_id like '%month%' then 30 when product_id like '%quarter%' then 90 else 365 end) else t1.vip_days end as vip_days2
        from(
              select distinct t1.app_id,t1.cs_order_id,t1.order_num,t1.vip_days ,t1.status,t1.pay_type ,t1.money,platform_order_id
              from public."oversea-api_osd_order" t1
              where environment=1 and order_type =4 -- and status in(1,3)   -- environment= 生产 order_type= 订阅会员
              and to_timestamp(created_at)::date>='2024-07-01'
                -- and to_timestamp(created_at)::date >= (current_date+interval '-1 day')::date -- 增 不可用
	    )t1
	    right join(
              select distinct product_id, uid ,order_id ,out_origin_order_id ,out_order_id  ,status as sub_status,payment_state -- 1正常2到期
                  ,to_timestamp(begin_time)::date::text as begin_date ,to_timestamp(end_time)::date::text as end_date
                  ,to_timestamp(end_time)::date-to_timestamp(begin_time)::date as diff_date
                  ,to_timestamp(created_at) as created_at
                  from public.middle_subscription m            -- 订阅表
                  where environment=1 and status>0             -- evironment = 1 生产 status>0 1:正常,2:到期,3:暂停,4:退款
                  and to_timestamp(created_at)::date>='2024-07-01'
                    -- and to_timestamp(created_at)::date >= (current_date+interval '-1 day')::date -- 增 不可用
                  -- order by order_id ,out_order_id
	    )t2 on t1.cs_order_id=t2.order_id
            where t1.cs_order_id is not null -- 过滤问题数据
            and t2.diff_date>=7 -- 过滤问题数据
            and t1.app_id<>'osd13469466' -- 过滤官网数据
            -- order by t2.uid,t2.order_id,t2.out_order_id
	)t1 left join new_reg_users nn on t1.uid=nn.uid           -- 链接新增表
)
-- 时间
-- select min(created_date) from tmp_all_order_log where order_type = 5; -- 最早的续订20240702

-- 两者数据量
-- 可以确定除了ios数据，order_type = 5 代表续订，手动则是第一次订阅 且order_type=4
-- select count(*) from tmp_all_order_log  --2174540
-- select count(*) from tmp_all_order_log where rn = 1  --1897263
-- select count(*) from tmp_all_order_log where rn != 1 and  -- 277296
-- select count(*) from tmp_all_order_log where rn != 1 and order_type=5 --277137
--select * from tmp_all_order_log where rn = 1 and order_type=5 and to_timestamp(created_at) >= '2025-01-01'-- 不带日期限制15191 -- 1.1号手 200多条，ios端问题已经被修复


-- select count(*) from tmp_all_order_log where order_type=5    --295629
-- select count(*) from tmp_subscription where order_type2=5 --290993

-- 一月一号以后用platform_order_id 关联
--   select count(*) from tmp_all_order_log where order_type=5 and to_timestamp(created_at) >= '2025-01-01'  --189205
--   select count(*) from tmp_subscription where order_type2=5 and created_at::date >= '2025-01-01' -- 192731
-- 观察platform_order_id数据
-- 发现格式完全不一样
--   select platform_order_id from tmp_all_order_log where order_type=5 and to_timestamp(created_at) >= '2025-01-01'  --189205
--   select platform_order_id from tmp_subscription where order_type2=5 and created_at::date >= '2025-01-01' -- 192731
    select  count(*)
    from (select * from tmp_all_order_log where order_type=5 and to_timestamp(created_at) >= '2025-01-01') t1
         join (select * from tmp_subscription where order_type2=5 and created_at::date >= '2025-01-01') t2
         on t1.platform_order_id = t2.platform_order_id

-- 查找订单问题
-- select * from all_order_log where order_num = 'SH121068918896906240'
-- select *,to_timestamp(begin_time) as begin_time,to_timestamp(end_time) as end_time from middle_subscription where order_id = 143136482136621056
-- select * from tmp_subscription where order_num = 'SH129867451281645568';
-- select * from tmp_all_order_log where order_num = 'SH144086312145059840'


--     流水经营报表对比
--     select a.d_date,b.begin_date,order_log as 流水表,jingying as 经营报表,order_log-jingying as 差值
--     from (select begin_date as d_date,count(distinct uid) as order_log from tmp_all_order_log where order_type=5 group by begin_date) a
--     join (select begin_date::date,count(distinct  uid ) as jingying from tmp_subscription where order_type2=5 group by begin_date) b
--     on a.d_date=b.begin_date

-- 加入begin_date end_date 和 手动自动
-- 目前有问题
-- select
--     *,
--     case when rn =1 then '手动' else '自动' end as type
-- from tmp_all_order_log;


-- 查看两者差值
-- 计算差集 3/3 差了300
--     select distinct uid,order_num from  tmp_all_order_log where begin_date='2025-03-03' and order_type=5
--     except
--     select distinct uid,order_num from tmp_subscription where begin_date = '2025-03-03' and order_type2=5;


-- 通过order_num和开始时间结束时间关联 订单表和经营报表
select


