--------------------------------------------------------
-- 拆解经营报表中续订人数相关的部分
--------------------------------------------------------
set timezone ='UTC-0';
with new_reg_users as (
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
)
,tmp_subscription as   (
    select t1.*
        ,row_number() over(partition by t1.uid,t1.vip_days,t1.order_type2 order by t1.begin_date ) as rn -- 周卡第n次订阅/第n次续订
        ,dense_rank() over(partition by t1.uid,t1.vip_days order by t1.order_id  ) as odrn               -- 第n次周卡
        ,coalesce(nn.country_code,'UNKNOWN') as country_code
        ,coalesce(nn.lang) as lang
        ,coalesce(nn.lang_name) as lang_name
        ,coalesce(nn.os) as os
	from(
        select t1.cs_order_id,t1.order_num,t1.vip_days,t1.status,t1.pay_type ,t1.money,t1.created_at,platform_order_id
              ,t2.*
              ,case when row_number() over(partition by t2.uid,t2.order_id order by t2.out_order_id)=1 then 4 else 5 end as order_type2
              -- ,case when  t2.out_origin_order_id<>t2.out_order_id then 5 else 4 end as order_type
              -- ,case when t1.vip_days is null then (case when product_id like '%week%' then 7 when product_id like '%month%' then 30 when product_id like '%quarter%' then 90 else 365 end) else t1.vip_days end as vip_days2
        from(
              select distinct t1.app_id,t1.cs_order_id,t1.order_num,t1.vip_days ,t1.status,t1.pay_type ,t1.money,to_timestamp(created_at) as created_at,platform_order_id
              from public."oversea-api_osd_order" t1
              where environment=1 and order_type =4 -- and status in(1,3)   -- environment= 生产 order_type= 订阅会员
              and to_timestamp(created_at)::date>='2024-07-01'
                -- and to_timestamp(created_at)::date >= (current_date+interval '-1 day')::date -- 增 不可用
	    )t1
	    right join(
              select distinct product_id, uid ,order_id ,out_origin_order_id ,out_order_id  ,status as sub_status,payment_state -- 1正常2到期
                  ,to_timestamp(begin_time)::date::text as begin_date ,to_timestamp(end_time)::date::text as end_date
                  ,to_timestamp(end_time)::date-to_timestamp(begin_time)::date as diff_date
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
   -- 订单流水表
, tmp_all_order_log as (
    select
       id,
       order_num,
       platform_order_id,
       app_id,
       uid,
       order_date,
       created_date,
       created_at
    from all_order_log
    where order_type = 5 and environment =1 and subscribe_type >= 1
      --and app_id<>'osd13469466'
)

    --select * from tmp_subscription where  uid = 14762574

-- 计算差集 差距很大
--     select distinct uid from  tmp_all_order_log where to_timestamp(created_at)::date='2025-03-03'
--     except
--     select distinct uid from tmp_subscription where begin_date = '2025-03-03' and order_type2=5;

--      对齐数据
--   select begin_date,count(distinct  uid ) from tmp_subscription where order_type2=5 group by begin_date
--   select to_timestamp(created_at)::date,count(distinct uid) from tmp_all_order_log group by to_timestamp(created_at)::date

--     select a.d_date,b.begin_date,order_log as 流水表,jingying as 经营报表,order_log-jingying as 差值
--     from (select to_timestamp(created_at)::date as d_date,count(distinct uid) as order_log from tmp_all_order_log group by to_timestamp(created_at)::date) a
--     join (select begin_date::date,count(distinct  uid ) as jingying from tmp_subscription where order_type2=5 group by begin_date) b
--     on a.d_date=b.begin_date

--  找到3/3 日当天 不同的uid差异
     select * from tmp_all_order_log where uid = 17980945




--  select count(*) from tmp_subscription     --243965   (diff_date去掉是258408) 官网就一条数据
--  select count(*) from tmp_all_order_log                        --250258  带status = 1 249876


-- select count(*) from tmp_all_order_log t1
--     join tmp_subscription t2
--         on t1.order_num=t2.order_num and to_timestamp(t1.created_at)::date = '2025-03-03'





