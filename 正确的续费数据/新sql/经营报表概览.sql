-------------------------------------------------
-- 经营报表概览
-- key：
-------------------------------------------------
set timezone ='UTC-0';
	    with new_reg_users as (
			select v_date as created_date
			,d_date::date as d_date
			,uid::int8 as uid
			,country_code
			,lang
			,lang_name
			,is_campaign
            ,os
			from public.dwd_user_info
	    )
	    --
	    ,tmp_subscription as(
		    select t1.*
                ,row_number() over(partition by t1.uid,t1.vip_days,t1.order_type2 order by t1.begin_date ) as rn -- 周卡第n次订阅/第n次续订
                ,dense_rank() over(partition by t1.uid,t1.vip_days order by t1.order_id  ) as odrn               -- 第n次周卡
                ,coalesce(nn.country_code,'UNKNOWN') as country_code
                ,coalesce(nn.lang) as lang
                ,coalesce(nn.lang_name) as lang_name
                ,coalesce(nn.os) as os
		    from(
                select t1.cs_order_id,t1.order_num,t1.vip_days,t1.status,t1.pay_type ,t1.money
                      ,t2.*
                      ,case when row_number() over(partition by t2.uid,t2.order_id order by t2.out_order_id)=1 then 4 else 5 end as order_type2
                      -- ,case when  t2.out_origin_order_id<>t2.out_order_id then 5 else 4 end as order_type
                      -- ,case when t1.vip_days is null then (case when product_id like '%week%' then 7 when product_id like '%month%' then 30 when product_id like '%quarter%' then 90 else 365 end) else t1.vip_days end as vip_days2
                from(
                      select distinct t1.app_id,t1.cs_order_id,t1.order_num,t1.vip_days ,t1.status,t1.pay_type ,t1.money
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

	    select t1.begin_date
			,count(distinct case when t1.order_type2=5 then t1.uid else null end) as repay_user -- 续订人数
			,count(distinct case when t1.order_type2=5 and t1.vip_days=  7 then t1.uid else null end) as repay_week_user -- 周卡续订人数
			,count(distinct case when t1.order_type2=5 and t1.vip_days= 30 then t1.uid else null end) as repay_month_user -- 月卡续订人数
			,count(distinct case when t1.order_type2=5 and t1.vip_days= 90 then t1.uid else null end) as repay_quarter_user -- 季卡续订人数
			,count(distinct case when t1.order_type2=5 and t1.vip_days=365 then t1.uid else null end) as repay_year_user -- 年卡续订人数
	        ,count(distinct case when t1.vip_days=  7 and t1.order_type2=4 and t1.odrn=1 and t1.rn=1 then t1.uid else null end) as firstsb_week_user -- 首次订阅周卡
	        ,count(distinct case when t1.vip_days=  7 and t1.order_type2=5 and t1.odrn=1 and t1.rn=1 then t1.uid else null end) as firstsb_1rp_week_user -- 首次订阅周卡首次续订
	        ,count(distinct case when t1.vip_days=  7 and t1.order_type2=5 and t1.odrn=1 and t1.rn>1 then t1.uid else null end) as firstsb_2rp_week_user -- 首次订阅周卡非首次续订
	        ,count(distinct case when t1.vip_days=  7 and t1.order_type2=4 and t1.odrn>1 and t1.rn>1 then t1.uid else null end) as againsb_week_user -- 非首次订阅周卡
	        ,count(distinct case when t1.vip_days=  7 and t1.order_type2=5 and t1.odrn>1 and t1.rn=1 then t1.uid else null end) as againsb_1rp_week_user -- 非首次订阅周卡首次续订
	        ,count(distinct case when t1.vip_days=  7 and t1.order_type2=5 and t1.odrn>1 and t1.rn>1 then t1.uid else null end) as againsb_2rp_week_user -- 非首次订阅周卡非首次续订
			from tmp_subscription t1
			where 1=1
			and t1.begin_date>='2024-07-01'
				-- and t1.begin_date >= (current_date+interval '-2 day')::date::text -- 增
			group by t1.begin_date