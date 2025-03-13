set timezone ='UTC-0';
-- delete from tmp.dw_operate_view_tmp01 where d_date>= (current_date+interval '-31 day')::date::text;
-- insert into tmp.dw_operate_view_tmp01
truncate table tmp.dw_operate_view_tmp01;
insert into tmp.dw_operate_view_tmp01
-- 脚本
--
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
,tmp_dau as(
    select  to_char(d_date,'yyyymmdd')::int as created_date ,uid::int8 , d_date
	from public.dwd_user_active
	-- where d_date >= (current_date+interval '-2 day')
)

,tmp_subscription as(
  select t1.*
  ,row_number() over(partition by t1.uid,t1.vip_days,t1.order_type2 order by t1.begin_date ) as rn -- 周卡第n次订阅/第n次续订
  ,dense_rank() over(partition by t1.uid,t1.vip_days order by t1.order_id  ) as odrn -- 第n次周卡
  ,coalesce(nn.country_code,'UNKNOWN') as country_code
  ,coalesce(nn.lang) as lang ,coalesce(nn.lang_name) as lang_name
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
      where environment=1 and order_type =4 -- and status in(1,3)
      and to_timestamp(created_at)::date>='2024-07-01'
      	-- and to_timestamp(created_at)::date >= (current_date+interval '-1 day')::date -- 增 不可用
    )t1
    right join(
      select distinct product_id, uid ,order_id ,out_origin_order_id ,out_order_id  ,status as sub_status,payment_state -- 1正常2到期
      ,to_timestamp(begin_time)::date::text as begin_date ,to_timestamp(end_time)::date::text as end_date
      ,to_timestamp(end_time)::date-to_timestamp(begin_time)::date as diff_date
      from public.middle_subscription m
      where environment=1 and status>0
      and to_timestamp(created_at)::date>='2024-07-01'
      	-- and to_timestamp(created_at)::date >= (current_date+interval '-1 day')::date -- 增 不可用
      -- order by order_id ,out_order_id
    )t2 on t1.cs_order_id=t2.order_id
    where t1.cs_order_id is not null -- 过滤问题数据
    and t2.diff_date>=7 -- 过滤问题数据
    and t1.app_id<>'osd13469466' -- 过滤官网数据
    -- order by t2.uid,t2.order_id,t2.out_order_id
  )t1
  left join new_reg_users nn on t1.uid=nn.uid
)

,tmp_primary as(
	select d.*
	,g.lang
	,g.lang_name
        ,g.os
	from(
    	select d.d_date  ,d.v_date::int as v_date
    	,v.country_code ,v.country_name ,v.area
    	from(
    		select d_date,v_date from analysis.dim_day
    		where d_date between '2024-07-01' and current_date::text
    			-- and d_date >= (current_date+interval '-2 day')::date::text -- 增
    	)d
    	left join v_dim_country_area v on 1=1
	)d
	left join(select distinct lang,lang_name,os from new_reg_users nru
	union all select distinct lang,lang_name,'UNKNOWN' as os from new_reg_users)g on 1=1
)
,
user_pay_status AS (
    SELECT DISTINCT
        uid
    	,to_timestamp(created_at)::date as d_date
    FROM all_order_log
    WHERE to_timestamp(created_at)::date >= '2024-07-01'
    --   AND order_type IN (1, 4, 5)
      AND environment = 1
      AND status = 1
      )
        -- ,tmp_result as( -- 报表
        select
         t0.d_date
    	,t0.country_code
    	,t0.country_name
    	,t0.area
        ,coalesce(t1.dau,0) as dau
    	,coalesce(t1.dau_2login,0) as dau_2login
    	,coalesce(t1.old_dau,0) as old_dau
    	,coalesce(t1.old_dau_2login,0) as old_dau_2login
        ,coalesce(t1.new_dau,0) as new_dau
    	,coalesce(t1.new_dau_2login,0) as new_dau_2login
    	,coalesce(t1.new_dau_campaign,0) as new_dau_campaign
    	,coalesce(t1.new_dau_natural,0) as new_dau_natural
    	,coalesce(t3.all_pay_order,0) as all_pay_order
    	,coalesce(t3.pay_amt,0)*0.01 as pay_amt
    	,coalesce(t3.pay_order,0) as pay_order
    	,coalesce(t3.pay_user,0) as pay_user
    	,coalesce(t3.pay_user_campaign,0) as pay_user_campaign
    	,coalesce(t3.pay_user_natural,0) as pay_user_natural
    	,coalesce(t3.pay_k_amt,0)*0.01 as pay_k_amt
    	,coalesce(t3.pay_week_amt,0)*0.01 as pay_week_amt
    	,coalesce(t3.pay_month_amt,0)*0.01 as pay_month_amt
    	,coalesce(t3.pay_quarter_amt,0)*0.01 as pay_quarter_amt
    	,coalesce(t3.pay_year_amt,0)*0.01 as pay_year_amt
    	,coalesce(t3.new_pay_amt,0)*0.01 as new_pay_amt
    	,coalesce(t3.new_pay_amt_campaign,0)*0.01 as new_pay_amt_campaign
    	,coalesce(t3.new_pay_amt_natural,0)*0.01 as new_pay_amt_natural
    	,coalesce(t3.new_pay_order,0) as new_pay_order
    	,coalesce(t3.new_pay_order_campaign,0) as new_pay_order_campaign
    	,coalesce(t3.new_pay_order_natural,0) as new_pay_order_natural
    	,coalesce(t3.new_pay_user,0) as new_pay_user
    	,coalesce(t3.new_pay_user_campaign,0) as new_pay_user_campaign
    	,coalesce(t3.new_pay_user_natural,0) as new_pay_user_natural
    	,coalesce(t3.old_pay_amt,0)*0.01 as old_pay_amt
    	,coalesce(t3.old_pay_order,0) as old_pay_order
    	,coalesce(t3.old_pay_user,0) as old_pay_user
    	,coalesce(t3.old_pay_user_campaign,0) as old_pay_user_campaign
    	,coalesce(t3.old_pay_user_natural,0) as old_pay_user_natural
    	,coalesce(t4.pay_refund_amt,0)*0.01 as pay_refund_amt
    	,coalesce(t6.ad_cost_tt,0)*0.0001 as ad_cost_tt
    	,coalesce(t6.ad_cost_fb,0)*0.0001 as ad_cost_fb
    	,coalesce(t6.ad_cost_asa,0)*0.0001 as ad_cost_asa
    	,coalesce(t7.ad_income_amt,0) as ad_income_amt
    	,coalesce(t1.dau_3login,0) as dau_3login
    	,coalesce(t1.dau_7login,0) as dau_7login
    	,coalesce(t1.dau_14login,0) as dau_14login
    	,coalesce(t1.dau_30login,0) as dau_30login
    	,coalesce(t6.ad_cost,0)*0.0001 as ad_cost
    	--
    	,coalesce(t31.repay_user,0) as repay_user
    	,coalesce(t31.repay_week_user,0) as repay_week_user
    	,coalesce(t31.repay_month_user,0) as repay_month_user
    	,coalesce(t31.repay_quarter_user,0) as repay_quarter_user
    	,coalesce(t31.repay_year_user,0) as repay_year_user
    	,coalesce(t31.firstsb_week_user,0) as firstsb_week_user
    	,coalesce(t31.firstsb_1rp_week_user,0) as firstsb_1rp_week_user
    	,coalesce(t31.firstsb_2rp_week_user,0) as firstsb_2rp_week_user
    	,coalesce(t31.againsb_week_user,0) as againsb_week_user
    	,coalesce(t31.againsb_1rp_week_user,0) as againsb_1rp_week_user
    	,coalesce(t31.againsb_2rp_week_user,0) as againsb_2rp_week_user
    	,coalesce(t32.due_user,0) as due_user
    	,coalesce(t32.due_week_user,0) as due_week_user
    	,coalesce(t32.due_month_user,0) as due_month_user
    	,coalesce(t32.due_quarter_user,0) as due_quarter_user
    	,coalesce(t32.due_year_user,0) as due_year_user
    	,coalesce(t32.due_firstsb_week_user,0) as due_firstsb_week_user
    	,coalesce(t32.due_firstsb_1rp_week_user,0) as due_firstsb_1rp_week_user
    	,coalesce(t32.due_firstsb_2rp_week_user,0) as due_firstsb_2rp_week_user
    	,coalesce(t32.due_againsb_week_user,0) as due_againsb_week_user
    	,coalesce(t32.due_againsb_1rp_week_user,0) as due_againsb_1rp_week_user
    	,coalesce(t32.due_againsb_2rp_week_user,0) as due_againsb_2rp_week_user
    	,t0.lang
    	,t0.lang_name
    	,coalesce(t6.ad_cost_other,0)*0.0001 as ad_cost_other
    	---
    	,coalesce(t1.pay_user_dau,0) as pay_user_dau
    	,coalesce(t1.pay_user_2login,0) as pay_user_2login
    	,coalesce(t1.new_pay_user_dau,0) as new_pay_user_dau
        ,coalesce(t1.new_pay_user_2login,0) as new_pay_user_2login
        ,coalesce(t1.old_pay_user_dau,0) as old_pay_user_dau
        ,coalesce(t1.old_pay_user_2login,0) as old_pay_user_2login
    	,coalesce(t3.all_pay_user,0) as all_pay_user
    	----
    	,t0.os
        from tmp_primary t0
        left join(
            select
             u1.created_date
            ,coalesce(u0.country_code,'UNKNOWN') as country_code -- ,u0.ad_channel
            ,coalesce(u0.lang,'UNKNOWN') as lang
    	,coalesce(u0.os,'UNKNOWN') as os
            ,count(distinct u1.uid) as dau
            ,count(distinct u2.uid) as dau_2login
            ,count(distinct u3.uid ) as dau_3login -- 用户第3留
            ,count(distinct u7.uid ) as dau_7login -- 用户第7留
            ,count(distinct u14.uid ) as dau_14login -- 用户第14留
            ,count(distinct u30.uid ) as dau_30login -- 用户第30留
            ,count(distinct case when (un.uid is null) then u1.uid else null end ) as old_dau -- 登录老用户数
            ,count(distinct case when (un.uid is null) then u2.uid else null end ) as old_dau_2login -- 老用户第二日登录
    		,count(distinct un.uid) as new_dau -- 新用户数
    		,count(distinct n2.uid) as new_dau_2login -- 新用户第二日登录
    		,count(distinct case when un.is_campaign=1 then un.uid else null end) as new_dau_campaign -- 新用户【推广量】
    		,count(distinct case when un.is_campaign=0 then un.uid else null end) as new_dau_natural -- 新用户【自然量】
    		-----
    		,count(distinct case when ps.uid is not null then u1.uid else null end) as pay_user_dau
    		,count(distinct case when ps.uid is not null then u2.uid else null end) as pay_user_2login
    		,count(distinct case when ps.uid is not null and un.uid is not null then un.uid else null end) as new_pay_user_dau
            ,count(distinct case when ps.uid is not null and un.uid is not null then n2.uid else null end) as new_pay_user_2login
            ,count(distinct case when ps.uid is not null and un.uid is null then u1.uid else null end) as old_pay_user_dau
            ,count(distinct case when ps.uid is not null and un.uid is null then u2.uid else null end) as old_pay_user_2login
            from tmp_dau u1
            left join new_reg_users un on u1.uid=un.uid and u1.created_date=un.created_date
    		left join tmp_dau as n2 on un.uid = n2.uid  and un.d_date=(n2.d_date +interval '-1 day')::date -- and un.country_code=u2.country_code
            left join tmp_dau u2 on u1.uid=u2.uid  and u1.d_date=(u2.d_date +interval '-1 day')::date  -- and u1.country_code=u2.country_code
            left join tmp_dau u3 on u1.uid=u3.uid  and u1.d_date=(u3.d_date +interval '-3 day')::date   -- and u1.country_code=u3.country_code
            left join tmp_dau u7 on u1.uid=u7.uid  and u1.d_date=(u7.d_date +interval '-7 day')::date    -- and u1.country_code=u7.country_code
            left join tmp_dau u14 on u1.uid=u14.uid  and u1.d_date=(u14.d_date +interval '-14 day')::date   -- and u1.country_code=u14.country_code
            left join tmp_dau u30 on u1.uid=u30.uid  and u1.d_date=(u30.d_date +interval '-30 day')::date 	-- and u1.country_code=u30.country_code
            left join new_reg_users u0 on u1.uid=u0.uid
    		left join user_pay_status ps on u1.uid = ps.uid and ps.d_date <= u1.d_date
            where 1=1
            group by
             u1.created_date
            ,coalesce(u0.country_code,'UNKNOWN') -- ,u0.ad_channel
            ,coalesce(u0.lang,'UNKNOWN')
    	,coalesce(u0.os,'UNKNOWN')

        )t1 on t0.v_date=t1.created_date and t0.country_code=t1.country_code and t0.lang=t1.lang and t0.os=t1.os
    	left join(
            select
             o.created_date
            ,coalesce(u0.country_code,'UNKNOWN') as country_code -- ,u0.ad_channel
            ,coalesce(u0.lang,'UNKNOWN') as lang
    	,coalesce(u0.os,'UNKNOWN') as os
    		,count(distinct o.order_num) as all_pay_order  -- 总订单数(包含失败)
    		,count(distinct  o.uid ) as  all_pay_user  -- 总生成订单人数(包含失败)
    		,sum(case when o.status = 1 then o.money else 0 end) as  pay_amt  -- 成功充值金额
    		,count(distinct case when o.status = 1 then o.order_num else null end) as  pay_order  -- 成功充值订单数
    		,count(distinct case when o.status = 1 then o.uid else null end) as  pay_user  -- 成功充值人数
    		,count(distinct case when (o.status = 1 and length(coalesce(o.campaign_id,'')) >1) then o.uid else null end) as  pay_user_campaign  -- 成功充值人数【推广量】
    		,count(distinct case when (o.status = 1 and length(coalesce(o.campaign_id,''))<=1) then o.uid else null end) as  pay_user_natural  -- 成功充值人数【自然量】
    	    ,sum(case when (o.status = 1 and o.order_type = 1) then o.money else 0 end) as pay_k_amt  -- 充值K币金额
    	    ,sum(case when (o.status = 1 and o.order_type in (4,5) and vip_days =   7) then o.money else 0 end) as pay_week_amt --- 充值周卡金额
    	    ,sum(case when (o.status = 1 and o.order_type in (4,5) and vip_days =  30) then o.money else 0 end) as pay_month_amt --- 充值月卡金额
    	    ,sum(case when (o.status = 1 and o.order_type in (4,5) and vip_days =  90) then o.money else 0 end) as pay_quarter_amt  --- 充值季卡金额
    	    ,sum(case when (o.status = 1 and o.order_type in (4,5) and vip_days = 365) then o.money else 0 end) as pay_year_amt  -- 充值年卡金额
            ,sum(case when (o.status = 1 and u.uid is not null) then o.money else 0 end) as new_pay_amt -- 新用户充值金额
            ,sum(case when (o.status = 1 and u.uid is not null and length(coalesce(o.campaign_id,'')) >1) then o.money else 0 end) as new_pay_amt_campaign-- 新用户充值金额【推广量】
            ,sum(case when (o.status = 1 and u.uid is not null and length(coalesce(o.campaign_id,''))<=1) then o.money else 0 end) as new_pay_amt_natural -- 新用户充值金额【自然量】
            ,count(case when (o.status = 1 and u.uid is not null) then o.id else null end) as new_pay_order -- 新用户充值订单数
            ,count(case when (o.status = 1 and u.uid is not null and length(coalesce(o.campaign_id,'')) >1) then o.id else null end) as new_pay_order_campaign-- 新用户充值订单数【推广量】
            ,count(case when (o.status = 1 and u.uid is not null and length(coalesce(o.campaign_id,''))<=1) then o.id else null end) as new_pay_order_natural -- 新用户充值订单数【自然量】
            ,count(distinct case when (o.status = 1 and u.uid is not null) then o.uid else null end) as new_pay_user --  新用户充值人数
            ,count(distinct case when (o.status = 1 and u.uid is not null and length(coalesce(o.campaign_id,'')) >1) then o.uid else null end) as new_pay_user_campaign -- 新用户充值人数【推广流】
            ,count(distinct case when (o.status = 1 and u.uid is not null and length(coalesce(o.campaign_id,''))<=1) then o.uid else null end) as new_pay_user_natural -- 新用户充值人数【自然量】
            ,sum(case when (o.status = 1 and u.uid is  null) then o.money else 0 end) as old_pay_amt -- 老用户充值金额
            ,count(case when (o.status = 1 and u.uid is  null) then o.id else null end) as old_pay_order -- 老用户总订单数
            ,count(distinct case when (o.status = 1 and u.uid is  null) then o.uid else null end) as old_pay_user -- 老用户充值人数
            ,count(distinct case when (o.status = 1 and u.uid is  null and length(coalesce(o.campaign_id,'')) >1) then o.uid else null end) as old_pay_user_campaign -- 老用户充值人数【推广量】
            ,count(distinct case when (o.status = 1 and u.uid is  null and length(coalesce(o.campaign_id,''))<=1) then o.uid else null end) as old_pay_user_natural -- 老用户充值人数【自然量】
            from public.all_order_log o
            left join new_reg_users u on o.uid = u.uid and o.created_date=u.created_date
            left join new_reg_users u0 on o.uid=u0.uid
            where 1=1 and o.environment = 1 and o.os in('android','ios')
            and o.created_date>=20240701
            -- and o.created_date>=20250101
            -- 	and to_char( to_timestamp(o.created_at),'YYYY-MM-DD') >= (current_date+interval '-2 day')::date::text -- 增
            group by o.created_date
            ,coalesce(u0.country_code,'UNKNOWN') -- ,u0.ad_channel
            ,coalesce(u0.lang,'UNKNOWN')
    	,coalesce(u0.os,'UNKNOWN')

        )t3 on t0.v_date=t3.created_date and t0.country_code=t3.country_code and t0.lang=t3.lang and t0.os=t3.os
    	left join(
    		select t1.begin_date
    		,t1.country_code
    		,t1.lang
    		,t1.os
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
    		,t1.country_code
    		,t1.lang
    		,t1.os

    	)t31 on t0.d_date=t31.begin_date and t0.country_code=t31.country_code and t0.lang=t31.lang and t0.os=t31.os
    	left join(
    		select t1.end_date
    		,t1.country_code
    		,t1.lang
    		,t1.os
    		,count(distinct t1.uid) as due_user -- 到期人数
    		,count(distinct case when t1.vip_days=  7 then t1.uid else null end) as due_week_user -- 周卡到期人数
    		,count(distinct case when t1.vip_days= 30 then t1.uid else null end) as due_month_user -- 月卡到期人数
    		,count(distinct case when t1.vip_days= 90 then t1.uid else null end) as due_quarter_user -- 季卡到期人数
    		,count(distinct case when t1.vip_days=365 then t1.uid else null end) as due_year_user -- 年卡到期人数
            ,count(distinct case when t1.vip_days=  7 and t1.order_type2=4 and t1.odrn=1 and t1.rn=1 then t1.uid else null end) as due_firstsb_week_user -- 首次订阅周卡
            ,count(distinct case when t1.vip_days=  7 and t1.order_type2=5 and t1.odrn=1 and t1.rn=1 then t1.uid else null end) as due_firstsb_1rp_week_user -- 首次订阅周卡首次续订
            ,count(distinct case when t1.vip_days=  7 and t1.order_type2=5 and t1.odrn=1 and t1.rn>1 then t1.uid else null end) as due_firstsb_2rp_week_user -- 首次订阅周卡非首次续订
            ,count(distinct case when t1.vip_days=  7 and t1.order_type2=4 and t1.odrn>1 and t1.rn>1 then t1.uid else null end) as due_againsb_week_user -- 非首次订阅周卡
            ,count(distinct case when t1.vip_days=  7 and t1.order_type2=5 and t1.odrn>1 and t1.rn=1 then t1.uid else null end) as due_againsb_1rp_week_user -- 非首次订阅周卡首次续订
            ,count(distinct case when t1.vip_days=  7 and t1.order_type2=5 and t1.odrn>1 and t1.rn>1 then t1.uid else null end) as due_againsb_2rp_week_user -- 非首次订阅周卡非首次续订
    		from tmp_subscription t1
    		where 1=1
    		and t1.end_date>='2024-07-01'
    			-- and t1.end_date >= (current_date+interval '-2 day')::date::text -- 增
    		group by t1.end_date
    		,t1.country_code
    		,t1.lang
    		,t1.os

    	)t32 on t0.d_date=t32.end_date and t0.country_code=t32.country_code and t0.lang=t32.lang  and t0.os=t32.os
        left join(
    		select r.refund_date as created_date
    		,coalesce(u0.country_code,'UNKNOWN') as country_code -- ,u0.ad_channel
    		,coalesce(u0.lang,'UNKNOWN') as lang
    		,coalesce(u0.os,'UNKNOWN') as os
    		,sum(r.total_money) as pay_refund_amt -- 退款金额
    		from public.all_refund_order_log r
    		left join new_reg_users u0 on r.uid=u0.uid
    		where r.environment = 1  and r.os in('android','ios')
    		and r.status = 1
            and r.refund_date>=20240701
            -- and r.refund_date>=20250101
            -- 	and to_char( to_timestamp(r.refund_time),'YYYY-MM-DD') >= (current_date+interval '-2 day')::date::text -- 增
    		group by r.refund_date
    		,coalesce(u0.country_code,'UNKNOWN') -- ,u0.ad_channel
    		,coalesce(u0.lang,'UNKNOWN')
    		,coalesce(u0.os,'UNKNOWN')

        )t4  on t0.v_date=t4.created_date and t0.country_code=t4.country_code and t0.lang=t4.lang and t0.os=t4.os
        left join(
    		select cd.created_date
    		,coalesce(cc.country_code,'UNKNOWN') as country_code
    		,coalesce(cn.lang,'UNKNOWN') as lang
    		,'UNKNOWN' as os
            ,sum(case when ad_channel = 'tt' then cost_amount else 0 end) as ad_cost_tt    -- 【tt渠道消耗】
            ,sum(case when ad_channel = 'fb' then cost_amount else 0 end) as ad_cost_fb    -- 【fb渠道消耗】
            ,sum(case when ad_channel = 'apple' then cost_amount else 0 end) as ad_cost_asa    -- 【asa渠道消耗】
            ,sum(case when ad_channel not in('tt','fb','apple') then cost_amount else 0 end) as ad_cost_other    -- 【小渠道消耗】
            ,sum(cost_amount) as ad_cost -- 总渠道消耗
    		from public.ad_cost_data_log cd -- 消耗明细表
    		left join v_dim_ad_campaign_info cn on cd.campaign_id=cn.campaign_id
    		left join v_dim_country_area cc on upper(cd.area)=cc.country_code
    		where 1=1
    		and cd.account_id not in('3851320725139192','1248567319618926')
    		and cd.created_date>=20240701
    		-- and cd.created_date>=20250101
    		-- 	and cd.created_date >= to_char((current_date+interval '-2 day'),'yyyymmdd')::int  -- 增
    		group by cd.created_date
    		,coalesce(cc.country_code,'UNKNOWN')
    		,coalesce(cn.lang,'UNKNOWN')

        )t6  on t0.v_date=t6.created_date and t0.country_code=t6.country_code and t0.lang=t6.lang and t0.os=t6.os
        left join(
    		select a.created_date
    		,coalesce(cc.country_code,'UNKNOWN')  as country_code
    		,'UNKNOWN' as lang
    		,CASE
                            WHEN a.os = 'ios' THEN 'ios'
                            WHEN a.os = 'android' THEN 'android'
                            ELSE 'UNKNOWN'
                            END AS os
    		,sum(adin_amt) as ad_income_amt -- 商业化广告收入
    		from public.dwd_adin_media_revenue a
    		left join v_dim_country_area cc on upper(a.country_code)=cc.country_code
    		where 1=1
    		and a.created_date::int >=20240701
    		-- and a.created_date::int >=20250101
    		-- 	and a.created_date::int >= to_char((current_date+interval '-2 day'),'yyyymmdd')::int  -- 增
    		group by a.created_date
    		,coalesce(cc.country_code,'UNKNOWN')
    		,CASE
                            WHEN a.os = 'ios' THEN 'ios'
                            WHEN a.os = 'android' THEN 'android'
                            ELSE 'UNKNOWN'
                            END

        )t7  on t0.v_date=t7.created_date::int and t0.country_code=t7.country_code and t0.lang=t7.lang and t0.os=t7.os
        ;

-- delete from public.dw_operate_view where d_date>= (current_date+interval '-31 day')::date::text;
-- insert into public.dw_operate_view select * from tmp.dw_operate_view_tmp01 where d_date>= (current_date+interval '-31 day')::date::text;
truncate table public.dw_operate_view;
insert into public.dw_operate_view select * from tmp.dw_operate_view_tmp01;