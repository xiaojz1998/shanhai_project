---------------------------------------------
-- File: 测试.sql
-- Time: 2025/6/4 20:27
-- User: xiaoj
-- Description:  
---------------------------------------------

select count(*)  from public."oversea-api_osd_pushed"

with tmp_person_push_info as(
    select
        concat('person_',t.vid::text) as push_id
        , to_timestamp(t2.created_at)::date::text as push_time -- 0 时区
        , '未知'::text as layered_name
        , concat('{',case when t1.name = '中文简体' then '简体中文' when t1.name = '印尼语' then '印度尼西亚语' else t1.name end ,'}') as lang_name
        , title as push_title
        , content as push_content
        , sum(t2.sent_count) as sent_unt
        , sum(t2.delivered_count) as push_unt
        , sum(t2.click_count) as click_unt
    from public."oversea-api_osd_personalized_push_configs" t
    left join public."oversea-api_osd_videos" t0 on t.vid = t0.id
    left join public."oversea-api_osd_categories" t1 on t0.category_id = t1.id
    left join public."oversea-api_osd_personalize_push_statistic" t2 on t.id = t2.push_id
    group by concat('person_',t.vid::text)
             , to_timestamp(t2.created_at)::date::text
             , '未知'::text
             , concat('{',case when t1.name = '中文简体' then '简体中文' when t1.name = '印尼语' then '印度尼西亚语' else t1.name end ,'}')
             , title
             , content
) select count(*) from tmp_person_push_info


with tmp_user_layered_info as (
    select
        t1.id
        , t1.lang_code
	    , t2."name" as lang_name
	from(
        select
            id
            , "name"                                        -- 分层名
            , lang_config::json ->> 0 as lang_code          -- 语言码
        from public."oversea-api_osd_user_layered_configs"  -- 用户分层表
        where 1=1
            and lang_config <>'[]'
            -- and "name" like '%英语PUSH-全量用户%'
	) t1
	left join public."oversea-api_osd_lang" t2 on t1.lang_code=t2.lang_code
)
, tmp_auto_push_info as (
    select
        push_id
        , push_time::date::text as push_time
        , layered_name
        , lang_name
        , push_title
        , push_content
        , sum(sent_unt) as sent_unt
        , sum(push_unt) as push_unt
        , sum(click_unt) as click_unt
    from(
        select
            push_id
            , push_time
            , string_agg(layered_name,';' order by layered_name) as layered_name        -- 用户分层名称
            , array_agg(distinct lang_name)::text as lang_name                          -- 用户分层语言
            , push_title
            , push_content
            , sent_unt
            , push_unt
            , click_unt
        from (
            select
                t1.*
                , case when t2.lang_name='英语阿拉伯语' then '阿拉伯语' else t2.lang_name end as lang_name
            from (
                select
                    t0.push_id
                    , to_timestamp(t0.push_time) as push_time           -- 0 时区
                    , json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'id' as layered_id
                    , json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'name' as layered_name
                    , title as push_title                        -- 推送标题
                    , "content" as push_content                  -- 推送内容
                    , t0.sent_count as sent_unt                     -- 命中用户数 推送人数
                    , t0.delivered_count as push_unt                -- 触达人数
                    , t0.click_count as click_unt                   -- 点击人数
                from public."oversea-api_osd_auto_pushes" t
                left join public."oversea-api_osd_auto_push_logs" t0 on t.id = t0.task_id
                where t0.push_id is not null and t0.push_id <> '' and  schedule_id != '' and lang != ''   -- 过滤掉老推送数据
            ) t1
            left join tmp_user_layered_info t2 on t1.layered_id = t2.id::text   -- 补充用户分层语言
        ) a
        group by push_id
                , push_time
                , push_title
                , push_content
                , sent_unt
                , push_unt
                , click_unt
    ) b
    group by push_id
            , push_time::date::text
            , layered_name
            , lang_name
            , push_title
            , push_content
) select count(*) from tmp_auto_push_info



select
    t2.id,sent_count,delivered_count,click_count,to_timestamp(t2.created_at)::date
from public."oversea-api_osd_personalized_push_configs" t
left join public."oversea-api_osd_videos" t0 on t.vid = t0.id
left join public."oversea-api_osd_categories" t1 on t0.category_id = t1.id
left join public."oversea-api_osd_personalize_push_statistic" t2 on t.id = t2.push_id
where t.vid = 983 --and to_timestamp(t2.created_at)::date = '2025-06-05'::date

select to_timestamp(1749195966)


select
    to_timestamp(created_at)::date
    , order_num
    , status
    , money
from public.all_order_log o
where  o.environment = 1                                           -- 生产环境
	-- and to_timestamp(created_at)::date = '2025-05-30'
    -- and push_id = 'person_4413'
    and order_num = 'SH184689470206554112'

select
        push_id
        , to_timestamp(created_at)::date as d_date          -- 数据日期
        , count(distinct o.order_num) as all_pay_order      -- 下单总数（包含失败单）
        , count(distinct case when o.status = 1 then o.order_num else null end) as  pay_order   -- 成功下单数
        , count(distinct case when o.status = 1 then concat(o.order_num,o.created_at,o.order_type) else null end) as  pay_cnt -- 成功充值次数
        , count(distinct case when o.status = 1 then o.uid else null end) as  pay_user  -- 成功充值人数
        , sum(case when o.status = 1 then o.money*0.01 else 0 end) as  pay_amt  -- 成功充值金额
	from(
		select
		    t1.*
            , t2.status
            , t3.push_id
		from(
			select
			    distinct order_num,order_type,created_at,created_date,uid ,money
			from public.all_order_log o
			where 1=1
			    and o.environment = 1                                           -- 生产环境
			    and to_timestamp(created_at)::date >'2024-11-01'
		)t1
		left join(
			select
			    order_num
			    , status
			from public."oversea-api_osd_order" o
			where 1=1
			    and o.environment = 1
			    and to_timestamp(created_at)::date >'2024-11-01'
			    and order_num = 'SH184689470206554112'
			-- and order_num='SH120232395921248256'
			group by order_num
		)t2 on t1.order_num=t2.order_num
		left join(
			select
			    distinct order_num,push_id
			from public.all_order_log o
			where 1=1
			    and o.environment = 1
			    and push_id is not null and push_id<>''
			    and to_timestamp(created_at)::date >'2024-11-01'
			-- and order_type >=4
		)t3 on t1.order_num=t3.order_num
	)o
	group by
	    push_id
	    , to_timestamp(created_at)::date
    having push_id = 'person_4413'