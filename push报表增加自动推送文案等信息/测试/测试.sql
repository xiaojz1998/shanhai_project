---------------------------------------------
-- File: 测试.sql
-- Time: 2025/6/16 15:28
-- User: xiaoj
-- Description:  
---------------------------------------------

-- 查看 oversea-api_osd_auto_push_copywriter 表的内容
select
    *
from public."oversea-api_osd_auto_push_copywriter";

select
    distinct name
from public."oversea-api_osd_lang";

select
    *
from all_refund_order_log
where order_num = 'SH184689470206554112';



    select
        *
    from(
			select
			    push_id,order_num,order_type,created_at,uid ,money
			from public.all_order_log o
			where 1=1
			    and o.environment = 1                                           -- 生产环境
			    and push_id is not null and push_id<>''                         -- 过滤掉不合法push_id
			    and to_timestamp(created_at)::date >'2024-11-01'
			group by push_id,order_num,order_type,created_at,uid ,money
		)t1
		left join(
		    select
		        order_num
		        , 0::bigint as status
		        , to_timestamp(created_at)::date as d_date
		    from public.all_refund_order_log
		    where 1 = 1
		        and environment = 1
		        and status = 1
		        and to_timestamp(created_at)::date >'2024-11-01'
		    group by order_num, to_timestamp(created_at)::date
		)t2 on t1.order_num=t2.order_num and to_timestamp(t1.created_at)::date = t2.d_date
    where t1.order_num = 'SH184689470206554112' and status != 0

    select null != 0


select * from public."oversea-api_osd_auto_push_logs";

select split_part('G23_1_en_US','_',4)
select max('sfsf',null)

-- 自动推送代码
with tmp_auto_push_info as (
    select
        t0.push_id
        , to_timestamp(t0.push_time)::date::text as push_time           -- 0 时区
        , '未知'::text as layered_name
        , concat('{',case when t2.name='英语阿拉伯语' then '阿拉伯语' else t2.name end,'}') as lang_name      -- 需要改
        , t1.admin_name as internal_title               -- 内部标题
        , lang_config::json -> concat(split_part(t0.push_id,'_',3) , '_' , split_part(t0.push_id,'_',4))->> 'title' as push_title -- 推送标题
        , lang_config::json -> concat(split_part(t0.push_id,'_',3) , '_' , split_part(t0.push_id,'_',4))->> 'content' as push_content -- 推送内容
        , sum(t0.sent_count) as sent_unt                     -- 命中用户数 推送人数
        , sum(t0.delivered_count) as push_unt                -- 触达人数
        , sum(t0.click_count) as click_unt                   -- 点击人数
    from public."oversea-api_osd_auto_pushes" t
    left join public."oversea-api_osd_auto_push_logs" t0 on t.id = t0.task_id
    left join public."oversea-api_osd_auto_push_copywriter" t1 on split_part(t0.push_id,'_',2) = t1.id::text
    left join public."oversea-api_osd_lang" t2 on concat(split_part(t0.push_id,'_',3) , '_' , split_part(t0.push_id,'_',4)) = t2.lang_code
    where t0.push_id is not null and t0.push_id <> '' and  schedule_id != '' and lang != ''   -- 过滤掉老推送数据
    group by t0.push_id
        , to_timestamp(t0.push_time)::date::text
        , concat('{',case when t2.name='英语阿拉伯语' then '阿拉伯语' else t2.name end,'}')
        , t1.admin_name
        , lang_config::json -> concat(split_part(t0.push_id,'_',3) , '_' , split_part(t0.push_id,'_',4))->> 'title'
        , lang_config::json -> concat(split_part(t0.push_id,'_',3) , '_' , split_part(t0.push_id,'_',4))->> 'content'
)