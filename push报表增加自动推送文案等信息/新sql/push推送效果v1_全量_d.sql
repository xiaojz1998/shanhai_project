---------------------------------------------
-- File: push推送效果v1_全量_d.sql
-- Time: 2025/6/16 15:26
-- User: xiaoj
-- Description:  
---------------------------------------------
set timezone ='UTC-0';
---------------------------------------------
-- 建表
---------------------------------------------
-- 临时表
-- drop table if exists tmp.tmp_dw_push_view_v1;
CREATE TABLE if not exists tmp.tmp_dw_push_view_v1 (
    d_date text
    , push_id text
    , push_time text
    , layered_name text
    , lang_name text
    , internal_name text
    , push_title text
    , push_content text
    , sent_unt integer
    , push_unt integer
    , click_unt integer
    , popup_pv integer
    , popup_uv integer
    , watch_pv integer
    , watch_uv integer
    , watch_duration numeric(20,2)
    , watch_eid integer
    , all_pay_order integer
    , pay_amt numeric(20,2)
    , pay_order integer
    , pay_user integer
    , pay_cnt integer
);

-- 主表
-- drop table if exists public.dw_push_view_v1;
CREATE TABLE if not exists public.dw_push_view_v1 (
    d_date text NOT NULL
    , push_id text NOT NULL
    , push_time text
    , layered_name text
    , lang_name text
    , internal_name text
    , push_title text
    , push_content text
    , sent_unt integer
    , push_unt integer
    , click_unt integer
    , popup_pv integer
    , popup_uv integer
    , watch_pv integer
    , watch_uv integer
    , watch_duration numeric(20,2)
    , watch_eid integer
    , all_pay_order integer
    , pay_amt numeric(20,2)
    , pay_order integer
    , pay_user integer
    , pay_cnt integer
    , primary key (d_date, push_id)
);

---------------------------------------------
-- 全量更新
---------------------------------------------
truncate table tmp.tmp_dw_push_view_v1;
insert into tmp.tmp_dw_push_view_v1
-- 用户分层信息
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
-- 手动推送信息
-- 维度：push_id
, tmp_manual_push_info as(
	-- push_id 类似 xxx
    -- 每个push_id 对应一条
	select
        push_id
        , push_time
        , string_agg(layered_name,';' order by layered_name) as layered_name        -- 用户分层名称
        , array_agg(distinct lang_name)::text as lang_name                          -- 用户分层语言
        , push_title
        , push_content
        -- , jump_type
        -- , push_hz
        , sent_unt
        , push_unt
        , click_unt
	from(
		select
		    t1.*
		    , case when t2.lang_name='英语阿拉伯语' then '阿拉伯语' else t2.lang_name end as lang_name
		from(
			-- 从 public."oversea-api_osd_pushed" 取到推送相关信息，除了语言
			select
                id::text as push_id
                , to_timestamp(pushed_at)::text as push_time    -- 0时区
                , user_layered_configs                       -- 用户分层配置
                , json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'id' as layered_id
                , json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'name' as layered_name
                , title as push_title                        -- 推送标题
                , "content" as push_content                  -- 推送内容
                -- , jump_type                                  -- 类型
                -- , push_retry_times as push_hz                -- 推送次数
                , sent_count as sent_unt                     -- 命中用户数 推送人数
                , delivered_count as push_unt                -- 触达人数
                , click_count as click_unt                   -- 点击人数
			from public."oversea-api_osd_pushed" x          -- push推送表
			-- where 1=1
				-- and (id=3640  or id=3049 or id=3923)
				-- and user_layered_configs like '%首页推荐-泰语%'
		)t1
		left join tmp_user_layered_info t2 on t1.layered_id = t2.id::text   -- 补充用户分层语言

	)a
	group by push_id
            , push_time
            , push_title
            , push_content
            -- , jump_type
            -- , push_hz
            , sent_unt
            , push_unt
            , click_unt
)
-- 个性化推送
-- 维度：push_id 和 push_time
, tmp_person_push_info as(
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
)
-- 自动化push
-- 维度：push_id 和 push_time
, tmp_auto_push_info as (
    select
        t0.push_id
        , to_timestamp(t0.push_time)::date::text as push_time           -- 0 时区
        , '未知'::text as layered_name
        , concat('{',case when t2.name='英语阿拉伯语' then '阿拉伯语' else t2.name end,'}') as lang_name      -- 需要改
        , t1.admin_name as internal_name               -- 内部标题
        , lang_config::json -> concat(split_part(t0.push_id,'_',3) , '_' , split_part(t0.push_id,'_',4))->> 'title' as push_title -- 推送标题
        , lang_config::json -> concat(split_part(t0.push_id,'_',3) , '_' , split_part(t0.push_id,'_',4))->> 'content' as push_content -- 推送内容
        , sum(t0.sent_count) as sent_unt                     -- 命中用户数 推送人数
        , sum(t0.delivered_count) as push_unt                -- 触达人数
        , sum(t0.click_count) as click_unt                   -- 点击人数
    -- from public."oversea-api_osd_auto_pushes" t
    from public."oversea-api_osd_auto_push_logs" t0 -- on t.id = t0.task_id
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
-- push 推送效果
-- pushid 和 d_date 相同的补齐推送效果
, tmp_push_effect as (
    select push_id, push_time::timestamp::date as d_date,  sent_unt, push_unt, click_unt from tmp_manual_push_info
    union all
    select push_id,  push_time::timestamp::date as d_date,  sent_unt, push_unt, click_unt from tmp_person_push_info
    union all
    select push_id,  push_time::timestamp::date as d_date,  sent_unt, push_unt, click_unt from tmp_auto_push_info
)
-- 从埋点表统计推送效果
-- 维度：push_id 和 d_date
,tmp_push_log as(
	select
        push_id
        , d_date
        , sum(popup_pv) as popup_pv                                             -- 充值面板弹出次数
        , count(distinct case when popup_pv>0 then uid else null end) popup_uv  -- 充值面板弹出人数
        , sum(watch_pv) as watch_pv                                             -- 看剧次数
        , count(distinct case when watch_pv>0 then uid else null end) watch_uv  -- 看剧人数
        , sum(watch_duration) as watch_duration                                 -- 看剧时长(分)
        , sum(watch_eid) as watch_eid                                           -- 看剧集数
	from(
		select
            to_timestamp(created_at)::date as d_date    -- 数据日期
            , push_id                                   -- push_id
            , uid                                       -- 用户id
            , count(case when event=58 then uid else null end) as popup_pv
            , count(case when event in(1,2,13,14) and vid>0 and eid>0 then uid else null end) as watch_pv
            , round(sum(case when event=2 and vid>0 and eid>0 then watch_time else 0 end)/60.0,2) as watch_duration
            , count(distinct case when event in(1,2,13,14) and vid>0 and eid>0 then eid else null end) as watch_eid
		from public.app_user_track_log a
		where 1=1
            and event in (58,1,2,13,14)
            and push_id<>''
		    and created_date > 20241101
			-- and (push_id='3640' or push_id='3049' or push_id='3923')
			-- 58 进入充值弹窗就上报
		group by
            to_timestamp(created_at)::date
            , push_id
            , uid
	)t0
	group by
	    push_id
        , d_date
)
-- 从订单表统计推送订单效果
-- 维度：push_id 和 d_date
,tmp_push_order as(
    select
        push_id
        , to_timestamp(created_at)::date as d_date                                                                          -- 数据日期
        , count(distinct t3.order_num) as all_pay_order                                                                     -- 下单总数（包含失败单）
        , count(distinct case when t3.status = 1 and t3.is_refund is null then t3.order_num else null end) as  pay_order    -- 成功下单数
        , count(distinct case when t3.status = 1 and t3.is_refund is null then concat(t3.order_num,t3.created_at,t3.order_type) else null end) as  pay_cnt -- 成功充值次数
        , count(distinct case when t3.status = 1 and t3.is_refund is null then t3.uid else null end) as  pay_user           -- 成功充值人数
        , sum(case when t3.status = 1 and t3.is_refund is null then t3.money*0.01 else 0 end) as  pay_amt                                          -- 成功充值金额
    from(
        select
            push_id
            , t1.order_num
            , order_type
            , status                -- 订单状态
            , created_at
            , uid
            , money                 -- 订单金额
            , t2.is_refund          -- 用于判断是否退款
        from(
            select
                push_id
                , order_num
                , order_type
                , status
                , created_at
                , uid
                , money
            from public.all_order_log o
            where 1=1
                and o.environment = 1                                               -- 生产环境
                and push_id is not null and push_id != ''                           -- 过滤掉不合法push_id
                and to_timestamp(created_at)::date >'2024-11-01'
            group by push_id,order_num,order_type,status,created_at,uid ,money
        )t1
        left join(
            -- 判断当天的订单是否是退款单
            select
                order_num
                , 1::bigint is_refund
                , to_timestamp(created_at)::date as d_date
            from public.all_refund_order_log
            where 1 = 1
                and environment = 1
                and status = 1
                and to_timestamp(created_at)::date >'2024-11-01'
            group by order_num, to_timestamp(created_at)::date
        )t2 on t1.order_num = t2.order_num and to_timestamp(t1.created_at)::date = t2.d_date
    ) t3
    group by
        push_id
        , to_timestamp(created_at)::date
)
-- 基础维度表
-- 维度：push_id 、 d_date
, tmp_primary as(
	select push_id ,d_date
	from(
		select distinct push_id ,d_date from tmp_push_log                   -- 订单表统计
		union all
		select distinct push_id ,d_date from tmp_push_order  -- 埋点表统计
		union all
		select distinct push_id ,d_date from tmp_push_effect -- 业务库统计信息
	)a
	where push_id is not null and push_id <>'' and d_date is not null
	group by push_id ,d_date
)
-- push 基础信息
-- push id 相同补齐基础维度信息
, tmp_push_basic_info as (
    select push_id, push_time,layered_name, lang_name, internal_name, push_title, push_content
    from (
        select push_id, push_time,layered_name, lang_name, internal_name, push_title, push_content
        from (
            select push_id, push_time,layered_name, lang_name,'未知'::text as internal_name , push_title, push_content from tmp_manual_push_info
            union all
            select push_id, '', layered_name, lang_name, '未知'::text as internal_name ,push_title, push_content from tmp_person_push_info group by push_id, layered_name, lang_name, push_title, push_content
            union all
            select push_id, '', layered_name, lang_name, internal_name,push_title, push_content from tmp_auto_push_info group by push_id, layered_name, lang_name,internal_name, push_title, push_content
        ) t
        union all
        select push_id
            , ''
            , '未知'::text
            , concat('{',case when t2.name='英语阿拉伯语' then '阿拉伯语' else t2.name end,'}')
            , t1.admin_name
            , lang_config::json -> concat(split_part(t0.push_id,'_',3) , '_' , split_part(t0.push_id,'_',4))->> 'title'
            , lang_config::json -> concat(split_part(t0.push_id,'_',3) , '_' , split_part(t0.push_id,'_',4))->> 'content'
        from (select push_id from tmp_primary where split_part(push_id, '_', 4) != '') t0
        left join public."oversea-api_osd_auto_push_copywriter" t1 on split_part(t0.push_id,'_',2) = t1.id::text
        left join public."oversea-api_osd_lang" t2 on concat(split_part(t0.push_id,'_',3) , '_' , split_part(t0.push_id,'_',4)) = t2.lang_code
    ) t
    group by push_id, push_time,layered_name, lang_name, internal_name, push_title, push_content
)
select
    coalesce(tp.d_date::text,'') as d_date
    , tp.push_id
    --
    , case when t0.push_time = '' or t0.push_time is null then tp.d_date::text else t0.push_time end as push_time
    , t0.layered_name
    , t0.lang_name
    , t0.internal_name
    , t0.push_title
    , t0.push_content
    --
    , tpe.sent_unt
    , tpe.push_unt
    , tpe.click_unt
    --
    , tpl.popup_pv
    , tpl.popup_uv
    , tpl.watch_pv
    , tpl.watch_uv
    , tpl.watch_duration
    , tpl.watch_eid
    --
    , tpo.all_pay_order
    , tpo.pay_amt
    , tpo.pay_order
    , tpo.pay_user
    , tpo.pay_cnt
from tmp_primary tp -- 基础维度层 push_id 和 d_date
left join tmp_push_basic_info t0 on tp.push_id=t0.push_id
left join tmp_push_effect tpe  on tp.push_id=tpe.push_id and tp.d_date=tpe.d_date
left join tmp_push_log tpl on tp.push_id=tpl.push_id and tp.d_date=tpl.d_date
left join tmp_push_order tpo on tp.push_id=tpo.push_id and tp.d_date=tpo.d_date
where 1=1
    -- and tp.push_id= 'person_4355' and tp.d_date = '2025-05-21'
    -- and tp.d_date='2025-05-21' and tp.push_id = 'person_1935'
    --and (tp.push_id like 'H26%' or tp.push_id like 'G25%')
    -- and (t0.push_id is not null or t1.push_id is not null)          -- 过滤掉一部分新的推送
    -- and tp.push_id like '%auto%'
    -- and t0.push_id is null
    -- and tpo.all_pay_order is not null
;

-- 全量更新
truncate table public.dw_push_view_v1  ;
insert into public.dw_push_view_v1  select * from tmp.tmp_dw_push_view_v1;