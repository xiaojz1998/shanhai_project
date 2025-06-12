---------------------------------------------
-- File: 测试.sql
-- Time: 2025/6/10 11:10
-- User: xiaoj
-- Description:  
---------------------------------------------
select pushed_at from public."oversea-api_osd_pushed" limit 100; -- 1724547601

-- 查看语言
select distinct name from public."oversea-api_osd_lang"


with tmp_push as (
    select
        push_date,
        lang,
        sum(push_unt) total_push_unt,
        sum(click_unt) total_click_unt,
        case when sum(push_unt)=0 then null
            else 1.0*max(click_unt)/max(push_unt)
            end as push_click_rate
    from (select
        push_id,
        push_time::date as push_date,
        push_unt,
        click_unt,
        case
             when t2.lang_name='英语阿拉伯语' then '阿拉伯语'
             when t2.lang_name=''or t2.lang_name is null then 'UNKNOWN' else t2.lang_name end as lang
    from(
        select
         id::text as push_id
        ,to_timestamp(pushed_at) at time zone 'UTC-8' as push_time  --,to_timestamp(pushed_at) at time zone 'UTC-8'
        ,json_array_elements((REGEXP_MATCH(replace(user_layered_configs, '\"', '"'),'\[.*?\]'))[1]::json) ->> 'id' as layered_id
        ,delivered_count as push_unt
        ,click_count as click_unt
        from public."oversea-api_osd_pushed" x
        -- 增量更新
        -- where (to_timestamp(pushed_at) at time zone 'UTC-8') ::date >=(current_date+interval '-2 day')::date
    )t1
    left join(
        select t1.*,t2."name" as lang_name
        from(
        select
            id ,
            lang_config::json ->> 0 as lang_code
        from public."oversea-api_osd_user_layered_configs"
        where lang_config <>'[]'
        )t1
        left join public."oversea-api_osd_lang" t2 on t1.lang_code=t2.lang_code
    )t2 on t1.layered_id=t2.id::text ) t3
    group by push_date,lang
)