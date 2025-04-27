---------------------------------------------------------------
--  寻找点击大于展示的问题
---------------------------------------------------------------
select
    ip,
    view_pv,
    click_pv
from (select
    ip,
    count(
        distinct case
        when event = 114 and get_json_object(ext_body, '$.reason')='0' and get_json_object(ext_body, '$.is_optimize') = '1'then id
        else null
        end
    ) as view_pv ,
    count(
        distinct case
        when event = 115 and get_json_object(ext_body, '$.reason')='0' and get_json_object(ext_body, '$.is_optimize') = '1'
            and get_json_object(ext_body, '$.go_page_url') not like 'https://san-api.stardust-tv.com%'
            then id
        else null
        end
    ) as click_pv
from "web_user_track_log"
where event in (114, 115, 127, 133)
    and to_timestamp(created_at) :: date between '2025-04-18' and current_date
group by ip) t
where click_pv>view_pv
