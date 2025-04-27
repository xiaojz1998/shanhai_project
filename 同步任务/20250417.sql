-- 同步app_install_event_log
select
    *
from app_install_event_log
where created_date = '20250410' or to_char(to_timestamp(created_at) at time zone 'UTC','yyyy-mm-dd')='2025-04-10'
limit 100