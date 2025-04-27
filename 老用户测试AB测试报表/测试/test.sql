------------------------------------------
-- file: test.sql
-- author: xiaoj
-- time: 2025/4/25 17:07
-- description:
------------------------------------------
select
    EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 as h
from dim_homepage_user
limit 100;