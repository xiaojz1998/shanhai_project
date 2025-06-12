---------------------------------------------
-- File: 测试.sql
-- Time: 2025/6/4 15:18
-- User: xiaoj
-- Description:  
---------------------------------------------

select d_date
FROM
  public.dw_video_overview t2
where vid=8826
group by d_date
order by d_date desc