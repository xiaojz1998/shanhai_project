--总数据量  836906条
select count(*) from analysis.dw_operate_roi_v2_calc01;

-- 测试dw_operate_schedule 中
select d_date,country_name,lang from dw_operate_schedule group by d_date,country_name,lang