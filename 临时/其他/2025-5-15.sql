------------------------------------------
-- file: 2025-5-15.sql
-- author: xiaoj
-- time: 2025/5/15 17:02
-- description:
------------------------------------------

-- 直接来源 public.dw_video_play_completed
-- 上游来源 dw_video_play  dw_video_completed

-- 测试 public.dw_video_play_completed dw_video_play  dw_video_completed 三张表都没有
select
    count(*)
from public.dw_video_play_completed
where vid in ('7600','7764','7777','7788','7792','8132','8159')

select
    count(*)
from dw_video_play
where vid in ('7600','7764','7777','7788','7792','8132','8159')

select
    count(*)
from dw_video_overview
where vid in ('7600','7764','7777','7788','7792','8132','8159')

-- 测试主表
with newuser_tb as (
			select
--          v_date as created_date
            d_date::date as d_date
           ,country_code
           ,ad_channel
           ,upper(put_system) as put_system
           ,ad_format
           ,uid::int8 as uid
           ,split_part(campaign_name,'_',5)::text as vid
           ,campaign_id,campaign_name
            from public.dwd_user_info
)
,tmp_vid_watch as (
    select
	 e0.vid::text as vid
    ,coalesce(nn.country_code,'UNKNOWN') as country_code
	,coalesce(nn.ad_channel,'未知') as ad_channel
	,count(distinct e0.uid) as vid_uv
	from(
	    select uid -- 历史所有观看用户，查询压力大
	    ,vid, eid
	    from public.app_user_track_log
	    where 1=1
	    and event in(2,13,14 ,1,87)
	    and vid>0 and eid>0
	    and  created_date>=20241020
	    	-- and to_timestamp(created_at)::date>= (current_date+interval '-1 day')::date -- 增
	    group by uid
	    ,vid, eid

		-- event in (2,13,14)
		-- 2 离开看剧页面的时候上报，统计观看时长	 drama_watch_time
		-- 13 向下滑动页面	drama_scroll_down
		-- 14 向上滑动页面	drama_scroll_up
		-- 1 进入短剧观看页面就上报	drama_watch
		-- 6 短剧观看页面完成广告任务点击领取奖励	drama_adpopup_claim
		--
	)e0
	left join newuser_tb nn on e0.uid=nn.uid
	left join "oversea-api_osd_video_episodes" e1 on e0.eid=e1.id
	where e0.vid=e1.vid
	group by
             e0.vid
	        ,coalesce(nn.country_code,'UNKNOWN')
	        ,coalesce(nn.ad_channel,'未知')
)select
     count(*)
 from tmp_vid_watch t0
left join "oversea-api_osd_videos" v1 on t0.vid=v1.id::text
left join "oversea-api_osd_categories" v2 on v1.category_id=v2.id
where v1.id is not null  and  vid::text in ('7600','7764','7777','7788','7792','8132','8159')