-- 飞书机器人-按剧消耗排序
with newuser_tb as(
select * from(
select to_timestamp(created_at)::date as d_date    
,case when (area='' or area is null) then 'UNKNOWN' else upper(area) end  as country_code        
,upper(case when campaign_id = '0' or campaign_id = '' then '自然'
when ad_channel = 'tt' then 'tt'
when ad_channel in ('fb','metaweb') then 'fb'
when ad_channel = 'apple' then 'apple'
when ad_channel = 'moloco_int' then 'moloco_int'
when substr(ad_channel,1,12) = 'social_media' then '社媒'
else '未知' end) as ad_channel
,upper(put_system) as put_system
,v_type
,ad_format
,uid
,split_part(campaign_name ,'_',5)::text as vid -- 不可用，使用消耗表的name 
,campaign_id,campaign_name
,row_number() over(partition by uid order by created_at) rk  -- 可能存在多次归因，以第一次为准
from public.user_log
where event = 1
and created_date >= 20240701) tmp 
where rk = 1 
and d_date = current_date - 1)


,tmp_user_pay as(
select p_date,uid
,sum(pay_amt) as pay_amt
from(
select to_timestamp(o.created_at)::date as p_date 
,o.uid 
,sum(o.money)*0.01 as pay_amt
from public.all_order_log o
where environment = 1  and status = 1 
and to_timestamp(o.created_at)::date = current_date - 1
group by to_timestamp(o.created_at)::date 
,o.uid 

union all 
select to_timestamp(r.refund_time)::date as p_date 
,uid 
,-sum(r.money)*0.01 as refund_amt  -- 减去退款
from public.all_refund_order_log r 
where r.environment = 1 and r.status = 1 
and to_timestamp(r.refund_time)::date = current_date - 1
group by to_timestamp(r.refund_time)::date 
,r.uid)a  
group by p_date,uid)

,tmp_camp_roi as(
select 
n1.campaign_id
,count(distinct n1.uid) as new_uv --当日充值新用户数
,sum(pay_amt) as pay_0
from newuser_tb n1 
inner join tmp_user_pay p1 on n1.uid=p1.uid and n1.d_date=p1.p_date              
group by 
n1.campaign_id)

, cost as (select 
split_part(campaign_name, '_', 5)  as vid,campaign_id
, round(sum(cost_amount)*1.0/10000,2) 剧消耗
  from  ad_cost_data_log
  where TO_TIMESTAMP(created_at) :: date = current_date - 1
  group by split_part(campaign_name, '_', 5), campaign_id) 

,tmp_primary as(
select distinct tb.campaign_id  ,coalesce(tc.vid,'UNKNOWN') as vid
from(
select distinct campaign_id from newuser_tb 
union all 
select distinct campaign_id from cost) tb 
  left join( select distinct campaign_id,vid from cost ) tc on tb.campaign_id=tc.campaign_id
  )

, new_roi as
(select a.vid, sum(剧消耗) 剧消耗, sum(pay_0) 新用户首日充值金额
from tmp_primary a left join tmp_camp_roi b on a.campaign_id = b.campaign_id
left join cost c on a.campaign_id = c.campaign_id
group by a.vid)

, active as 
(select count(distinct uid) dau
from ( 
  select uid, TO_TIMESTAMP(created_at) :: date active_date
from app_user_track_log
where true 
and event in (1,16)
and TO_TIMESTAMP(created_at) :: date = current_date - 1
group by uid, TO_TIMESTAMP(created_at) :: date

union 

select uid, TO_TIMESTAMP(created_at) :: date active_date
from user_log
where true 
and TO_TIMESTAMP(created_at) :: date = current_date - 1
group by uid, TO_TIMESTAMP(created_at) :: date) a)

, drama as (select
a.vid
, count(distinct a.uid) 剧播放人数
, count(a.uid) 剧播放量
, count(distinct concat(uid,'+',eid)) 看剧集数
, case when count(distinct a.uid) = 0 then 0 
else round(count(distinct concat(uid,'+',eid))*1.0/count(distinct a.uid),1) end as 人均播放集数
, case when count(distinct a.uid) = 0 then 0 
 else round(sum(case when event =2 then watch_time else 0 end)*1.0/(count(distinct a.uid)*60),1) end as "人均看剧时长(分钟)"
from app_user_track_log a 
where 1=1
and event in (1,2,13, 14) 
and vid>0 
and eid>0
and TO_TIMESTAMP(a.created_at) :: date = current_date - 1
group by a.vid)

, revenue as 
(select 
goods_id
, count(distinct uid) as 付费人数 
, count(uid) as 充值次数
, round(sum(money)*1.0/100,2) 充值金额
, case when count(distinct uid) = 0 then 0
else round((sum(money)*1.0/100)/count(distinct uid),2) end as up值
from all_order_log b
where 1=1
and status = 1
and TO_TIMESTAMP(created_at) :: date = current_date - 1
and goods_id !=  0
and environment = 1
group by goods_id
)

, drama_total as (SELECT COALESCE(c.name,'总计') 语种, b.name 剧名,b.english_name as 剧译名, b.id
FROM  "oversea-api_osd_videos" b
LEFT join "oversea-api_osd_categories" c on b.category_id = c.id
GROUP BY rollup(c.name), b.name,b.english_name, b.id )

, rank as 
 (select
  语种
  , 剧名
  , 剧译名
  , COALESCE(剧播放人数,0) 剧播放人数
  , COALESCE("人均看剧时长(分钟)",0) 人均看剧时长
  , COALESCE(人均播放集数,0) 人均播放集数
    , COALESCE(case when dau = 0 or dau is null then 0
  else round(剧播放人数*100.0/dau,2) end,0) as 播放率
  , COALESCE(充值金额,0) 充值金额
  , COALESCE(充值次数,0) 充值次数
  , COALESCE(up值,0) up值
  , COALESCE(case when 剧播放人数 = 0 then 0
  else round(付费人数*100.0/剧播放人数,2) end,0) as 付费率
  , COALESCE(剧消耗,0) 剧消耗
  , COALESCE(case when 剧消耗 = 0 or 剧消耗 is null then 0
  else round(新用户首日充值金额*100.0/剧消耗,2) end,0) as 新用户ROI
from drama_total a left join drama b on a.id = b.vid
left join revenue c on a.id = c.goods_id
left join new_roi d on cast(a.id as varchar) = d.vid
left join active e on 1=1)

-- 英语
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '英语'
order by 剧消耗 desc, 充值金额 desc limit 10


-- 葡萄牙语
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '葡萄牙语'
order by 剧消耗 desc, 充值金额 desc limit 10

-- 西班牙语
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '西班牙语'
order by 剧消耗 desc, 充值金额 desc limit 10


-- 印尼语
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '印尼语'
order by 剧消耗 desc, 充值金额 desc limit 10


-- 德语
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '德语'
order by 剧消耗 desc, 充值金额 desc limit 10


-- 韩语
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '韩语'
order by 剧消耗 desc, 充值金额 desc limit 10


-- 日语
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '日语'
order by 剧消耗 desc, 充值金额 desc limit 10

-- 法语
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '法语'
order by 剧消耗 desc, 充值金额 desc limit 10


-- 泰语
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '泰语'
order by 剧消耗 desc, 充值金额 desc limit 10


-- 中文简体
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '中文简体'
order by 剧消耗 desc, 充值金额 desc limit 10


-- 繁体中文
select 
  语种
  , 剧名
  , 剧译名
  , 剧播放人数
  , concat(cast(播放率 as varchar),'%') as 播放率
  , 人均看剧时长
  , 人均播放集数
  , 充值金额
  , 充值次数
  , up值
  , concat(cast(付费率 as varchar),'%') as 付费率
  , 剧消耗
  , 新用户ROI
from rank
where 语种 = '繁体中文'
order by 剧消耗 desc, 充值金额 desc limit 10
