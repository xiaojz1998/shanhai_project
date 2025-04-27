select t1.id,title,content,jump_type,vid,name
from public."oversea-api_osd_pushed" t1
left join public."oversea-api_osd_videos" t2 on t1.vid=t2.id