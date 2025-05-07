------------------------------------------
-- file: 增量转全量脚本.sql
-- author: xiaoj
-- time: 2025/4/29 01:45
-- description:
------------------------------------------
SET odps.sql.timezone = UTC;

-- 建表
CREATE TABLE IF NOT EXISTS ods_cs_data_log_app_install_event_log_hf(
`id`                            BIGINT COMMENT '主键id',
`platform`                      STRING COMMENT '手机系统',
`bundle_id`                     STRING COMMENT '包id',
`app_name`                      STRING COMMENT '包名',
`af_id`                         STRING COMMENT 'appsfyler-id',
`country_code`                  STRING COMMENT '国家地区代码',
`install_date`                  BIGINT COMMENT '安装时间（北京时区，YMD）',
`install_time`                  STRING COMMENT '安装时间（当日时区）',
`api_version`                   STRING COMMENT '',
`ip`                            STRING COMMENT '',
`app_id`                        STRING COMMENT '',
`idfa`                          STRING COMMENT '',
`idfv`                          STRING COMMENT '',
`event_name`                    STRING COMMENT '',
`created_at`                    BIGINT COMMENT '上报时间',
`created_date`                  BIGINT COMMENT '上报时间（YMD）',
`event_time`                    STRING COMMENT '上报时间(当日时区)',
`campaign_name`                 STRING COMMENT '',
`agent`                         STRING COMMENT '',
`campaign_id`                   STRING COMMENT '',
`ad_group_id`                   STRING COMMENT '',
`ad_id`                         STRING COMMENT '',
`put_system`                    STRING COMMENT '',
`v_type`                        STRING COMMENT '',
`ad_format`                     STRING COMMENT '',
`ad_channel`                    STRING COMMENT '',
`device_download_time`          STRING COMMENT '下载时间',
`media_source`                  STRING COMMENT '',
`ad_group_name`                 STRING COMMENT '',
`ad_name`                       STRING COMMENT '',
`install_at`                    BIGINT COMMENT '安装时间（北京时区）',
`ad_group_id_s`                 STRING COMMENT 'ad_group_id',
`ad_group_names`                STRING COMMENT '',
`android_id`                    STRING COMMENT '',
`campaign_type`                 STRING COMMENT '',
`retargeting_conversion_type`   STRING COMMENT '',
`conversion_type`               STRING COMMENT ''
)
COMMENT 'app安装信息'
PARTITIONED BY (dt STRING COMMENT '日期') ;


-- 每小时增量更新安装信息
insert overwrite mcstv.ods_cs_data_log_app_install_event_log_hf partition(dt='${dt}')
select
    t1.id,platform,bundle_id,app_name,af_id,country_code,install_date,install_time,api_version,ip,app_id,idfa,
    idfv,event_name,created_at,created_date,event_time,campaign_name,agent,campaign_id,ad_group_id, ad_id,
    put_system,v_type,ad_format,ad_channel,device_download_time,media_source,ad_group_name,ad_name,install_at,
    ad_group_id_s,ad_group_names,android_id,campaign_type,retargeting_conversion_type,conversion_type
from (
    --每天9点初始化当天分区的全量订单
    select
    t1.id,platform,bundle_id,app_name,af_id,country_code,install_date,install_time,api_version,ip,app_id,idfa,
    idfv,event_name,created_at,created_date,event_time,campaign_name,agent,campaign_id,ad_group_id, ad_id,
    put_system,v_type,ad_format,ad_channel,device_download_time,media_source,ad_group_name,ad_name,install_at,
    ad_group_id_s,ad_group_names,android_id,campaign_type,retargeting_conversion_type,conversion_type
    from (
        -- 上一日分区的所有数量
        select * from mcstv.ods_cs_data_log_app_install_event_log_hf where dt=date_add('${dt}',-1) and '${hr}'='00'
    ) t1
    left join (
        select id from mcstv.ods_cs_data_log_app_install_event_log_hf where dt='${dt}'
    ) t2 on t1.id = t2.id
    where t2.id is null

    union all
    -- 今日分区的数据
    select
    id,platform,bundle_id,app_name,af_id,country_code,install_date,install_time,api_version,ip,app_id,idfa,
    idfv,event_name,created_at,created_date,event_time,campaign_name,agent,campaign_id,ad_group_id, ad_id,
    put_system,v_type,ad_format,ad_channel,device_download_time,media_source,ad_group_name,ad_name,install_at,
    ad_group_id_s,ad_group_names,android_id,campaign_type,retargeting_conversion_type,conversion_type
    from mcstv.ods_cs_data_log_app_install_event_log_hf WHERE  dt='${dt}'
) t1
LEFT join (
    select id from mcstv.stage_cs_data_log_app_install_event_log_hi where dt='${dt}'
) t2 on t1.id = t2.id
WHERE t2.id is null

union all

select
    id,platform,bundle_id,app_name,af_id,country_code,install_date,install_time,api_version,ip,app_id,idfa,
    idfv,event_name,created_at,created_date,event_time,campaign_name,agent,campaign_id,ad_group_id, ad_id,
    put_system,v_type,ad_format,ad_channel,device_download_time,media_source,ad_group_name,ad_name,install_at,
    ad_group_id_s,ad_group_names,android_id,campaign_type,retargeting_conversion_type,conversion_type
from mcstv.stage_cs_data_log_app_install_event_log_hi WHERE dt='${dt}' ;
