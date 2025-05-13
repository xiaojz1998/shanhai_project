
SET odps.sql.timezone = UTC;

WITH user_active AS
(
    SELECT  CAST(uid AS BIGINT) uid
            ,d_date
            ,vid
            ,eid
            ,event_name
            ,CAST(ad_type AS INT) ad_type
    FROM    mcstv.dwd_user_active_hi
    WHERE   dt >= DATE_ADD(CURRENT_DATE(),-10)
    AND     event_name IN ('rewards_checkin','rewards_claim','rewards_task_go','rewards_task_claim','unlockEpisodes') -- 短剧观看页面完成看广告任务时触发	drama_adpopup_claim
    -- 到付费集时，购买K币、订阅会员、用已有的K币和广告解锁付费集时上报 drama_unlock_success
)
,t_order AS
(
    --付款和退款
    SELECT  uid
            ,d_date
            ,CAST(pay_statistics AS INT) pay_statistics
            ,SUM(recharge_money) recharge_money
            ,SUM(recharge_times) recharge_times
            ,SUM(cancel_recharge_times) cancel_recharge_times
    FROM    (
                SELECT  CAST(uid AS BIGINT) uid
                        ,d_date
                        ,(IF(order_status = 1 AND environment = 1,money,0)) recharge_money
                        ,(IF(order_status = 1 AND environment = 1,1,0)) recharge_times
                        ,(IF(order_status = 2 AND environment = 1,1,0)) cancel_recharge_times
                        ,CASE   WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') = 0 THEN '1'
                                WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 3 THEN '3' -- WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') >= 3 AND DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN 'current_4_7_pay'
                                WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN '7'
                                WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 15 THEN '15'
                                ELSE '999'
                        END AS pay_statistics
                FROM    mcstv.dwd_order_hi
                WHERE   dt >= DATE_ADD(CURRENT_DATE(),-20)
            ) t
    GROUP BY uid
             ,d_date
             ,pay_statistics
)
,t_order_detail AS
(
    SELECT  uid
            ,SUM(CASE    WHEN pay_statistics <= 1 THEN recharge_money ELSE 0 END) current_pay_money
            ,SUM(CASE    WHEN pay_statistics <= 1 THEN recharge_times ELSE 0 END) current_pay_times
            ,SUM(CASE    WHEN pay_statistics <= 1 THEN cancel_recharge_times ELSE 0 END) current_cancel_times
            ,SUM(CASE    WHEN pay_statistics <= 3 THEN recharge_money ELSE 0 END) current_3_pay_money
            ,SUM(CASE    WHEN pay_statistics <= 3 THEN recharge_times ELSE 0 END) current_3_pay_times
            ,SUM(CASE    WHEN pay_statistics <= 3 THEN cancel_recharge_times ELSE 0 END) current_3_cancel_times
            ,SUM(
                CASE    WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') >= 3 AND DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN recharge_money ELSE 0 END
            ) current_4_7_pay_money
            ,SUM(
                CASE    WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') >= 3 AND DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN recharge_times ELSE 0 END
            ) current_4_7_pay_times
            ,SUM(
                CASE    WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') >= 3 AND DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN cancel_recharge_times ELSE 0 END
            ) current_4_7_cancel_times
            ,SUM(CASE    WHEN pay_statistics <= 7 THEN recharge_money ELSE 0 END) current_7_pay_money
            ,SUM(CASE    WHEN pay_statistics <= 7 THEN recharge_times ELSE 0 END) current_7_pay_times
            ,SUM(CASE    WHEN pay_statistics <= 7 THEN cancel_recharge_times ELSE 0 END) current_7_cancel_times
            ,SUM(CASE    WHEN pay_statistics <= 15 THEN recharge_money ELSE 0 END) current_15_pay_money
            ,SUM(CASE    WHEN pay_statistics <= 15 THEN recharge_times ELSE 0 END) current_15_pay_times
            ,SUM(CASE    WHEN pay_statistics <= 15 THEN cancel_recharge_times ELSE 0 END) current_15_cancel_times
    FROM    t_order
    GROUP BY uid
)
,t_active AS
(
    -- 活跃用户
    SELECT  uid
            ,CAST(act_statistics AS INT) act_statistics
            ,d_date
    FROM    (
                SELECT  CAST(uid AS BIGINT) uid
                        ,d_date
                        ,CASE   WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') = 0 THEN '1'
                                WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 3 THEN '3' -- WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') >= 3 AND DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN 'current_4_7_act'
                                WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN '7'
                                WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 15 THEN '15'
                                WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 30 THEN '30'
                                ELSE '999'
                        END AS act_statistics
                FROM    mcstv.dwd_user_active_hi
                WHERE   dt >= DATE_ADD(CURRENT_DATE(),-32)
            ) t
    GROUP BY uid
             ,act_statistics
             ,d_date
) -- select 1
,t_active_detail AS
(
    SELECT  uid
            ,COUNT(DISTINCT CASE    WHEN act_statistics <= 1 THEN d_date END) current_act_day
            ,COUNT(DISTINCT CASE    WHEN act_statistics <= 3 THEN d_date END) current_3_act_days
            ,COUNT(DISTINCT
                  CASE    WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') >= 3 AND DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN d_date END
            ) current_4_7_act_days
            ,COUNT(DISTINCT CASE    WHEN act_statistics <= 7 THEN d_date END) current_7_act_days
            ,COUNT(DISTINCT CASE    WHEN act_statistics <= 15 THEN d_date END) current_15_act_days
            ,COUNT(DISTINCT CASE    WHEN act_statistics <= 30 THEN d_date END) current_30_act_days
    FROM    t_active
    GROUP BY uid
)
,t_ad_unlock AS
(
    SELECT  uid
            ,COUNT(DISTINCT
                  CASE    WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 1 THEN CONCAT(vid,eid) END
            ) current_1_ad_unlock
            ,COUNT(DISTINCT
                  CASE    WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 3 THEN CONCAT(vid,eid) END
            ) current_3_ad_unlock
            ,COUNT(DISTINCT
                  CASE    WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN CONCAT(vid,eid) END
            ) current_7_ad_unlock
    FROM    user_active
    WHERE   event_name = 'unlockEpisodes'
    AND     ad_type = 1
    GROUP BY uid
)
,t_k_coin_unlock AS
(
    SELECT  uid
            ,COUNT(DISTINCT
                  CASE    WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 1 THEN CONCAT(vid,eid) END
            ) current_1_k_unlock
            ,COUNT(DISTINCT
                  CASE    WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 3 THEN CONCAT(vid,eid) END
            ) current_3_k_unlock
            ,COUNT(DISTINCT
                  CASE    WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN CONCAT(vid,eid) END
            ) current_7_k_unlock
    FROM    user_active
    WHERE   event_name = 'unlockEpisodes'
    AND     ad_type = 2
    GROUP BY uid
)
,t_sign_ad_task AS
(
    -- 签到位广告任务完成次数
    SELECT  uid
            ,SUM(
                IF(DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 1,1,0)
            ) current_1_ad_task_times
            ,SUM(
                IF(DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 3,1,0)
            ) current_3_ad_task_times
    FROM    user_active
    WHERE   event_name = 'rewards_claim'
    GROUP BY uid
) --  select 1
,t_inspirit_ad_task AS
(
    -- 任务中心激励广告完成次数
    SELECT  uid
            ,SUM(
                IF(DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 1,1,0)
            ) current_1_inspirit_ad_task_times
            ,SUM(
                IF(DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 3,1,0)
            ) current_3_inspirit_ad_task_times
    FROM    user_active
    WHERE   event_name = 'rewards_task_go'
    GROUP BY uid
)
,t_h5_task AS
(
    -- 任务中心H5广告完成次数
    SELECT  uid
            ,SUM(
                IF(DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 1,1,0)
            ) current_1_h5_ad_task_times
            ,SUM(
                IF(DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 3,1,0)
            ) current_3_h5_ad_task_times
    FROM    user_active
    WHERE   event_name = 'rewards_task_claim'
    GROUP BY uid
) --  select 1
,t_sign AS
(
    -- 近7天签到次数
    SELECT  uid
            ,SUM(
                IF(DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(d_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7,1,0)
            ) current_7_sign_times
    FROM    user_active
    WHERE   event_name = 'rewards_checkin'
    GROUP BY uid
)
,t_subscription AS
(
    SELECT  status
            ,uid
    FROM    (
                SELECT  status
                        ,CAST(uid AS BIGINT) uid
                        ,ROW_NUMBER() OVER (PARTITION BY uid ORDER BY updated_at DESC ) rn
                FROM    mcstv.ods_middle_subscription_hf
                WHERE   dt >= '2024-01-01'
            ) t
    WHERE   rn = 1
)
,t_history AS
(
    SELECT  a.uid
            ,IF(b.uid IS NOT NULL,act_days + 1,act_days) act_days
            ,IF(a.vids IS NOT NULL AND b.vids IS NOT NULL,ARRAY_UNION(a.vids,b.vids),COALESCE(a.vids,b.vids)) vids
            ,a.pay_order + COALESCE(o.times,0) pay_order
            ,a.pay_amt + COALESCE(o.money,0) pay_amt
    FROM    (
                SELECT  CAST(uid AS BIGINT) uid
                        ,act_days
                        ,vids
                        ,pay_order
                        ,pay_amt
                FROM    mcstv.dws_user_history_metrics_di
                -- WHERE   dt = DATE_ADD(CURRENT_DATE(),-1)
                -- 取最新时间的分区
                where dt = (SELECT MAX(dt) FROM  mcstv.dws_user_history_metrics_di)
            ) a
    LEFT JOIN   (
                    SELECT  uid
                            ,COLLECT_SET(vids) vids
                    FROM    (
                                SELECT  CAST(uid AS BIGINT) uid
                                        ,CONCAT_WS('|',vid,eid) vids
                                FROM    mcstv.dwd_user_active_hi
                                WHERE   dt >= (SELECT DATE_ADD(MAX(dt),1)  FROM  mcstv.dws_user_history_metrics_di)
                                AND     eid IS NOT NULL
                            ) t
                    GROUP BY uid
                ) b
    ON      a.uid = b.uid
    LEFT JOIN   (
                    SELECT  CAST(uid AS BIGINT) uid
                            ,COUNT(1) times
                            ,SUM(money) money
                    FROM    mcstv.dwd_order_hi
                    WHERE   dt >= (SELECT DATE_ADD(MAX(dt),1)  FROM  mcstv.dws_user_history_metrics_di)
                    GROUP BY uid
                ) o
    ON      a.uid = o.uid
) -- select 1
INSERT OVERWRITE TABLE mcstv.dws_user_level_label_hf
SELECT  a.uid
        ,ad_channel --渠道
        ,os --设备
        ,app_name --应用
        ,app_id --应用渠道
        ,lang_name --语言
        ,country_name --国家名称
        ,country_level --国家梯度
        ,CAST(uid_last_digit AS INT) uid_last_digit --用户尾号
        ,is_iaa
        ,reg_days reg_days
        ,IF(reg_statistics = 'current_reg',1,0) current_reg
        ,IF(reg_statistics IN ('current_3_reg','current_reg'),1,0) current_3_reg
        ,IF(reg_statistics = 'current_4_7_reg',1,0) current_4_7_reg
        ,IF(reg_statistics IN ('current_reg','current_3_reg','current_4_7_reg'),1,0) current_7_reg
        ,IF(reg_statistics = 'current_8_15_reg',1,0) current_8_15_reg
        ,IF(reg_statistics = 'current_15_30_reg',1,0) current_15_30_reg
        ,IF(reg_statistics = 'current_31_90_reg',1,0) current_31_90_reg
        ,IF(reg_statistics = 'current_90_reg',1,0) current_90_reg
        ,COALESCE(h.act_days,0) act_days -- 历史活跃天数
        ,COALESCE(current_act_day,0) current_act_day --近1天活跃天数
        ,COALESCE(current_3_act_days,0) current_3_act_days --近3天活跃天数
        ,COALESCE(current_4_7_act_days,0) current_4_7_act_days --近4-7天活跃天数
        ,COALESCE(current_7_act_days,0) current_7_act_days --近7天活跃天数
        ,COALESCE(current_15_act_days,0) current_15_act_days --近15天活跃天数
        ,COALESCE(current_30_act_days,0) current_30_act_days --近30天活跃天数
        ,COALESCE(h.pay_order,0) pay_times -- 历史充值次数
        ,COALESCE(current_pay_times,0) current_pay_times --近1天充值次数
        ,COALESCE(current_3_pay_times,0) current_3_pay_times --近3天充值次数
        ,COALESCE(current_4_7_pay_times,0) current_4_7_pay_times --近4-7天充值次数
        ,COALESCE(current_7_pay_times,0) current_7_pay_times --近7天充值次数
        ,COALESCE(current_15_pay_times,0) current_15_pay_times --近15天充值次数
        ,COALESCE(current_cancel_times,0) current_cancel_times --近1天取消充值次数
        ,COALESCE(current_3_cancel_times,0) current_3_cancel_times --近3天取消充值次数
        ,COALESCE(current_7_cancel_times,0) current_7_cancel_times --近7天取消充值次数
        ,COALESCE(CAST(h.pay_amt*100 AS BIGINT),0) pay_money -- 历史充值金额
        ,COALESCE(CAST(current_pay_money*100 AS BIGINT),0) current_pay_money --近1天充值金额
        ,COALESCE(CAST(current_3_pay_money*100 AS BIGINT),0) current_3_pay_money --近3天充值金额
        ,COALESCE(CAST(current_7_pay_money*100 AS BIGINT),0) current_7_pay_money --近7天充值金额
        ,dsub.status subscribe_status --订阅状态
        ,COALESCE(h.vids.size(),0) history_watch_eids --历史观看集数
        ,COALESCE(current_1_ad_unlock,0) current_1_ad_unlock --近1天广告解锁剧集
        ,COALESCE(current_3_ad_unlock,0) current_3_ad_unlock --近3天广告解锁剧集
        ,COALESCE(current_7_ad_unlock,0) current_7_ad_unlock --近7天广告解锁剧集
        ,COALESCE(current_1_k_unlock,0) current_1_k_unlock --近1天K币解锁剧集
        ,COALESCE(current_3_k_unlock,0) current_3_k_unlock --近3天K币解锁剧集
        ,COALESCE(current_7_k_unlock,0) current_7_k_unlock --近7天K币解锁剧集
        ,COALESCE(current_1_ad_task_times,0) current_1_ad_task_times --近1天签到位广告任务完成次数
        ,COALESCE(current_3_ad_task_times,0) current_3_ad_task_times --近3天签到位广告任务完成次数
        ,COALESCE(current_1_inspirit_ad_task_times,0) current_1_inspirit_ad_task_times --近1天任务中心激励广告完成次数
        ,COALESCE(current_3_inspirit_ad_task_times,0) current_3_inspirit_ad_task_times --近3天任务中心激励广告完成次数
        ,0 current_1_inspirit_ad_show_click_times --近1天激励广告展示过程中点击次数
        ,0 current_3_inspirit_ad_show_click_times --近3天激励广告展示过程中点击次数
        ,COALESCE(current_1_h5_ad_task_times,0) current_1_h5_ad_task_times --近1天任务中心H5广告完成次数
        ,COALESCE(current_3_h5_ad_task_times,0) current_3_h5_ad_task_times --近3天任务中心H5广告完成次数
        ,COALESCE(current_7_sign_times,0) current_7_sign_times --近7天签到次数
        ,CURRENT_TIMESTAMP()
FROM    (
            SELECT  CAST(uid AS BIGINT) uid
                    ,ad_channel --渠道
                    ,os --设备
                    ,app_name --应用
                    ,app_id --应用渠道
                    ,lang_name --语言
                    ,country_name --国家名称
                    ,country_level --国家梯度
                    ,uid % 10 AS uid_last_digit --用户尾号
                    ,IF(INSTR(TOUPPER(campaign_name),'_MAI') > 0,'IAA用户','其他') is_iaa
                    ,DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') reg_days
                    ,CASE   WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') = 0 THEN 'current_reg'
                            WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 3 THEN 'current_3_reg'
                            WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') >= 3 AND DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 7 THEN 'current_4_7_reg'
                            WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') >= 7 AND DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 15 THEN 'current_8_15_reg'
                            WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') >= 15 AND DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 30 THEN 'current_15_30_reg'
                            WHEN DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') >= 30 AND DATEDIFF(CURRENT_TIMESTAMP(),CAST(TO_DATE(reg_date,'yyyy-mm-dd') AS TIMESTAMP),'dd') < 90 THEN 'current_31_90_reg'
                            ELSE 'current_90_reg'
                    END AS reg_statistics
            FROM    mcstv.dwd_user_info_hf
            WHERE   dt = CURRENT_DATE()
        ) a
LEFT JOIN t_active_detail ac
ON      a.uid = ac.uid
LEFT JOIN t_order_detail o
ON      a.uid = o.uid
LEFT JOIN t_subscription sub
ON      a.uid = sub.uid
LEFT JOIN t_ad_unlock adu
ON      a.uid = adu.uid
LEFT JOIN t_k_coin_unlock kcu
ON      a.uid = kcu.uid
LEFT JOIN t_sign_ad_task tsa
ON      a.uid = tsa.uid
LEFT JOIN t_inspirit_ad_task iat
ON      a.uid = iat.uid
LEFT JOIN t_h5_task h5t
ON      a.uid = h5t.uid
LEFT JOIN t_sign si
ON      a.uid = si.uid
LEFT JOIN t_history h
ON      a.uid = h.uid
LEFT JOIN mcstv.dim_subscription dsub
ON      sub.status = dsub.id
;