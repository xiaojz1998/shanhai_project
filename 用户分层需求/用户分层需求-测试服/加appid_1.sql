-----------------------------------------------------------
-- 原始app_id 作废，区分不同app的埋点数据
-----------------------------------------------------------
--------------------------------------------------------------------
-- 建一个新的总埋点表
--------------------------------------------------------------------
drop table if exists public.total_user_track_log;
create table if not exists public.total_user_track_log
(
    id               text not null primary key,
    model            text,
    order_type       text,
    uid              text,
    device_id        text,
    event_name       text,
    sku_id           text,
    system           text,
    order_id         text,
    os               text,
    ad_entrance      text,
    vip_entrance     text,
    network          text,
    percent          text,
    vid              bigint,
    balance          bigint,
    event_id         bigint,
    k_coin           bigint,
    time             bigint,
    aid              text,
    brand            text,
    unlock_type      text,
    vid_is_pay       bigint,
    eid              bigint,
    money            bigint,
    is_vip           bigint,
    eid_is_pay       bigint,
    ext_body         text,
    app_name         text,
    created_at       bigint,
    entrance         text,
    oaid             text,
    payment          text,
    sort             bigint,
    ad_type          bigint,
    app_id           text,
    created_date     date,
    ad_id            text,
    account_id       text,
    ad_channel       text,
    fail_code        text,
    fail_msg         text,
    link_id          text,
    app_version      text,
    vip_expired_time text,
    rank             bigint,
    play_speed       text,
    click_content    text,
    task_id          bigint,
    task_type        bigint,
    skip_to          text,
    signin_days      bigint,
    action           text,
    source           text,
    num              bigint,
    is_auto_unlock   bigint,
    category_name    text,
    tab_name         text,
    type             text,
    page_name        text,
    popup_name       text,
    app_source_id       text,
    app_source_name     text
);
--------------------------------------------------------------------
-- 注入total_user_track_log
--------------------------------------------------------------------
truncate table public.total_user_track_log;
INSERT INTO public.total_user_track_log
with tmp_total_user_track_log as (
    select
    md5(concat(id,'duanju'))  as id               ,
    model            ,
    order_type       ,
    uid              ,
    device_id        ,
    event_name       ,
    sku_id           ,
    system           ,
    order_id         ,
    os               ,
    ad_entrance      ,
    vip_entrance     ,
    network          ,
    percent          ,
    vid              ,
    balance          ,
    event_id         ,
    k_coin           ,
    time             ,
    aid              ,
    brand            ,
    unlock_type      ,
    vid_is_pay       ,
    eid              ,
    money            ,
    is_vip           ,
    eid_is_pay       ,
    ext_body         ,
    app_name         ,
    created_at       ,
    entrance         ,
    oaid             ,
    payment          ,
    sort             ,
    ad_type          ,
    app_id           ,
    created_date     ,
    ad_id            ,
    account_id       ,
    ad_channel       ,
    fail_code        ,
    fail_msg         ,
    link_id          ,
    app_version      ,
    vip_expired_time ,
    rank             ,
    play_speed       ,
    click_content    ,
    task_id          ,
    task_type        ,
    skip_to          ,
    signin_days      ,
    action           ,
    source           ,
    num              ,
    is_auto_unlock   ,
    category_name    ,
    tab_name         ,
    type             ,
    page_name        ,
    popup_name       ,
    '1' as app_source_id ,
    'duanju' as app_source_name
from public.user_track_log
union all
select
    md5(concat(id,'xingju'))  as id             ,
    model            ,
    order_type       ,
    uid              ,
    device_id        ,
    event_name       ,
    sku_id           ,
    system           ,
    order_id         ,
    os               ,
    ad_entrance      ,
    vip_entrance     ,
    network          ,
    percent          ,
    vid              ,
    balance          ,
    event_id         ,
    k_coin           ,
    time             ,
    aid              ,
    brand            ,
    unlock_type      ,
    vid_is_pay       ,
    eid              ,
    money            ,
    is_vip           ,
    eid_is_pay       ,
    ext_body         ,
    app_name         ,
    created_at       ,
    entrance         ,
    oaid             ,
    payment          ,
    sort             ,
    ad_type          ,
    app_id           ,
    created_date     ,
    ad_id            ,
    account_id       ,
    ad_channel       ,
    fail_code        ,
    fail_msg         ,
    link_id          ,
    app_version      ,
    vip_expired_time ,
    rank             ,
    play_speed       ,
    click_content    ,
    task_id          ,
    task_type        ,
    skip_to          ,
    signin_days      ,
    action           ,
    source           ,
    num              ,
    is_auto_unlock   ,
    category_name    ,
    tab_name         ,
    type             ,
    page_name        ,
    popup_name       ,
    '2' as app_source_id,
    'xingju' as app_source_name
from public.xingju_user_track_log
)
select
    *
from tmp_total_user_track_log;
--------------------------------------------------------------------
-- 注入 dw_video_hot
--------------------------------------------------------------------
truncate table dw.dw_video_hot;
INSERT INTO dw.dw_video_hot (vid,app_source_id,play_pv, pay_pv, search_pv, hot, etl_time)
WITH play_stats AS (
    SELECT
        vid,
        app_source_id,
        SUM(pv) AS pv_sum
    FROM
    (
        SELECT
            vid,
            device_id,
            app_source_id,
            COUNT(DISTINCT created_at) AS pv
        FROM total_user_track_log
        WHERE event_name = 'enter_player'
          AND created_date >= '2024-12-17'
          AND vid > 0
        GROUP BY vid, device_id,app_source_id
    ) t
    GROUP BY vid,app_source_id
),
recharge_stats AS (
    SELECT
        vid,
        app_source_id,
        COUNT(DISTINCT order_id) AS pv
    FROM total_user_track_log
    WHERE event_name = 'recharge_success'
      AND created_date >= '2024-12-17'
      AND vid > 0
    GROUP BY vid,app_source_id
),
search_stats AS (
    SELECT
        vid,
        app_source_id,
        SUM(pv) AS pv_sum
    FROM
    (
        SELECT
            vid,
            device_id,
            app_source_id,
            COUNT(DISTINCT created_at) AS pv
        FROM total_user_track_log
        WHERE event_name = 'show_cover'
          AND created_date >= '2024-12-17'
          AND entrance = 'search_result'
        GROUP BY vid, device_id,app_source_id
    ) t
    GROUP BY vid,app_source_id
),
all_vids AS (
    SELECT DISTINCT vid,app_source_id FROM total_user_track_log WHERE vid > 0
   and created_date >= '2024-12-17'
)
SELECT
    a.vid,
    a.app_source_id,
    COALESCE(p.pv_sum, 0) AS play_pv,
    COALESCE(r.pv, 0) AS pay_pv,
    COALESCE(s.pv_sum, 0) AS search_pv,
    ((COALESCE(p.pv_sum, 0) * 0.5 + COALESCE(r.pv, 0) * 0.35 + COALESCE(s.pv_sum, 0) * 0.15) * 100) AS hot,
    NOW() AS etl_time
FROM
    all_vids a
LEFT JOIN play_stats p ON a.vid = p.vid and a.app_source_id = p.app_source_id
LEFT JOIN recharge_stats r ON a.vid = r.vid and a.app_source_id = r.app_source_id
LEFT JOIN search_stats s ON a.vid = s.vid and a.app_source_id = s.app_source_id
order by  hot desc;

--------------------------------------------------------------------
-- 新建用户分层表
--------------------------------------------------------------------
drop table if exists dw.dw_user_layer_tag;
CREATE TABLE if not exists dw.dw_user_layer_tag (
    id text NOT NULL,
    device_id text,
    os text,
    first_date date,
    guiyin_date date,
    ad_channel_id text,
    ad_channel text,
    unlock_eid_cnt bigint,
    watch_eid_cnt bigint,
    act_dnt bigint,
    silent_dnt bigint,
    product_id text,
    product_name text,
    product_category text,
    product_pay_order bigint,
    product_pay_amt bigint,
    etl_time timestamp with time zone,
    subscription_status integer,
    app_source_id text
    ,PRIMARY KEY (id)
);
COMMENT ON TABLE dw.dw_user_layer_tag IS NULL;
COMMENT ON COLUMN dw.dw_user_layer_tag.id IS '主键';
COMMENT ON COLUMN dw.dw_user_layer_tag.device_id IS '设备id';
COMMENT ON COLUMN dw.dw_user_layer_tag.os IS '设备类型';
COMMENT ON COLUMN dw.dw_user_layer_tag.first_date IS '新增日期';
COMMENT ON COLUMN dw.dw_user_layer_tag.guiyin_date IS '归因日期';
COMMENT ON COLUMN dw.dw_user_layer_tag.ad_channel_id IS '渠道id';
COMMENT ON COLUMN dw.dw_user_layer_tag.ad_channel IS '渠道';
COMMENT ON COLUMN dw.dw_user_layer_tag.unlock_eid_cnt IS '解锁剧集数';
COMMENT ON COLUMN dw.dw_user_layer_tag.watch_eid_cnt IS '观看剧集数';
COMMENT ON COLUMN dw.dw_user_layer_tag.act_dnt IS '活跃天数';
COMMENT ON COLUMN dw.dw_user_layer_tag.silent_dnt IS '静默天数';
COMMENT ON COLUMN dw.dw_user_layer_tag.product_id IS '充值产品id';
COMMENT ON COLUMN dw.dw_user_layer_tag.product_name IS '充值产品';
COMMENT ON COLUMN dw.dw_user_layer_tag.product_category IS '充值类型';
COMMENT ON COLUMN dw.dw_user_layer_tag.product_pay_order IS '充值次数';
COMMENT ON COLUMN dw.dw_user_layer_tag.product_pay_amt IS '充值金额（分）';
COMMENT ON COLUMN dw.dw_user_layer_tag.subscription_status IS '用户维度-会员状态';
COMMENT ON COLUMN dw.dw_user_layer_tag.app_source_id IS 'app来源';
