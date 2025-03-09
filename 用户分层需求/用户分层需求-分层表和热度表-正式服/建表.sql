-----------------------------------------------
-- 短剧热度表
-----------------------------------------------
DROP TABLE if exists dw.dw_video_hot;
CREATE TABLE if not exists dw.dw_video_hot (
    vid integer NOT NULL,
    app_source_id text,
    play_pv bigint,
    pay_pv bigint,
    search_pv bigint,
    hot bigint,
    etl_time timestamp with time zone
    ,PRIMARY KEY (vid,app_source_id)
);
-----------------------------------------------
-- 用户分层表
-----------------------------------------------
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
-----------------------------------------------
-- 用户分层表 中间表
-----------------------------------------------
drop table if exists tmp.dw_user_layer_tag_tmp01;
drop table if exists tmp.dw_user_layer_tag_tmp02;
CREATE TABLE if not exists tmp.dw_user_layer_tag_tmp01 (
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
COMMENT ON TABLE tmp.dw_user_layer_tag_tmp01 IS NULL;
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.id IS '主键';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.device_id IS '设备id';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.os IS '设备类型';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.first_date IS '新增日期';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.guiyin_date IS '归因日期';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.ad_channel_id IS '渠道id';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.ad_channel IS '渠道';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.unlock_eid_cnt IS '解锁剧集数';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.watch_eid_cnt IS '观看剧集数';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.act_dnt IS '活跃天数';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.silent_dnt IS '静默天数';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.product_id IS '充值产品id';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.product_name IS '充值产品';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.product_category IS '充值类型';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.product_pay_order IS '充值次数';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.product_pay_amt IS '充值金额（分）';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.subscription_status IS '用户维度-会员状态';
COMMENT ON COLUMN tmp.dw_user_layer_tag_tmp01.app_source_id IS 'app来源';