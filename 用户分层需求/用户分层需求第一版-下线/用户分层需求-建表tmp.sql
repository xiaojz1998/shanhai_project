drop table if exists tmp.dw_user_layer_tag_tmp02;
create table if not exists tmp.dw_user_layer_tag_tmp02
(
    id                text not null primary key,
    device_id         text,
    os                text,
    first_date        date,
    guiyin_date       date,
    ad_channel_id     text,
    ad_channel        text,
    unlock_eid_cnt    bigint,
    watch_eid_cnt     bigint,
    act_dnt           bigint,
    silent_dnt        bigint,
    product_id        text,
    product_name      text,
    product_category  text,
    product_pay_order bigint,
    product_pay_amt   bigint,
    etl_time          timestamp with time zone,
    subscription_status int
);


