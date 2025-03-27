set timezone = 'UTC-0';
with main_tb as (
            select p_date,
                    soure_name,
                    soure_no,
                    campaign_id,
                    campaign_name,
                    country_code,
                    sum(cost_amount) cost_amount
             from (select TO_TIMESTAMP(created_at)::date p_date,
                          case
                              when ad_channel = 'mobiprobebd521' then 'Adeliver'
                              when ad_channel = 'moloco_int' then 'Moloco'
                              when ad_channel = 'apple' then 'Apple Search Ads'
                              when ad_channel = 'mtg' then 'MTG'
                              else '其他' end            soure_name,--渠道名称
                          case
                              when ad_channel = 'mobiprobebd521' then 'mobiprobe'
                              when ad_channel = 'moloco_int' then 'moloco'
                              else '其他' end            soure_no,--渠道号
                          a.campaign_id, --广告系列ID
                          campaign_name, --广告系列名称
                          upper(a.area)                  country_code,--国家
                          cost_amount --消耗金额
                   from public.ad_cost_data_log a
                   -- left join v_dim_country_area c on upper(a.area) = c.country_code
                   where TO_TIMESTAMP(created_at)::date >= current_date - interval '32 day'
                     and TO_TIMESTAMP(created_at)::date < current_date
                     and ad_channel in (
                                        'mobiprobebd521',
                                        'moloco_int',
                                        'apple',
                                        'mtg'
                       )) t
             group by p_date, soure_name, soure_no, campaign_id, campaign_name, country_code),


     active_tb AS (select campaign_id,
                          uid,
                          case when d_date - reg_date = 1 then uid end uid_1day,
                          reg_date,
                          d_date,
                          reg_country
                   from public.dwd_user_active
                   where reg_date >= (CURRENT_DATE - interval '32 day')::DATE
                     and reg_date < CURRENT_DATE
                   group by campaign_id, d_date,
                            uid,
                            reg_date, reg_country)
        ,
     af_tab as (select to_timestamp(created_at)::date p_date, count(1) user_num, campaign_id, country_code
                from app_install_event_log
                where to_timestamp(created_at)::date >= current_date - interval '32 day'
                  and to_timestamp(created_at)::date < current_date
                group by campaign_id, country_code, to_timestamp(created_at)::date),


     pay_tb as
         (select p_date::date,
                 --  campaign_id,
                 uid,
                 sum(money) as money
          from (select to_timestamp(created_at)::date as p_date, campaign_id, uid, 1.0 * ("money") / 100 as "money"
                from all_order_log
                where status = 1
                  and environment = 1
                union all
                select to_timestamp(created_at)::date as p_date, campaign_id, uid, -1.0 * ("money") / 100 as "money"
                from all_refund_order_log
                where status = 1
                  and environment = 1) tmp
          group by p_date::date,
                   --    campaign_id,
                   uid),
     roi_tb as
         (select reg_date,
                 campaign_id,
                 reg_country,
                 sum(case when statistics = '0' then pay_uv else 0 end)   as u_num_0,
                 sum(case when statistics in ('0','7') then pay_uv else 0 end)   as u_num_7,
                 sum(case when statistics in ('15','7','0') then pay_uv else 0 end)  as u_num_15,
                 sum(case when statistics in ('30','15','7','0') then pay_uv else 0 end)  as u_num_30,
                 sum(case when statistics = '0' then pay_amt else 0 end)  as money_0,
                 sum(case when statistics in ('0','7') then pay_amt else 0 end)  as money_7,
                 sum(case when statistics in ('15','7','0')  then pay_amt else 0 end) as money_15,
                 sum(case when statistics in ('30','15','7','0') then pay_amt else 0 end) as money_30
          from (select reg_date,
                       campaign_id,
                       reg_country,
                       statistics,
                       count(distinct uid)     pay_uv,
                       sum(coalesce(money, 0)) pay_amt
                from (select
                                -- CASE
                                --  WHEN DATE_PART('day', a.p_date::timestamp - reg_date) >= 7 THEN '7'
                                --  WHEN DATE_PART('day', a.p_date ::timestamp - reg_date) >= 15 THEN '15'
                                --  WHEN DATE_PART('day', a.p_date ::timestamp - reg_date) >= 30 THEN '30'
                                --  WHEN DATE_PART('day', a.p_date ::timestamp - reg_date) = 0 THEN '0'
                                --  ELSE
                                --      'all'
                                --  END as statistics,
                              CASE
                                 WHEN DATE_PART('day', a.p_date::timestamp - reg_date)>=0 and DATE_PART('day', a.p_date::timestamp - reg_date) <= 7 THEN '7'
                                 WHEN DATE_PART('day', a.p_date::timestamp - reg_date)>=0 and DATE_PART('day', a.p_date ::timestamp - reg_date) <= 15 THEN '15'
                                 WHEN DATE_PART('day', a.p_date::timestamp - reg_date)>=0 and DATE_PART('day', a.p_date ::timestamp - reg_date) <=30 THEN '30'
                                 WHEN DATE_PART('day', a.p_date ::timestamp - reg_date) = 0 THEN '0'
                                 ELSE
                                     'all'
                                 END as statistics,
                             a.uid,
                             money,
                             b.campaign_id,
                             reg_date,
                             reg_country
                      from (
                      select campaign_id, reg_date, reg_country,uid from active_tb group by campaign_id,uid, reg_date, reg_country) b
                               left join pay_tb a on a.uid = b.uid
                      --   and a.campaign_id = b.campaign_id
                     ) t
                group by reg_date, campaign_id, statistics, reg_country ) f
          group by reg_date, campaign_id, reg_country)

select a.p_date,
       a.campaign_id,
       a.campaign_name,
       a.soure_name,
       a.soure_no,

       country_name,
       a.cost_amount,

       coalesce(b.new_user,0) new_user,
       coalesce(b.keep_user_num_1day,0) keep_user_num_1day,
       coalesce(f.user_num,0)            af_user_num,
       coalesce(u_num_0, 0)  u_num_0,
       coalesce(u_num_7, 0)  u_num_7,
       coalesce(u_num_15, 0) u_num_15,
       coalesce(u_num_30, 0) u_num_30,
       coalesce(money_0, 0)  money_0,
       coalesce(money_7, 0)  money_7,
       coalesce(money_15, 0) money_15,
       coalesce(money_30, 0) money_30,
       current_timestamp
from main_tb a
         left join (select campaign_id,
                           count(distinct uid)      new_user,
                           count(distinct uid_1day) keep_user_num_1day,
                           reg_date,
                           reg_country
                    from active_tb
                    group by campaign_id, reg_date, reg_country) b
                   on a.campaign_id = b.campaign_id and a.p_date = b.reg_date and a.country_code = b.reg_country
         left join af_tab f on a.campaign_id = f.campaign_id and a.p_date = f.p_date and a.country_code = f.country_code
         left join roi_tb r
                   on a.campaign_id = r.campaign_id and a.p_date = r.reg_date and a.country_code = r.reg_country
         left join v_dim_country_area c on  a.country_code = c.country_code