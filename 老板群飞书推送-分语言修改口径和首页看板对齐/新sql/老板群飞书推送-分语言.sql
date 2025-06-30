---------------------------------------------
-- File: 老板群飞书推送-分语言.sql
-- Time: 2025/6/16 14:55
-- User: xiaoj
-- Description:  
---------------------------------------------
with t as (
    -- 粒度：日期和合并的语言
    select
        t.日期,
        t.语言,
        pay_amt,
        new_pay_amt,
        old_pay_amt,
        ad_cost,
        pay_refund_amt,
        dau
    from (
        select
              d_date :: date as 日期,
              -- 合并部分语言
              case
                when lang_name in ('日语','韩语') then '日韩'
                when lang_name in ('法语','德语') then '法德'
                when lang_name in ('西班牙语','葡萄牙语') then '西葡'
                when lang_name in ('泰语','印度尼西亚语','简体中文','繁体中文') then '泰印中繁'
                else lang_name
              end as 语言,
              sum(pay_amt) as pay_amt,
              sum(new_pay_amt) as new_pay_amt,
              sum(old_pay_amt) as old_pay_amt,
              sum(ad_cost) as ad_cost,
              sum(pay_refund_amt) as pay_refund_amt --,
              -- sum(dau) as dau
        from public.dw_operate_view a
        group by
              d_date :: date,
              case
                when lang_name in ('日语','韩语') then '日韩'
                when lang_name in ('法语','德语') then '法德'
                when lang_name in ('西班牙语','葡萄牙语') then '西葡'
                when lang_name in ('泰语','印度尼西亚语','简体中文','繁体中文') then '泰印中繁'
                else lang_name
              end
    ) t
    left join (
        select
            d_date::date as 日期,
             -- 合并部分语言
            case
              when lang_name in ('日语','韩语') then '日韩'
              when lang_name in ('法语','德语') then '法德'
              when lang_name in ('西班牙语','葡萄牙语') then '西葡'
              when lang_name in ('泰语','印度尼西亚语','简体中文','繁体中文') then '泰印中繁'
              else lang_name
            end as 语言,
            sum(active_uv) as dau
        from public.ads_rpt_home_page_hi
        group by
            d_date::date,
            case
              when lang_name in ('日语','韩语') then '日韩'
              when lang_name in ('法语','德语') then '法德'
              when lang_name in ('西班牙语','葡萄牙语') then '西葡'
              when lang_name in ('泰语','印度尼西亚语','简体中文','繁体中文') then '泰印中繁'
              else lang_name
            end
    ) t0 on t.日期 = t0.日期 and t.语言 = t0.语言
),
t1 as (
    -- 粒度： 日期和语言
    -- 功能： 根据 t 再计算部分字段
    select
      t.日期,
      语言,
      pay_amt 总充值金额,
      sum(pay_amt) over(partition by 语言,to_char(t.日期,'YYYY-MM') order by t.日期) 当月累计收入,
      new_pay_amt 新用户充值金额,
      old_pay_amt 老用户充值金额,
      ad_cost 消耗,
      dau,
      sum(ad_cost) over(partition by 语言,to_char(t.日期,'YYYY-MM')order by t.日期) 当月累计消耗,
      sum(pay_refund_amt) over(partition by 语言,to_char(t.日期,'YYYY-MM')order by t.日期) 当月累计退款,
      case when ad_cost = 0 then 0 else round(new_pay_amt * 1.0 / ad_cost, 2) end 新用户ROI,
      case when ad_cost = 0 then 0 else round((pay_amt-pay_refund_amt) * 1.0 / ad_cost, 2) end 整体ROI
    from t
),
t2 as (
    -- 求昨日留存率
    -- select
    --     d_date :: date + 1 日期,
    --     case
    --       when lang_name in ('日语','韩语') then '日韩'
    --       when lang_name in ('法语','德语') then '法德'
    --       when lang_name in ('西班牙语','葡萄牙语') then '西葡'
    --       when lang_name in ('泰语','印度尼西亚语','简体中文','繁体中文') then '泰印中繁'
    --       else lang_name
    --     end as 语言,
    --     case when sum(dau) = 0 then 0 else round(sum(dau_2login) * 100.0 / sum(dau), 2) end as 昨天留存率
    --   from
    --     public.dw_operate_view a
    --   where 1 = 1
    --     and d_date :: date = current_date - 2
    --   group by
    --     d_date :: date + 1,
    --     case
    --       when lang_name in ('日语','韩语') then '日韩'
    --       when lang_name in ('法语','德语') then '法德'
    --       when lang_name in ('西班牙语','葡萄牙语') then '西葡'
    --       when lang_name in ('泰语','印度尼西亚语','简体中文','繁体中文') then '泰印中繁'
    --       else lang_name
    --     end
    select
        d_date :: date - 1 日期,
        case
          when lang_name in ('日语','韩语') then '日韩'
          when lang_name in ('法语','德语') then '法德'
          when lang_name in ('西班牙语','葡萄牙语') then '西葡'
          when lang_name in ('泰语','印度尼西亚语','简体中文','繁体中文') then '泰印中繁'
          else lang_name
        end as 语言,
        case when sum(byst_uv)=0 then 0 else sum(yst_uv)*1.0/sum(byst_uv) end as 昨天留存率
    from public.ads_rpt_home_page_remain_hi
    where d_date::date = current_date
    group by d_date :: date - 1,
        case
          when lang_name in ('日语','韩语') then '日韩'
          when lang_name in ('法语','德语') then '法德'
          when lang_name in ('西班牙语','葡萄牙语') then '西葡'
          when lang_name in ('泰语','印度尼西亚语','简体中文','繁体中文') then '泰印中繁'
          else lang_name
        end
),
tmp_af_daily as (
    select
        d_date,
        case
           when lang_name in ('日语','韩语') then '日韩'
           when lang_name in ('法语','德语') then '法德'
           when lang_name in ('西班牙语','葡萄牙语') then '西葡'
           when lang_name in ('泰语','印尼语','中文简体','繁体中文') then '泰印中繁'
           else lang_name
         end as lang_name,
        round(sum(pay_amt),2) as total_pay_amt,
        round(sum(case when ascribe_channel='自归因' or(conversion_type in ('install','unknown') and campaign_type in ('ua','organic','unknown'))  then af_d0 else 0 end),2) as af_d0
    from public.ads_operate_roi_ascribe
    group by d_date,
        case
           when lang_name in ('日语','韩语') then '日韩'
           when lang_name in ('法语','德语') then '法德'
           when lang_name in ('西班牙语','葡萄牙语') then '西葡'
           when lang_name in ('泰语','印尼语','中文简体','繁体中文') then '泰印中繁'
           else lang_name
         end
) ,
tmp_af_daily_with_month_accum_payamt as (
    select
        d_date,
        lang_name,
        total_pay_amt,
        af_d0,
        sum(total_pay_amt) over (partition by lang_name,to_char(d_date,'YYYY-MM') order by d_date) as month_accum_payamt -- 当月累计支付金额
    from tmp_af_daily
)
select
  t2.日期,
  t2.语言,
  总充值金额 as "昨天总充值金额",
  新用户充值金额 as "昨天新用户充值金额",
  老用户充值金额 as "昨天老用户充值金额",
  消耗 as "昨天消耗",
  新用户ROI as 昨天新用户ROI,
  整体ROI as 昨天整体ROI,
  case when 消耗 = 0 then 0 else round(1.0* af_d0 / 消耗,2) end "昨日新用户ROI(染色后)",
  case when 消耗 = 0 then 0 else round(1.0* total_pay_amt / 消耗,2) end "昨日整体ROI(染色后)",
  dau as "昨天DAU",
  concat(cast(round(昨天留存率*100,2) as varchar),'%') 昨天留存率,
  当月累计收入 as "当月累计收入",
  当月累计消耗 as "当月累计消耗",
  case when 当月累计消耗 = 0 then 0 else round((当月累计收入 - 当月累计退款) * 1.0 / 当月累计消耗,2) end 当月整体ROI,
  case when 当月累计消耗 = 0 then 0 else round(month_accum_payamt * 1.0/当月累计消耗,2) end "当月整体ROI(染色后)"
from t2
    inner join t1 on t1.日期 = t2.日期 and t1.语言 = t2.语言    --只求昨天的指标
    inner join tmp_af_daily_with_month_accum_payamt t3 on t2.日期 = t3.d_date and t2.语言 = t3.lang_name
where
  t2.语言 <> 'UNKNOWN'
order by
  case
    when t2.语言 = '英语' then 1
    when t2.语言 = '日韩' then 2
    when t2.语言 = '法德' then 3
    when t2.语言 = '西葡' then 4
    when t2.语言 = '泰印中繁' then 5
    when t2.语言 = '阿拉伯语' then 6
    when t2.语言 = '越南语' then 7
    when t2.语言 = '土耳其语' then 8
    else 99
  end