------------------------------------------
-- file: 老板群飞书推送-分语言.sql
-- author: xiaoj
-- time: 2025/5/8 11:49
-- description:
------------------------------------------

------------------------------------------
--  分析
--  需要表 dw_operate_view 经营概览
------------------------------------------


with t1 as (
  -- 粒度： 日期和语言
  -- 功能： 再计算部分字段
  select
    a.日期,
    语言,
    pay_amt 总充值金额,
    sum(pay_amt) over(
      partition by 语言,
      to_char(
        a.日期,
        'YYYY-MM'
      )
      order by
        a.日期
    ) 当月累计收入,
    new_pay_amt 新用户充值金额,
    old_pay_amt 老用户充值金额,
    ad_cost 消耗,
    dau,
    sum(ad_cost) over(
      partition by 语言,
      to_char(
        a.日期,
        'YYYY-MM'
      )
      order by
        a.日期
    ) 当月累计消耗,
    sum(pay_refund_amt) over(
      partition by 语言,
      to_char(
        a.日期,
        'YYYY-MM'
      )
      order by
        a.日期
    ) 当月累计退款,
    case
      when ad_cost = 0 then 0
      else round(new_pay_amt * 1.0 / ad_cost, 2)
    end 新用户ROI,
    case
      when ad_cost = 0 then 0
      else round((pay_amt-pay_refund_amt) * 1.0 / ad_cost, 2)
    end 整体ROI
  from
    (
      -- 粒度：日期和合并的语言
      select
        d_date :: date as 日期,
        -- 合并部分语言
        case
          when lang_name in (
            '日语',
            '韩语'
          ) then '日韩'
          when lang_name in (
            '法语',
            '德语'
          ) then '法德'
          when lang_name in (
            '西班牙语',
            '葡萄牙语'
          ) then '西葡'
          when lang_name in (
            '泰语',
            '印度尼西亚语',
            '简体中文',
            '繁体中文'
          ) then '泰印中繁'
          else lang_name
        end as 语言,
        sum(pay_amt) as pay_amt,
        sum(new_pay_amt) as new_pay_amt,
        sum(old_pay_amt) as old_pay_amt,
        sum(ad_cost) as ad_cost,
        sum(pay_refund_amt) as pay_refund_amt,
        sum(dau) as dau
      from
        public.dw_operate_view a        -- 经营概览
      where
        1 = 1
      group by
        d_date :: date,
        case
          when lang_name in (
            '日语',
            '韩语'
          ) then '日韩'
          when lang_name in (
            '法语',
            '德语'
          ) then '法德'
          when lang_name in (
            '西班牙语',
            '葡萄牙语'
          ) then '西葡'
          when lang_name in (
            '泰语',
            '印度尼西亚语',
            '简体中文',
            '繁体中文'
          ) then '泰印中繁'
          else lang_name
        end
    ) a
),
-- 粒度： 日期和部分和并的语言
-- 求昨天的留存率
t2 as (
  select
    d_date :: date + 1 日期,
    case
      when lang_name in (
        '日语',
        '韩语'
      ) then '日韩'
      when lang_name in (
        '法语',
        '德语'
      ) then '法德'
      when lang_name in (
        '西班牙语',
        '葡萄牙语'
      ) then '西葡'
      when lang_name in (
        '泰语',
        '印度尼西亚语',
        '简体中文',
        '繁体中文'
      ) then '泰印中繁'
      else lang_name
    end as 语言,
    case
      when sum(dau) = 0 then 0
      else round(sum(dau_2login) * 100.0 / sum(dau), 2)
    end as 昨天留存率
  from
    public.dw_operate_view a
  where
    1 = 1
    and d_date :: date = current_date - 2
  group by
    d_date :: date + 1,
    case
      when lang_name in (
        '日语',
        '韩语'
      ) then '日韩'
      when lang_name in (
        '法语',
        '德语'
      ) then '法德'
      when lang_name in (
        '西班牙语',
        '葡萄牙语'
      ) then '西葡'
      when lang_name in (
        '泰语',
        '印度尼西亚语',
        '简体中文',
        '繁体中文'
      ) then '泰印中繁'
      else lang_name
    end
)
select
  t1.日期,
  t1.语言,
  总充值金额 as "昨天总充值金额($)",
  新用户充值金额 as "昨天新用户充值金额($)",
  老用户充值金额 as "昨天老用户充值金额($)",
  消耗 as "昨天消耗($)",
  新用户ROI as 昨天新用户ROI,
  整体ROI as 昨天整体ROI,
  dau as "昨天DAU",
  concat(
    cast(
      昨天留存率 as varchar
    ),
    '%'
  ) 昨天留存率,
  当月累计收入 as "当月累计收入($)",
  当月累计消耗 as "当月累计消耗($)",
  case
    when 当月累计消耗 = 0 then 0
    else round(
      (
        当月累计收入 - 当月累计退款
      ) * 1.0 / 当月累计消耗,
      2
    )
  end 当月整体ROI
from
  t1
  inner join t2 on t1.日期 = t2.日期
  and t1.语言 = t2.语言
where
  t1.语言 <> 'UNKNOWN'
order by
  case
    when t1.语言 = '英语' then 1
    when t1.语言 = '日韩' then 2
    when t1.语言 = '法德' then 3
    when t1.语言 = '西葡' then 4
    when t1.语言 = '泰印中繁' then 5
     when t1.语言 = '阿拉伯语' then 6
      when t1.语言 = '越南语' then 7
       when t1.语言 = '土耳其语' then 8
    else 99
  end