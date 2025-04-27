  SELECT
    to_timestamp(created_at) :: date AS p_date,
    campaign_id,
    upper(country_code) as country_code,
    upper(platform) as os,
    count(
      distinct case
        when event = 114 and get_json_object(ext_body, '$.reason')='0' then id
        else null
      end
    ) as auto_landing_view_pv,
    count(
      distinct case
        when event = 114 and get_json_object(ext_body, '$.reason')='0' then ip
        else null
      end
    ) as auto_landing_view_uv,
    count(
      distinct case
        when event = 114 and get_json_object(ext_body, '$.reason')='1' then id
        else null
      end
    ) as manual_landing_view_pv,
    count(
      distinct case
        when event = 114 and get_json_object(ext_body, '$.reason')='1' then ip
        else null
      end
    ) as manual_landing_view_uv,
    count(
      distinct case
        when event = 115 and get_json_object(ext_body, '$.reason')='0' then id
        else null
      end
    ) as auto_landing_click_pv,
    count(
      distinct case
        when event = 115 and get_json_object(ext_body, '$.reason')='0' then ip
        else null
      end
    ) as auto_landing_click_uv,
    count(
      distinct case
        when event = 115 and get_json_object(ext_body, '$.reason')='1' then id
        else null
      end
    ) as manual_landing_click_pv,
    count(
      distinct case
        when event = 115 and get_json_object(ext_body, '$.reason')='1' then ip
        else null
      end
    ) as manual_landing_click_uv,
    sum(
      case
        when event = 133 then enter_time
        else null
      end
    ) as landing_data_time,
    count(
      distinct case
        when event = 133 then id
        else null
      end
    ) as landing_data_time_pv,
    sum(
      case
        when event = 133
        and enter_time < 30000 then enter_time
        else null
      end
    ) as landing_data_time_30s,
    count(
      distinct case
        when event = 133
        and enter_time < 30000 then id
        else null
      end
    ) as landing_data_time_30s_pv,
    sum(
      case
        when event = 127 then enter_time
        else null
      end
    ) as landing_stand_time,
    count(
      distinct case
        when event = 127 then id
        else null
      end
    ) as landing_stand_time_pv,
    sum(
      case
        when event = 127
        and enter_time < 30000 then enter_time
        else null
      end
    ) as landing_stand_time_30s,
    count(
      distinct case
        when event = 127
        and enter_time < 30000 then id
        else null
      end
    ) as landing_stand_time_30s_pv
  from
    (
      select
        created_at,
        campaign_id,
        country_code,
        platform,
        event,
        ext_body,
        enter_time,
        id,
        ip,
        case
          when campaign_id = '0'
          or campaign_id = '' then '自然'
          when ad_channel = 'tt' then 'tt'
          when ad_channel in ('fb', 'metaweb') then 'fb'
          when ad_channel = 'apple' then 'apple'
          when ad_channel = 'moloco_int' then 'moloco_int'
          when substr(ad_channel, 1, 12) = 'social_media' then '社媒'
          else '未知'
        end as ad_channel
      FROM
        "web_user_track_log"
      WHERE
        event in (114, 115, 127, 133)
        and to_timestamp(created_at) :: date between '2025-04-10'
        and CURRENT_DATE
    ) as landing_info
  group by
    to_timestamp(created_at) :: date,
    campaign_id,
    upper(country_code),
    upper(platform)

  -----------------------------------------------------
  -- 114 ext_body
  -----------------------------------------------------
  select
      count(*)
  FROM "web_user_track_log"
  where event = 114
        and to_timestamp(created_at) :: date between '2025-04-19'
        and CURRENT_DATE
        and get_json_object(ext_body, '$.reason') = ''
        and get_json_object(ext_body, '$.is_optimize') = '1'
        and get_json_object(ext_body, '$.go_page_url') not like 'https://san-api.stardust-tv.com%'
  limit 1000

  select
      ext_body,to_timestamp(created_at) :: date
  from "web_user_track_log"
  where page_url='https://tt.stardust-tv.com/app/22849.html?p1=SH_TikTok_W2A-IOS_%E7%BF%BB%E8%AF%91_5096_%E5%A5%BD%E5%AD%95%E7%94%9C%E5%A6%BB-%E9%98%BF%E6%8B%89%E4%BC%AF%E8%AF%AD_VO_G1_WJB_0404%2FEE03&p2=1828403874202706&p3=0404_2&p4=1828403871241249&p5=0331_%E5%A5%BD%E5%AD%95%E7%94%9C%E5%A6%BB%E8%A2%AB%E9%92%BB%E7%9F%B3%E8%80%81%E5%85%AC%E5%AE%A0%E4%B8%8A%E5%A4%A9_2-11_5096_%E9%98%BF%E6%8B%89%E4%BC%AF%E8%AF%AD_7_%E7%BF%BB%E8%AF%91_%E4%BA%8C%E5%88%9B_%E9%95%BF_Z2_WZT_AI%E4%BA%A7%E5%87%BA&p6=1828403894603825&p7=TikTok&deep_link_value=shanhai%3A%2F%2Fpush%3Ftype%3D1%26playletId%3D5096%26fragmentId%3D331503&af_label=tiktok_WJB_56420,41e66915fa5885d523a4f71a34a6a253&channel=tiktokweb&af_force_deeplink=true&af_sub5=E_C_P_CrgBZUSQS0bcjBv2dc4LdB_O2W7avUzUiJbODQz4QAFuSdDPF1d3wNiR0ZJy8v0scYRp-iet1ojFtcNae0cCRJaVdnqNSAxs4r72OlcTNYzokLVUT5ucjfj12EErtKaFoUBVGM1sDqUpiGKGfzL6UCUG3J3iBT-5m5CumCwU_3EXIJRqpSleahR_kZRYGgngkzcusiRWj7HSqup4te-R_zWubi1esiboa5SXEEtGhzKuvHo5hOd1VTzh4xIEdjIuMA&ttclid=E_C_P_CrgBZUSQS0bcjBv2dc4LdB_O2W7avUzUiJbODQz4QAFuSdDPF1d3wNiR0ZJy8v0scYRp-iet1ojFtcNae0cCRJaVdnqNSAxs4r72OlcTNYzokLVUT5ucjfj12EErtKaFoUBVGM1sDqUpiGKGfzL6UCUG3J3iBT-5m5CumCwU_3EXIJRqpSleahR_kZRYGgngkzcusiRWj7HSqup4te-R_zWubi1esiboa5SXEEtGhzKuvHo5hOd1VTzh4xIEdjIuMA&ttclid=E_C_P_Cs0BQB-IGbSHlYz_FYiM7WyghXcKbUEKTBbB-5nDe1rK_JuF82valJOY5o_Hj2Z1X_H2Knnw2ljCov67BbFYRNGzLspMehJ1leYgiOPR36V6ntyvrvzMjI5bloVDyZTWGZ0cIksma3ZSlveiQtJR4oU-KMOXQmiaiO46g8ltfab5nPFs1UjYjC4BXw1CJn10U20XL1qtslj-kDd-eNRsXj8pKpxG931bbPdrD3Vql8qgYVN7rdeBeujJFvoFJjmF4MOmoSPsK4gMqG5tDimCsBIEdjIuMA&ttclid=E_C_P_Cs0BQB-IGbSHlYz_FYiM7WyghXcKbUEKTBbB-5nDe1rK_JuF82valJOY5o_Hj2Z1X_H2Knnw2ljCov67BbFYRNGzLspMehJ1leYgiOPR36V6ntyvrvzMjI5bloVDyZTWGZ0cIksma3ZSlveiQtJR4oU-KMOXQmiaiO46g8ltfab5nPFs1UjYjC4BXw1CJn10U20XL1qtslj-kDd-eNRsXj8pKpxG931bbPdrD3Vql8qgYVN7rdeBeujJFvoFJjmF4MOmoSPsK4gMqG5tDimCsBIEdjIuMA'
        and event = 114
        and to_timestamp(created_at) :: date between '2025-04-16'
        and CURRENT_DATE
  order by  created_at desc
  limit 10


  -----------------------------------------------------
  -- 落地页展示次数
  -----------------------------------------------------
set timezone = 'utc-8';
select
    p_date,
    sum(landing_view_pv),
    sum(landing_view_uv)
from (SELECT
    to_timestamp(created_at) :: date AS p_date,
    campaign_id,
    upper(country_code) as country_code,
    upper(platform) as os,
    count(
      distinct case
        when event = 114 then id
        else null
      end
    ) as landing_view_pv,
    count(
      distinct case
        when event = 114 then ip
        else null
      end
    ) as landing_view_uv,
    count(
      distinct case
        when event = 115 then id
        else null
      end
    ) as landing_click_pv,
    count(
      distinct case
        when event = 115 then ip
        else null
      end
    ) as landing_click_uv,
    sum(
      case
        when event = 133 then enter_time
        else null
      end
    ) as landing_data_time,
    count(
      distinct case
        when event = 133 then id
        else null
      end
    ) as landing_data_time_pv,
    sum(
      case
        when event = 133
        and enter_time < 30000 then enter_time
        else null
      end
    ) as landing_data_time_30s,
    count(
      distinct case
        when event = 133
        and enter_time < 30000 then id
        else null
      end
    ) as landing_data_time_30s_pv,
    sum(
      case
        when event = 127 then enter_time
        else null
      end
    ) as landing_stand_time,
    count(
      distinct case
        when event = 127 then id
        else null
      end
    ) as landing_stand_time_pv,
    sum(
      case
        when event = 127
        and enter_time < 30000 then enter_time
        else null
      end
    ) as landing_stand_time_30s,
    count(
      distinct case
        when event = 127
        and enter_time < 30000 then id
        else null
      end
    ) as landing_stand_time_30s_pv
  from
    (
      select
        created_at,
        campaign_id,
        country_code,
        platform,
        event,
        enter_time,
        id,
        ip,
        case
          when campaign_id = '0'
          or campaign_id = '' then '自然'
          when ad_channel = 'tt' then 'tt'
          when ad_channel in ('fb', 'metaweb') then 'fb'
          when ad_channel = 'apple' then 'apple'
          when ad_channel = 'moloco_int' then 'moloco_int'
          when substr(ad_channel, 1, 12) = 'social_media' then '社媒'
          else '未知'
        end as ad_channel
      FROM
        "web_user_track_log"
      WHERE
        event in (114, 115, 127, 133)
        and to_timestamp(created_at) :: date between '2024-12-21'
        and CURRENT_DATE
    ) as landing_info
  group by
    to_timestamp(created_at) :: date,
    campaign_id,
    upper(country_code),
    upper(platform) ) t
     group by p_date


--------------------------------------
-- 测试uid
--------------------------------------
select
    uid
from "web_user_track_log"
where event in (114, 115, 127, 133)
    and to_timestamp(created_at) :: date between '2025-04-14' and current_date
limit 100

select
    *
from "web_user_track_log"
where event in (114,115,127)
    and to_timestamp(created_at) :: date between '2025-04-15' and current_date
    and ip = '36.81.139.207'