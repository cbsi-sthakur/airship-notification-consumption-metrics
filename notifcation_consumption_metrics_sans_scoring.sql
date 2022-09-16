  ----- WITH CIDNE no geo correct video plays


  WITH hits AS (
    SELECT
      hit_id
      , day_dt
      , event_dt_ht
      , post_visitor_id
      , visit_session_id
      , page_event_type_id
      , post_event_id_list_desc
      , post_page_nm
      , page_nm
      , geo_country_cd
      , referrer_type_nm
      , ROW_NUMBER() OVER (PARTITION BY visit_session_id ORDER BY event_dt_ht) hit_rank
      , visit_page_nbr
      , post_evar1_desc
      , post_evar3_desc
      , post_evar5_desc
      , post_evar6_desc
      , post_evar10_desc
      , post_evar22_desc
      , post_evar56_desc
      , post_evar57_desc
      , CASE WHEN (page_event_type_id = 0 OR post_page_nm is not null) THEN 1 ELSE 0 END AS pv_event 
    FROM `dw_vw.omniture_event_cdm_cbsicbssportssite`
    WHERE
      report_suite_id_nm = 'cbsicbssportssite'
     AND day_dt BETWEEN '2022-08-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    --  AND post_evar57_desc LIKE '%top%th%'
    AND post_evar56_desc NOT LIKE '%gametracker%'
--   AND day_dt BETWEEN '2020-05-11' AND '2020-05-17'
  --    AND LOWER(geo_country_cd) = 'usa'
  )
  , sportsapp AS (
    SELECT * FROM hits
    WHERE post_evar1_desc LIKE '%app%'
    AND post_evar1_desc NOT LIKE '%fantasy_app%'
  
  -- CBSSports App Segment
    -- ((
    --   post_evar5_desc IN (
    --   'cbsisqortsapp-android'
    --   , 'cbsicbssportsapp'
    --   , 'cbsisportsapp-iphone'
    --   , 'cbsisportsapp'
    --   , 'cbsisportsapp-atablet'
    --   , 'cbsisportsapp-android'
    --   , 'cbsisportsapp-ipad'
    --   )
    --   OR post_evar1_desc IN (
    --   'cbssports_app_ios'
    --   , 'cbssports_app_android'
    --   ))
    --   AND (
    --     (page_nm NOT LIKE '%launch screen%' 
    --       AND page_nm NOT LIKE '%left drawer%')
    --       OR page_nm IS NULL)
    --   )
  )

  , all_hits as (

  SELECT
    day_dt,
    post_visitor_id,
    visit_session_id,
    page_event_type_id,
    post_event_id_list_desc,
    post_page_nm,
    post_evar1_desc,
    post_evar56_desc,
    post_evar57_desc,
    'sportsapp' as platform,
    hit_rank,
    post_evar6_desc,
    geo_country_cd
  FROM sportsapp

  ),

airship_data AS (

SELECT *
FROM `i-dss-sports-data.sports_vw.airship_sports_push_alert_performance_day`
WHERE notification_dt BETWEEN '2022-08-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)

  SELECT * FROM(
  -- Total
  SELECT
    all_hits.day_dt as ds,
    CASE WHEN post_evar1_desc LIKE '%ios%' THEN 'iOS' WHEN post_evar1_desc LIKE '%android%' THEN 'Android' ELSE 'Other' END AS device_os,
    post_evar56_desc,
    TRIM(REGEXP_REPLACE(post_evar57_desc, r'([^\p{ASCII}]+)', '')) AS post_evar57_desc,
    REGEXP_CONTAINS(post_evar57_desc, r'([^\p{ASCII}]+)') AS has_emojis,
    campaign_category_cd,
    message_desc,
    -- 'total' as project,
    user_detail_platform_cd,
    -- notification_day_nm  notification_time,
    notification_week_of_year_nbr,
    CONCAT(all_hits.day_dt, " ", SAFE.PARSE_TIME('%H:%M',notification_time)) AS notification_timestamp,
    MAX(direct_open_cnt) AS direct_open_cnt,
    MAX(push_notification_delivery_cnt) AS push_notification_delivery_cnt,
    MAX(direct_open_rate_pct) AS direct_open_rate_pct,
    COUNT(DISTINCT post_visitor_id) AS uniques,
    COUNT(DISTINCT visit_session_id) as visits,
  --   COUNT(DISTINCT visit_session_id) as visits,
    COUNTIF(((page_event_type_id = 0 OR post_page_nm is not null) OR page_event_type_id IN (10,11,12)) AND page_event_type_id = 0) AS page_views,
    -- COUNTIF(FORMAT("%t", ARRAY(SELECT id FROM UNNEST(post_event_id_list_desc))) LIKE '%718%') AS video_plays2,--media starts
    COUNTIF(FORMAT("%t", ARRAY(SELECT id FROM UNNEST(post_event_id_list_desc))) LIKE '%251%') AS video_plays,
    COUNTIF(FORMAT("%t", ARRAY(SELECT id FROM UNNEST(post_event_id_list_desc))) LIKE '%723%') AS video_ad_plays,
    SUM(COALESCE(SAFE_CAST((SELECT value FROM UNNEST(post_event_id_list_desc) WHERE id = '722') AS INT64),0))/60 AS video_time_spent_mins,
    SUM(COALESCE(SAFE_CAST((SELECT value FROM UNNEST(post_event_id_list_desc) WHERE id = '725') AS INT64),0))/60 AS video_ad_time_spent_mins
  FROM all_hits
  JOIN airship_data ad
  -- ON LOWER(REPLACE(TRIM(REGEXP_REPLACE(message_desc, r'([^\p{ASCII}]+)', '')), '\\n', '')) = LOWER(TRIM(REGEXP_REPLACE(post_evar57_desc, r'([^\p{ASCII}]+)', '')))
  ON LOWER(REPLACE(TRIM(REGEXP_REPLACE(message_desc, r'[^a-zA-Z]', '')), '\\n', '')) = LOWER(TRIM(REGEXP_REPLACE(post_evar57_desc, r'[^a-zA-Z]', '')))

  AND ad.notification_dt = all_hits.day_dt
  AND CASE WHEN LOWER(post_evar1_desc) LIKE '%ios%' THEN 'ios' WHEN LOWER(post_evar1_desc) LIKE '%android%' THEN 'android' ELSE 'other' END = LOWER(user_detail_platform_cd)
  GROUP BY 1,2,3,4,5,6,7,8,9,10
  )
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
  ORDER BY ds DESC, page_views DESC,1,2,3,4,5
