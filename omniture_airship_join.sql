SELECT DISTINCT 
      -- hit_id
       day_dt
      , post_evar1_desc
      -- , event_dt_ht
      -- , post_visitor_id
      -- , visit_session_id
      , post_evar56_desc
      , post_evar57_desc
      , message_desc
    FROM `dw_vw.omniture_event_cdm_cbsicbssportssite` om
    JOIN `i-dss-sports-data.sports_vw.airship_sports_push_alert_performance_day` pap
    ON LOWER(TRIM(REGEXP_REPLACE(message_desc, r'([^\p{ASCII}]+)', ''))) = LOWER(TRIM(REGEXP_REPLACE(post_evar57_desc, r'([^\p{ASCII}]+)', '')))
    WHERE notification_dt = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) 
    AND day_dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND report_suite_id_nm = 'cbsicbssportssite'
    AND post_evar1_desc LIKE '%app%'
    AND LOWER(message_desc) LIKE '%connor%'
    AND LOWER(post_evar57_desc) LIKE '%connor%'
