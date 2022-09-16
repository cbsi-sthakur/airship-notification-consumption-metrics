SELECT DISTINCT 
      -- hit_id
       day_dt
      , post_evar1_desc
      -- , event_dt_ht
      -- , post_visitor_id
      -- , visit_session_id
      , post_evar56_desc
      , post_evar57_desc
      , CAST(post_evar57_desc AS BYTES) AS post_evar57_desc_bytes
      , TO_HEX(CAST(post_evar57_desc AS BYTES)) AS post_evar57_desc_bytes
      , TRIM(REGEXP_REPLACE(post_evar57_desc, r'([^\p{ASCII}]+)', '')) AS sans_ascii
    FROM `dw_vw.omniture_event_cdm_cbsicbssportssite`
    WHERE
      report_suite_id_nm = 'cbsicbssportssite'
     AND day_dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
     AND post_evar1_desc LIKE '%app%' 
     AND LOWER(post_evar57_desc) LIKE '%connor%' #works for aug 23 2022


-- with sample AS (SELECT '\u522b\u8001\u662f\u665a' AS S)
--     SELECT s, NORMALIZE(s, NFC) AS s_NFC, NORMALIZE(s, NFKC) AS s_NFKC, NORMALIZE(s, NFD) AS s_NFD, NORMALIZE(s, NFKD) AS s_NFKD
--     from sample