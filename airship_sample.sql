SELECT *
-- DISTINCT message_desc, CAST(message_desc AS STRING) AS message_desc_utf, 
-- TO_HEX(CAST(message_desc AS BYTES)) AS message_desc_to_hex,
-- TRIM(REGEXP_REPLACE(message_desc, r'([^\p{ASCII}]+)', '')) AS message_desc_sans_ascii
FROM `i-dss-sports-data.sports_vw.airship_sports_push_alert_performance_day` 
WHERE notification_dt = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) 
AND LOWER(post_evar57_desc) LIKE '%connor%' #works for aug 23 2022
-- AND LOWER(message_desc) LIKE '%top%th%'
-- ORDER BY direct_open_cnt DESC
LIMIT 100