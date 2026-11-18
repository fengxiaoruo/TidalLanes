SELECT fixture_id,
       CASE WHEN hour(cast(stamp AS timestamp))>=7 AND hour(cast(stamp AS timestamp))<9 THEN 'AM'
            WHEN hour(cast(stamp AS timestamp))>=17 AND hour(cast(stamp AS timestamp))<19 THEN 'PM'
            ELSE 'OTHER' END period,
       cast(substr(stamp,12,2) AS int)*6+cast(floor(cast(substr(stamp,15,2) AS int)/10.0) AS int) slot10,
       cast(substr(stamp,12,2) AS int)*2+cast(floor(cast(substr(stamp,15,2) AS int)/30.0) AS int) slot30
FROM (
SELECT 't00' fixture_id, '2025-05-19 00:00:00' stamp
UNION ALL
SELECT 't01' fixture_id, '2025-05-19 06:59:59' stamp
UNION ALL
SELECT 't02' fixture_id, '2025-05-19 07:00:00' stamp
UNION ALL
SELECT 't03' fixture_id, '2025-05-19 07:29:59' stamp
UNION ALL
SELECT 't04' fixture_id, '2025-05-19 07:30:00' stamp
UNION ALL
SELECT 't05' fixture_id, '2025-05-19 08:59:59' stamp
UNION ALL
SELECT 't06' fixture_id, '2025-05-19 09:00:00' stamp
UNION ALL
SELECT 't07' fixture_id, '2025-05-19 16:59:59' stamp
UNION ALL
SELECT 't08' fixture_id, '2025-05-19 17:00:00' stamp
UNION ALL
SELECT 't09' fixture_id, '2025-05-19 18:59:59' stamp
UNION ALL
SELECT 't10' fixture_id, '2025-05-19 19:00:00' stamp
UNION ALL
SELECT 't11' fixture_id, '2025-05-19 23:59:59' stamp
) t;
