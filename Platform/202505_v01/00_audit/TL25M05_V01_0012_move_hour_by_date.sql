SELECT `date`,
       substr(cast(stime AS string),12,2) raw_hour,
       count(1) n_rows,
       count(DISTINCT move_id) n_move_id_approx_within_hour
FROM move_month
WHERE city='V0110000' AND `date` IN (20250506,20250519,20250520)
GROUP BY `date`, substr(cast(stime AS string),12,2);
