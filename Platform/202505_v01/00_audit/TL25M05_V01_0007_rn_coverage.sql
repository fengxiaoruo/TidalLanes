SELECT `date`, count(1) n_rows,
       min(cast(`time` AS bigint)) min_rn_time, max(cast(`time` AS bigint)) max_rn_time,
       sum(CASE WHEN next_rn_id IS NULL THEN 1 ELSE 0 END) n_no_next
FROM move_rn
WHERE city='V0110000' AND `date`=20250519
GROUP BY `date`
ORDER BY `date`;
