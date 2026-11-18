SELECT 'speed202505_pair_passage_day_20250519_all' source_table,
       count(1) n_rows,
       min(`date`) min_source_date,
       max(`date`) max_source_date,
       min(passage_date) min_passage_date,
       max(passage_date) max_passage_date
FROM speed202505_pair_passage_day_20250519_all
WHERE city='V0110000'
UNION ALL
SELECT 'speed202505_pair_passage_week_src20250520' source_table,
       count(1) n_rows,
       min(`date`) min_source_date,
       max(`date`) max_source_date,
       min(passage_date) min_passage_date,
       max(passage_date) max_passage_date
FROM speed202505_pair_passage_week_src20250520
WHERE city='V0110000';
