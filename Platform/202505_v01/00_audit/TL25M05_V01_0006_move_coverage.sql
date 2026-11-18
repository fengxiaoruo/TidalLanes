SELECT substr(cast(stime AS string),12,2) raw_hour,
       count(1) n_rows,
       min(cast(stime AS string)) min_stime,
       max(cast(stime AS string)) max_stime,
       min(hour(from_unixtime(unix_timestamp(cast(stime AS string))))) min_unix_hour,
       max(hour(from_unixtime(unix_timestamp(cast(stime AS string))))) max_unix_hour,
       sum(if(cast(stime AS timestamp) IS NULL,1,0)) n_bad_stime,
       sum(if(cast(moi_id AS string)=cast(move_id AS string),1,0)) n_moi_eq_move,
       sum(if(start_poi IS NULL,1,0)) n_null_start_poi,
       sum(if(end_poi IS NULL,1,0)) n_null_end_poi,
       sum(if(cast(start_poi AS string)='0',1,0)) n_zero_start_poi,
       sum(if(cast(end_poi AS string)='0',1,0)) n_zero_end_poi,
       sum(if(cast(mode AS int)=1,1,0)) n_mode1
FROM move_month
WHERE city='V0110000' AND `date`=20250519
GROUP BY substr(cast(stime AS string),12,2)
ORDER BY raw_hour;
