SELECT partition_name, high_value
FROM dba_tab_partitions
WHERE table_owner = 'DSB_DE'
  AND table_name  = 'RB2_FS_TRANS_IN'
ORDER BY partition_position;
