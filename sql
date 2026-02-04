/* Найти кандидатов: где вообще есть LAST_SAVED / LAST_UPDATE и т.п. */
SELECT owner, table_name, column_name, data_type
FROM all_tab_columns
WHERE owner = 'RB_REP'
  AND (
       UPPER(column_name) LIKE '%LAST%SAV%'
    OR UPPER(column_name) LIKE '%LAST%UPD%'
    OR UPPER(column_name) LIKE '%LAST%MOD%'
    OR UPPER(column_name) LIKE '%SAV%TIME%'
    OR UPPER(column_name) LIKE '%UPD%TIME%'
  )
ORDER BY table_name, column_name;