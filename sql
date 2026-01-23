
SELECT
    segment_name AS table_name,
    ROUND(SUM(bytes) / 1024 / 1024, 2) AS size_mb
FROM user_segments
WHERE segment_type IN ('TABLE','TABLE PARTITION','TABLE SUBPARTITION',
                       'LOBSEGMENT','LOB PARTITION','LOB SUBPARTITION',
                       'NESTED TABLE','IOT_OVERFLOW')
GROUP BY segment_name
ORDER BY size_mb DESC;


-- поменяй :owner на нужную схему (например 'RUDWH_DDS')
SELECT
    owner,
    segment_name AS object_name,
    ROUND(SUM(bytes) / 1024 / 1024, 2) AS size_mb
FROM dba_segments
WHERE owner = :owner
  AND segment_type IN ('TABLE','TABLE PARTITION','TABLE SUBPARTITION',
                       'INDEX','INDEX PARTITION','INDEX SUBPARTITION',
                       'LOBSEGMENT','LOB PARTITION','LOB SUBPARTITION',
                       'IOT_OVERFLOW')
GROUP BY owner, segment_name
ORDER BY size_mb DESC;


-- :owner = схема (например 'RUDWH_DDS')
WITH seg AS (
    SELECT owner, segment_name, segment_type, bytes
    FROM dba_segments
    WHERE owner = :owner
),
tbl AS (
    SELECT owner, table_name
    FROM dba_tables
    WHERE owner = :owner
),
t_bytes AS (
    -- сама таблица (включая партиции)
    SELECT t.owner, t.table_name, SUM(s.bytes) bytes
    FROM tbl t
    JOIN seg s
      ON s.owner = t.owner
     AND s.segment_name = t.table_name
     AND s.segment_type IN ('TABLE','TABLE PARTITION','TABLE SUBPARTITION','IOT_OVERFLOW')
    GROUP BY t.owner, t.table_name
),
i_bytes AS (
    -- индексы таблицы (включая партиции)
    SELECT i.table_owner AS owner, i.table_name, SUM(s.bytes) bytes
    FROM dba_indexes i
    JOIN seg s
      ON s.owner = i.owner
     AND s.segment_name = i.index_name
     AND s.segment_type IN ('INDEX','INDEX PARTITION','INDEX SUBPARTITION')
    WHERE i.table_owner = :owner
    GROUP BY i.table_owner, i.table_name
),
l_bytes AS (
    -- LOB сегменты таблицы (включая партиции)
    SELECT l.owner, l.table_name, SUM(s.bytes) bytes
    FROM dba_lobs l
    JOIN seg s
      ON s.owner = l.owner
     AND s.segment_name = l.segment_name
     AND s.segment_type IN ('LOBSEGMENT','LOB PARTITION','LOB SUBPARTITION')
    WHERE l.owner = :owner
    GROUP BY l.owner, l.table_name
)
SELECT
    t.owner,
    t.table_name,
    ROUND(NVL(tb.bytes,0)/1024/1024, 2) AS table_mb,
    ROUND(NVL(ib.bytes,0)/1024/1024, 2) AS index_mb,
    ROUND(NVL(lb.bytes,0)/1024/1024, 2) AS lob_mb,
    ROUND((NVL(tb.bytes,0)+NVL(ib.bytes,0)+NVL(lb.bytes,0))/1024/1024, 2) AS total_mb
FROM tbl t
LEFT JOIN t_bytes tb ON tb.owner=t.owner AND tb.table_name=t.table_name
LEFT JOIN i_bytes ib ON ib.owner=t.owner AND ib.table_name=t.table_name
LEFT JOIN l_bytes lb ON lb.owner=t.owner AND lb.table_name=t.table_name
ORDER BY total_mb DESC;
