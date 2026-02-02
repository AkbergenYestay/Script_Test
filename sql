(
WITH base AS (
  SELECT
    FROM_TZ(CAST(TRUNC(CURRENT_DATE, 'MM') AS TIMESTAMP), '+00:00') AS month_start_utc
  FROM dual
)
SELECT
  '["'
  || TO_CHAR(month_start_utc + INTERVAL '1' DAY,  'YYYY-MM-DD HH24:MI:SSTZH:TZM')
  || '","'
  || TO_CHAR(month_start_utc + INTERVAL '14' DAY, 'YYYY-MM-DD HH24:MI:SSTZH:TZM')
  || '")'
FROM base
) AS period