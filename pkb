WITH maxval AS (
  SELECT
    MAX(date_value)   AS date_value,
    MAX(report_id)    AS report_id,
    iin
  FROM DDS.PKB_CLIENT_REPORTS_FL t0
  WHERE proc_status <> '1108'
    AND TRUNC(date_change) <= TO_DATE(:snapshot_date, 'YYYY-MM-DD')
  GROUP BY iin
),
pkb_tb AS (
  SELECT
    pcpf.contract_id,
    pcpf.report_id,
    MAX(pcpf.day_cnt) AS max_delay_day_cnt_2y,
    MAX(pcpf.pay_sum) AS max_delay_amount_2y
  FROM DDS.PKB_CONTRACT_PAYMENTS_FL pcpf
  JOIN maxval m
    ON m.date_value = pcpf.date_value
   AND m.report_id = pcpf.report_id
  WHERE
    -- вычисляем первый день месяца как date и фильтруем по интервалу snapshot-730..snapshot
    TO_DATE(
      '01-' || LPAD(pcpf.monthno, 2, '0') || '-' ||
      (CASE WHEN pcpf.year < 100 THEN (pcpf.year + 2000) ELSE pcpf.year END),
      'DD-MM-YYYY'
    ) BETWEEN TO_DATE(:snapshot_date, 'YYYY-MM-DD') - 730
        AND TO_DATE(:snapshot_date, 'YYYY-MM-DD')
  GROUP BY pcpf.contract_id, pcpf.report_id
)
SELECT /*+ PARALLEL(8) */
  contract_id,
  report_id,
  max_delay_day_cnt_2y,
  max_delay_amount_2y,
  TO_DATE(:snapshot_date, 'YYYY-MM-DD') AS snapshot
FROM pkb_tb;