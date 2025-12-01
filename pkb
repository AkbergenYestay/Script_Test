WITH maxval AS (
  SELECT date_value, report_id, iin
  FROM (
    SELECT t0.date_value,
           t0.report_id,
           t0.iin,
           ROW_NUMBER() OVER (PARTITION BY t0.iin ORDER BY t0.date_value DESC, t0.report_id DESC) rn
    FROM DDS.PKB_CLIENT_REPORTS_FL t0
    WHERE proc_status <> '1108'
      AND TRUNC(date_change) <= TO_DATE(:snapshot_date,'YYYY-MM-DD')
  )
  WHERE rn = 1
),
pkp_clean AS (
  SELECT contract_id,
         report_id,
         day_cnt,
         pay_sum,
         date_value,
         TO_DATE(
           '01-' || LPAD(monthno,2,'0') || '-' ||
           CASE WHEN year < 100 THEN TO_CHAR(year+2000) ELSE TO_CHAR(year) END,
           'DD-MM-YYYY'
         ) AS payment_date
  FROM DDS.PKB_CONTRACT_PAYMENTS_FL
)
SELECT /*+ PARALLEL(8) */
  p.contract_id,
  p.report_id,
  MAX(p.day_cnt) AS max_delay_day_cnt_2y,
  MAX(p.pay_sum) AS max_delay_amount_2y,
  TO_DATE(:snapshot_date,'YYYY-MM-DD') AS snapshot
FROM pkp_clean p
JOIN maxval m
  ON m.date_value = p.date_value
 AND m.report_id  = p.report_id
WHERE p.payment_date BETWEEN TO_DATE(:snapshot_date,'YYYY-MM-DD') - 730
                        AND TO_DATE(:snapshot_date,'YYYY-MM-DD')
GROUP BY p.contract_id, p.report_id;