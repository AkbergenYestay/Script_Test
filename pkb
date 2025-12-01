with 
  maxval as
   (select 
max(date_value) date_value, max(report_id) report_id, iin
      from DDS.PKB_CLIENT_REPORTS_FL t0
     where proc_status != '1108'
       --and date_value<=trunc(sysdate-1)
       and trunc(date_change) <= to_date('$$SNAPSHOT_DATE', 'yyyy-mm-dd')
       --and iin in ('710222350215','820801350428','921118351000')
     group by iin),
  pkb_tb as
   (select 
 PCPF.CONTRACT_ID,
           PCPF.REPORT_ID,
           MAX(PCPF.DAY_CNT) as MAX_DELAY_DAY_CNT_2Y,
           MAX(PCPF.PAY_SUM) as MAX_DELAY_AMOUNT_2Y
      FROM DDS.PKB_CONTRACT_PAYMENTS_FL PCPF
     --inner join pkb_tb pkb_tb on pkb_tb.date_value=PCPF.Date_Value and pkb_tb.report_id=PCPF.Report_Id and pkb_tb.contract_id=PCPF.Contract_Id
     inner join maxval m
        on m.date_value = PCPF.Date_Value
       and m.report_id = PCPF.Report_Id
     WHERE 1 = 1
       and (TO_DATE('01' || '.' || CASE
                      WHEN PCPF.MONTHNO < 10 THEN
                       '0' || PCPF.MONTHNO
                      ELSE
                       PCPF.MONTHNO
                    END || '.' || CASE
                      WHEN PCPF.YEAR < 10 THEN
                       '200' || PCPF.YEAR
                      WHEN PCPF.YEAR >= 10 AND PCPF.YEAR < 2000 THEN
                       '20' || PCPF.YEAR
                      ELSE
                       PCPF.YEAR
                    END,
                    'DD.MM.YY') - TRUNC(sysdate)) <= 730
     GROUP BY PCPF.CONTRACT_ID, PCPF.REPORT_ID)
select /*+ PARALLEL(8 */
   CONTRACT_ID,
   REPORT_ID,
   MAX_DELAY_DAY_CNT_2Y,
   MAX_DELAY_AMOUNT_2Y,
   to_date('$$SNAPSHOT_DATE', 'yyyy-mm-dd') as SNAPSHOT
from pkb_tb
