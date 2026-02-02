(WITH base AS (
	  SELECT FROM_TZ(CAST(TRUNC(CURRENT_DATE) AS TIMESTAMP), '+00:00') AS local_midnight
	  FROM dual
	)
	SELECT
	  '['
	  || TO_CHAR(local_midnight - INTERVAL '5' HOUR, 'YYYY-MM-DD HH24:MI:SSTZH:TZM')
	  || ','
	  || TO_CHAR(local_midnight + INTERVAL '13' DAY + INTERVAL '19' HOUR, 'YYYY-MM-DD HH24:MI:SSTZH:TZM')
	  || ')' AS tstzrange_literal
	FROM base) as period,
	
WITH date_filter AS (	
    select
		/*date 'B_DATE' AS b_date,
        date 'E_DATE' AS e_date */
		
		add_months(trunc(sysdate, 'mm'), -1) as b_date,
		trunc(sysdate, 'mm') as e_date
    from dual
),
xls AS (
    SELECT 
        t1.tc_txn_iid,t1.DATE_VALUE
    FROM dds.W4_TC_TXN_XLS_FL t1
    WHERE 
        t1.txn_type = 22
        AND t1.DATE_VALUE >= (SELECT b_date FROM date_filter) - 20
        AND t1.DATE_VALUE < (SELECT e_date FROM date_filter) + 5
),
bonus as (
  select 
	  a.iin, 
	  sum(amount) as amount
	  from (
	  SELECT distinct
	      g.iin_bin as iin,
	      t.term_gm_subject_id,
	      t.tc_txn_link_iid,
	      t.tc_txn_iid,
	      t.txn_type,
	      t.txn_source,
	      t.txn_date,
	      t.void_txn_iid,
	      t.void_orig_txn_iid,
	      ta.fact_amount as amount,
	      t.terminal_no,
	      ta.txn_type txn_type_ta,
	      T.PROCESS_DATE_UTC,
	      T.ORIG_PURCH_AMT,
	      t.txn_ext_ref,
	      s.campaign_name
	  FROM dds.W4_TC_TXN_XLS_FL t
	  LEFT JOIN DDS.W4_TC_TXN_AMOUNT_XLS_FL ta ON ta.tc_txn_iid = t.tc_txn_iid
	  LEFT join dds.W4_CAMPAIGN_XLS_S s ON ta.w4_campaign_xls_h_iid = s.dwh_id AND s.is_actual = 'A'
	  join dds.gm_subject_h g on t.term_gm_subject_id = g.dwh_id
	  WHERE 1=1
	    and t.date_value >= (SELECT b_date FROM date_filter)
	    and ta.date_value >= (SELECT b_date FROM date_filter)
	    and t.date_value < (SELECT e_date FROM date_filter) + 5
	    and ta.date_value < (SELECT e_date FROM date_filter) + 5
	    and trunc(t.txn_date) >= (SELECT b_date FROM date_filter)
	    and trunc(t.txn_date) < (SELECT e_date FROM date_filter)  
	    and trunc(T.PROCESS_DATE_UTC) >= (SELECT b_date FROM date_filter)
	    and trunc(T.PROCESS_DATE_UTC) < (SELECT e_date FROM date_filter)
	    and ta.txn_type NOT IN (52, 4, 99)
	    and t.txn_type <> 5
	    AND (
	        NVL(t.orig_purch_amt, 0) <> 0 
	        OR NVL(t.disc_amt, 0) <> 0 
	        OR NVL(t.rdm_amt, 0) <> 0 
	        OR NVL(t.awd_amt, 0) <> 0 
	        OR t.txn_source IN (2, 4, 13, 14, 15)
	        OR (t.txn_source = 17 AND t.txn_type = 1)
	        OR (t.txn_source = 17 AND t.txn_type = 5)
	    )
	    AND NOT (
	        t.txn_type = 22
	        OR (t.txn_type = 2 AND t.txn_source = 17)
	        OR (
	            t.txn_type = 5 
	            AND t.void_orig_txn_iid IN (
	                SELECT tc_txn_iid FROM xls WHERE date_value = t.date_value
	            )
	        )
	    ) 
	  ) a
  where a.amount > 0
  group by a.iin
  having sum(amount) >= 200
)
select /* +PARALLEL(8)*/
	b.iin, 
	round(b.amount) as amount,
	'bonus_proc' as product_code,
	trunc(sysdate) as snapshot_date,
	'["' || to_char(trunc(sysdate, 'mm') + 1, 'DD.MM.YYYY HH24:MI:SS') || '","' || to_char(trunc(sysdate, 'mm') + 14, 'DD.MM.YYYY HH24:MI:SS') || '"]' as period,
	'month' as period_type,
	trunc(sysdate) as upload_date
from bonus b
