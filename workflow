WITH bounds AS (
    SELECT
        DATE '2026-01-11' AS day_start,
        DATE '2026-01-12' AS next_day_start
    FROM dual
),
need AS (
    SELECT 'wf_PROD_edw_to_operations_account'  wf, 's_m_PROD_account_edw_to_operations'  tsk FROM dual UNION ALL
    SELECT 'wf_PROD_edw_to_operations_deposit', 's_m_PROD_deposit_edw_to_operations'       FROM dual UNION ALL
    SELECT 'wf_PROD_edw_to_operations_openway', 's_m_PROD_openway_edw_to_operations'       FROM dual
),
last_run AS (
    SELECT
        r.workflow_id,
        r.workflow_name,
        r.workflow_run_id,
        r.start_time AS wf_start_time,
        r.end_time   AS wf_end_time,
        r.run_err_code AS wf_run_err_code,
        ROW_NUMBER() OVER (
            PARTITION BY r.workflow_id
            ORDER BY r.start_time DESC
        ) AS rn
    FROM RB_REP.OPB_WFLOW_RUN r
    CROSS JOIN bounds b
    WHERE r.workflow_name IN (
        'wf_PROD_edw_to_operations_account',
        'wf_PROD_edw_to_operations_deposit',
        'wf_PROD_edw_to_operations_openway'
    )
      AND r.start_time >= b.day_start
      AND r.start_time <  b.next_day_start
)
SELECT
    n.wf AS workflow_name,
    n.tsk AS task_name,
    lr.workflow_run_id,
    lr.wf_start_time,
    lr.wf_end_time,
    lr.wf_run_err_code,
    lg.start_time AS task_start_time,
    lg.end_time   AS task_end_time,
    lg.last_err_code AS task_last_err_code,
    lg.last_err_msg  AS task_last_err_msg,
    NVL(lg.affected_rows, 0) AS affected_rows,
    CASE
        WHEN lr.workflow_run_id IS NULL THEN 'NO RUN'
        WHEN lr.wf_end_time IS NULL THEN 'WF RUNNING'
        WHEN lg.instance_name IS NULL THEN 'TASK NOT FOUND'
        WHEN lg.end_time IS NULL THEN 'TASK RUNNING'
        WHEN NVL(lr.wf_run_err_code,0) <> 0 OR NVL(lg.last_err_code,0) <> 0 THEN 'ERROR/WARNING'
        ELSE 'SUCCESS'
    END AS overall_status
FROM need n
LEFT JOIN last_run lr
       ON lr.workflow_name = n.wf
      AND lr.rn = 1
LEFT JOIN RB_REP.OPB_SWIDGINST_LOG lg
       ON lg.workflow_id = lr.workflow_id
      AND lg.workflow_run_id = lr.workflow_run_id
      AND lg.instance_name = n.tsk
ORDER BY workflow_name;
