WITH bounds AS (
    SELECT
        DATE '2026-01-11' AS day_start,
        DATE '2026-01-12' AS next_day_start
    FROM dual
),
need AS (
    SELECT 'wf_PROD_edw_to_operations_account'  wf, 's_m_PROD_account_edw_to_operations'  task FROM dual UNION ALL
    SELECT 'wf_PROD_edw_to_operations_deposit', 's_m_PROD_deposit_edw_to_operations'       FROM dual UNION ALL
    SELECT 'wf_PROD_edw_to_operations_openway', 's_m_PROD_openway_edw_to_operations'       FROM dual
),
last_wf_run AS (
    SELECT
        r.workflow_id,
        r.workflow_name,
        r.workflow_run_id,
        r.start_time AS wf_start_time,
        r.end_time   AS wf_end_time,
        r.run_err_code,
        ROW_NUMBER() OVER (
            PARTITION BY r.workflow_id
            ORDER BY r.start_time DESC
        ) rn
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
    n.wf                 AS workflow_name,
    n.task               AS task_name,
    w.workflow_run_id,
    w.wf_start_time,
    w.wf_end_time,
    w.run_err_code       AS wf_err_code,
    t.start_time         AS task_start_time,
    t.end_time           AS task_end_time,
    t.run_err_code       AS task_err_code,
    CASE
        WHEN w.workflow_run_id IS NULL THEN 'NO RUN'
        WHEN w.wf_end_time IS NULL THEN 'WF RUNNING'
        WHEN t.task_name IS NULL THEN 'TASK NOT FOUND'
        WHEN t.end_time IS NULL THEN 'TASK RUNNING'
        WHEN NVL(w.run_err_code,0) <> 0
          OR NVL(t.run_err_code,0) <> 0 THEN 'ERROR/WARNING'
        ELSE 'SUCCESS'
    END AS status
FROM need n
LEFT JOIN last_wf_run w
       ON w.workflow_name = n.wf
      AND w.rn = 1
LEFT JOIN RB_REP.OPB_TASK_INST_RUN t
       ON t.workflow_run_id = w.workflow_run_id
      AND t.task_name = n.task
      AND t.task_type = 68
ORDER BY workflow_name;