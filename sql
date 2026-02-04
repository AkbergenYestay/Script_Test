WITH need AS (
    -- wf_w4_pfm_top1mcg -> s_m_w4_pfm_top1mcg
    SELECT 7387 AS workflow_id, 7389 AS task_id FROM dual UNION ALL

    -- wf_w4_pfm_top3_transactions -> s_m_w4_pfm_top3_transactions
    SELECT 7468, 7470 FROM dual UNION ALL

    -- wf_w4_pfm_top3_merchants -> s_m_w4_pfm_top3_merchants
    SELECT 7427, 7429 FROM dual UNION ALL

    -- wf_w4_pfm_treat_yourself -> s_m_w4_pfm_treat_yourself
    SELECT 7418, 7421 FROM dual
),
last_wf_run AS (
    SELECT
        r.workflow_id,
        r.workflow_name,
        r.workflow_run_id,
        r.start_time AS wf_start_time,
        r.end_time   AS wf_end_time,
        r.run_err_code,
        ROW_NUMBER() OVER (PARTITION BY r.workflow_id ORDER BY r.start_time DESC) rn
    FROM RB_REP.OPB_WFLOW_RUN r
    WHERE TRUNC(r.start_time) = TRUNC(SYSDATE)
      AND r.workflow_id IN (SELECT workflow_id FROM need)
),
ins_rows AS (
    SELECT
        s.workflow_run_id,
        MAX(s.affected_rows) AS inserted_rows
    FROM RB_REP.OPB_SWIDGINST_LOG s
    GROUP BY s.workflow_run_id
),
last_task_run AS (
    SELECT
        tr.workflow_run_id,
        tr.instance_id,
        tr.instance_name,
        tr.task_id,
        tr.start_time AS task_start_time,

        CASE
            WHEN tr.end_time IS NULL THEN NULL
            WHEN tr.start_time IS NOT NULL AND tr.end_time < tr.start_time THEN NULL
            WHEN tr.end_time > SYSDATE THEN NULL
            ELSE tr.end_time
        END AS task_end_time,

        tr.run_err_code AS task_run_err_code,
        ROW_NUMBER() OVER (
            PARTITION BY tr.workflow_run_id, tr.instance_id
            ORDER BY tr.start_time DESC
        ) rn
    FROM RB_REP.OPB_TASK_INST_RUN tr
    WHERE TRUNC(tr.start_time) = TRUNC(SYSDATE)
      AND tr.task_id IN (SELECT task_id FROM need)
)
SELECT
    w.workflow_name,
    w.workflow_id,
    w.workflow_run_id,
    w.wf_start_time,
    w.wf_end_time,
    CASE
        WHEN w.workflow_run_id IS NULL THEN 'NOT STARTED'
        WHEN w.wf_end_time IS NULL THEN 'RUNNING'
        WHEN NVL(w.run_err_code,0) <> 0 THEN 'ERROR/WARNING'
        ELSE 'SUCCESS'
    END AS wf_status,
    NVL(ir.inserted_rows, 0) AS inserted_rows,

    n.task_id,
    tr.task_start_time,
    tr.task_end_time,
    CASE
        WHEN w.workflow_run_id IS NULL THEN 'NO RUN'
        WHEN tr.task_start_time IS NULL THEN 'TASK NO RUN'
        WHEN tr.task_end_time IS NULL THEN 'TASK RUNNING'
        WHEN NVL(tr.task_run_err_code,0) <> 0 THEN 'TASK ERROR/WARNING'
        ELSE 'TASK SUCCESS'
    END AS task_status
FROM need n
LEFT JOIN last_wf_run w
    ON w.workflow_id = n.workflow_id
   AND w.rn = 1
LEFT JOIN ins_rows ir
    ON ir.workflow_run_id = w.workflow_run_id
LEFT JOIN last_task_run tr
    ON tr.workflow_run_id = w.workflow_run_id
   AND tr.task_id = n.task_id
   AND tr.rn = 1
ORDER BY n.workflow_id
