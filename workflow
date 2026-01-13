CREATE TABLE IF NOT EXISTS pfm.pfm_run_ctx (
  workflow_name   text        NOT NULL,
  workflow_run_id text        NOT NULL,
  run_uuid        uuid        NOT NULL,
  started_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_name, workflow_run_id)
);


INSERT INTO pfm.pfm_run_ctx (workflow_name, workflow_run_id, run_uuid)
VALUES ('$PMWorkflowName', '$PMWorkflowRunId', gen_random_uuid())
ON CONFLICT (workflow_name, workflow_run_id)
DO NOTHING;

UPDATE pfm.pfm_top3_transactions t
SET subscription_id = (
      SELECT run_uuid::text
      FROM pfm.pfm_run_ctx
      WHERE workflow_name = '$PMWorkflowName'
        AND workflow_run_id = '$PMWorkflowRunId'
    ),
    partition_id = (mod(abs(hashtext(coalesce(t.iin, ''))), 4) + 1)
WHERE t.subscription_id IS NULL
  AND t.upload_date = CURRENT_DATE;


INSERT INTO pfm.pfm_top_table_load_status
  (table_name, load_date, status, subscription_id, message)
VALUES
(
  'PFM_TOP3_TRANSACTIONS',
  now(),
  'SUCCESS',
  (SELECT run_uuid::text
   FROM pfm.pfm_run_ctx
   WHERE workflow_name = '$PMWorkflowName'
     AND workflow_run_id = '$PMWorkflowRunId'),
  CONCAT(
    'wf=', '$PMWorkflowName',
    '; run_id=', '$PMWorkflowRunId',
    '; sess=', '$PMSessionName',
    '; rows=', '$PMRowsLoaded'
  )
)
ON CONFLICT ON CONSTRAINT pfm_top_table_load_status_table_name_status_load_date_key
DO NOTHING;



INSERT INTO pfm.pfm_top_table_load_status
  (table_name, load_date, status, subscription_id, message)
VALUES
(
  'PFM_TOP3_TRANSACTIONS',
  now(),
  'FAILED',
  (SELECT run_uuid::text
   FROM pfm.pfm_run_ctx
   WHERE workflow_name = '$PMWorkflowName'
     AND workflow_run_id = '$PMWorkflowRunId'),
  CONCAT(
    'wf=', '$PMWorkflowName',
    '; run_id=', '$PMWorkflowRunId',
    '; sess=', '$PMSessionName',
    '; err=', '$PMErrorCode'
  )
)
ON CONFLICT ON CONSTRAINT pfm_top_table_load_status_table_name_status_load_date_key
DO NOTHING;