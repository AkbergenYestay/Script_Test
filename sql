CREATE UNIQUE INDEX IF NOT EXISTS pfm_etl_log_uq_workflow_start
ON public.pfm_etl_log (workflow_id, start_time);