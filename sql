
SELECT * FROM (
select distinct
	wf.workflow_id as WORKFLOW_ID,
	nvl(lower(wrm.workflow_name), lower(wfname.workflow_name)) as WF_NAME,
	wrm.start_time as WF_STARTTIME,
	wrm.end_time as WF_ENDTIME,
	CASE
      WHEN wrm.RUN_ERR_CODE <> 0 THEN 'ERROR/WARNING'
      WHEN wrm.RUN_ERR_CODE = 0 AND wrm.START_TIME IS NOT NULL AND wrm.END_TIME IS NULL THEN 'RUNNING'
      WHEN wrm.START_TIME IS NULL THEN 'NOT STARTED'
      ELSE 'SUCCESS'
    END      WF_STATUS,
	ins.instance_name as INST_NAME,
	ins.ins_status as INST_STATUS,
	ins.start_time as INST_STARTTIME,
	ins.end_time as INST_ENDTIME,
	to_char(to_date(s.START_TIME, 'mm.dd.yyyy HH24:MI:SS'), 'HH24:MI:SS') as WF_SCHEDTIME
from RB_REP.OPB_workflow wf
	-- MAIN WORKFLOW
	left join ( select workflow_name, workflow_id, max(start_time) 
				from RB_REP.OPB_WFLOW_RUN
				where 1=1 --and workflow_id in (5810, 5731)
				group by workflow_name, workflow_id
	) wfname on wfname.workflow_id=wf.workflow_id
	-- SHEDULER TIME	
	left join RB_REP.OPB_SCHEDULER s ON s.SCHEDULER_ID = wf.SCHEDULER_ID
	-- WORKFLOW RUN	
	left join ( select * 
				from RB_REP.OPB_WFLOW_RUN t0
				where 1=1
				and trunc(START_TIME) = trunc(SYSDATE)
				--and t0.workflow_id = 5810
				and t0.START_TIME = (
					select max(t1.start_time) 
					from RB_REP.OPB_WFLOW_RUN t1 
					where 1=1
					and t1.workflow_id = t0.workflow_id
			  )
	) wrm on wf.workflow_id = wrm.workflow_id
	-- INSTANCE STATUS	
	left join ( select ins.workflow_id, ins.instance_name, run.start_time, run.end_time,
				case
			      when run.RUN_ERR_CODE <> 0 THEN 'ERROR/WARNING'
			      when run.RUN_STATUS_CODE = 6 AND run.START_TIME IS NOT NULL AND run.END_TIME = DATE '1753-01-01' THEN 'RUNNING'
			      when run.START_TIME IS NULL THEN 'NOT STARTED'
			      when run.RUN_STATUS_CODE = 1 THEN 'SUCCESS'
			      else 'SUCCESS'
				end ins_status
				from rb_rep.opb_task_inst ins
				left join (
					select * from RB_REP.OPB_TASK_INST_RUN t0
					where 1=1
					and trunc(t0.start_time) = trunc(sysdate)
					--and t0.workflow_id = 5810
					--and t0.task_type = 68
					and lower(t0.instance_name) not like '%sys_dependency%'
					and t0.start_time = (
						select max(t1.start_time) 
						from rb_rep.opb_task_inst_run t1 
						where 1=1
						and t1.start_time=t0.start_time
						and t0.instance_id=t1.instance_id
					)
				) run on run.instance_id = ins.instance_id 
			where 1=1
			and lower(run.instance_name) not like '%sys_dependency%'
			--and ins.workflow_id = 5810
			and ins.task_type = 68
	) ins on wf.workflow_id = ins.workflow_id
where (wf.workflow_id in (4726)) 	-- WF_DM_INCALL_CENTERSEGMENTATION_EDW
)
ORDER BY wf_starttime, wf_name
