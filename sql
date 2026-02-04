/* =========================================
   INFORMATICA REPOSITORY (rb_rep)
   WORKFLOW → SESSION → MAPPING → SRC → TGT
   ========================================= */

SELECT
    subj.subject_name                         AS folder_name,
    wf.workflow_name                          AS workflow_name,
    wf.is_valid                               AS wf_valid,
    wf.last_saved                             AS wf_last_saved,

    ti.task_instance_name                     AS session_name,
    t.task_type                               AS task_type,

    mp.mapping_name                           AS mapping_name,

    src.src_name                              AS source_name,
    src.dbd_name                              AS source_dbd,

    tgt.tgt_name                              AS target_name,
    tgt.dbd_name                              AS target_dbd

FROM opb_subject subj

JOIN opb_wflow wf
  ON wf.subject_id = subj.subject_id

JOIN opb_task_inst ti
  ON ti.workflow_id = wf.workflow_id

JOIN opb_task t
  ON t.task_id = ti.task_id
 AND t.task_type = 'SESSION'

LEFT JOIN opb_mapping mp
  ON mp.mapping_id = t.mapping_id

LEFT JOIN opb_mapping_src mps
  ON mps.mapping_id = mp.mapping_id
LEFT JOIN opb_src src
  ON src.src_id = mps.src_id

LEFT JOIN opb_mapping_targ mpt
  ON mpt.mapping_id = mp.mapping_id
LEFT JOIN opb_targ tgt
  ON tgt.targ_id = mpt.targ_id

WHERE subj.subject_name = 'RB_REP'
-- AND wf.workflow_name LIKE 'WF_%'

ORDER BY
    subj.subject_name,
    wf.workflow_name,
    ti.task_instance_name;