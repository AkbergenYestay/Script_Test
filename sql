/* =========================================
   WORKFLOW INFO (AS IN IMAGE)
   ========================================= */

SELECT
    wf.workflow_name                              AS workflow_name,              -- Наименование workflow
    wf.create_time                                AS start_date,                 -- Дата запуска
    wf.last_saved                                 AS last_update_date,           -- Дата последнего изменения
    wf.user_defined_source                        AS source_system,              -- Источник выгрузки данных
    wf.user_defined_period                        AS load_period,                -- Период выгрузки
    wf.user_defined_target                        AS target_object,              -- Целевая таблица / витрина
    wf.comments                                   AS comment                     -- Примечание

FROM opb_subject subj
JOIN opb_wflow wf
  ON wf.subject_id = subj.subject_id

WHERE subj.subject_name = 'RB_REP'
-- AND wf.workflow_name LIKE 'WF_%'

ORDER BY wf.workflow_name;