WITH rc AS (
    SELECT 'Part Advice ' AS DISPL_COL, 'A' AS DATA_COL            
    UNION ALL SELECT 'Adjustment ' AS DISPL_COL, 'J' AS DATA_COL
    UNION ALL SELECT 'Request ' AS DISPL_COL, 'Q' AS DATA_COL
    UNION ALL SELECT 'Reversal ' AS DISPL_COL, 'R' AS DATA_COL
    UNION ALL SELECT 'Void ' AS DISPL_COL, 'V' AS DATA_COL
    UNION ALL SELECT 'Void Request ' AS DISPL_COL, 'v' AS DATA_COL
)
SELECT 
    posting_date,
    card_id,
    trans_date,
    trans_amount,
    trans_curr,
    fee_amount,
    acc_amount,
    end_balance,
    acc_curr,
    tr_type,
    TRANS_COUNTRY,
    TRANS_CITY,
    trans_details,
    mcc,
    auth_code,
    ret_ref_number,
    DCC
FROM (
    SELECT 
        entry.id as entry_id,
        entry.posting_date,
        entry.contract_for AS card_id,
        COALESCE(doc.trans_date, m_transaction.posting_date) AS trans_date,
        doc.REQUEST_CATEGORY,
        TRIM(CAST(
            CASE m_transaction.service_class
                WHEN 'T' THEN COALESCE(
                    SIGN(entry.amount) *
                    ABS(CASE
                        WHEN doc.REQUEST_CATEGORY = 'J' THEN m_transaction.trans_amount
                        ELSE doc.trans_amount
                    END),
                    entry.amount
                )
                WHEN 'M' THEN 
                    CASE WHEN doc.trans_curr = '' THEN entry.amount
                    ELSE SIGN(entry.amount) * ABS(doc.trans_amount)
                    END
                WHEN 'A' THEN COALESCE(
                    SIGN(entry.amount) * ABS(doc.trans_amount),
                    entry.amount
                )
                WHEN 'I' THEN 0
                ELSE COALESCE(
                    SIGN(entry.amount) * ABS(doc.trans_amount),
                    entry.amount
                )
            END AS string)
        ) AS trans_amount,
        CASE m_transaction.service_class
            WHEN 'u' THEN acc_curr.name
            ELSE COALESCE(tc.name, doc.trans_curr)
        END AS trans_curr,
        LTRIM(CAST(entry.fee_amount AS string)) AS fee_amount,
        LTRIM(CAST(entry.amount AS string)) AS acc_amount,
        0 AS end_balance,
        COALESCE(acc_curr.name, account.curr) AS acc_curr,
        SUBSTR(
            CONCAT(
                CASE m_transaction.Service_Class
                    WHEN 'T' THEN trans_subtype.NAME
                    WHEN 'M' THEN trans_subtype.NAME
                    WHEN 'A' THEN trans_subtype.NAME
                    ELSE m_transaction.trans_code
                END,
                ' ',
                rc.displ_col
            ),
            1, 32
        ) AS Tr_Type,
        doc.TRANS_COUNTRY,
        doc.TRANS_CITY,
        doc.trans_details,
        doc.sic_code AS mcc,
        doc.auth_code,
        doc.ret_ref_number,
        doc.source_reg_num,
        ma.account_name,
        ma.code,
        m_transaction.service_class,
        m_transaction.posting_db_date,
        CASE
            WHEN (doc.ADD_INFO LIKE '%POI_AMOUNT%' AND doc.ADD_INFO LIKE '%POI_CURR%')
                 OR (doc.ADD_INFO LIKE '%DCC_IND=Y%') THEN 'Y'
            ELSE 'N'
        END AS DCC
    FROM nf_way4.opt_acnt_contract_gp acnt_contract
    JOIN nf_way4.opt_client client ON client.id = acnt_contract.client__id
    LEFT JOIN nf_way4.serv_pack serv_pack ON serv_pack.id = acnt_contract.serv_pack__id
    LEFT JOIN nf_way4.acc_scheme acc_scheme ON acc_scheme.id = acnt_contract.acc_scheme__id
    LEFT JOIN nf_way4.contr_subtype contr_subtype ON contr_subtype.id = acnt_contract.contr_subtype__id
    LEFT JOIN nf_way4.currency currency ON currency.code = acnt_contract.curr AND currency.amnd_state = 'A'
    JOIN nf_way4.account account 
        ON account.acnt_contract__oid = acnt_contract.id
       AND account.oper_date_year = 2025               -- 🔒 Зафиксировать год
       AND account.oper_date_month = 7
       AND account.oper_date_day = 23
    JOIN nf_way4.account_type account_type_ ON account_type_.id = account.account_type
    JOIN nf_way4.item item ON item.account__oid = account.id
    JOIN nf_way4.entry entry 
        ON entry.item__id = item.id
       AND entry.oper_date_year = 2025                 -- 🔒 Зафиксировать год
       AND entry.oper_date_month = 7
       AND entry.oper_date_day = 23
    JOIN nf_way4.m_transaction m_transaction 
        ON m_transaction.id = entry.m_transaction__id
       AND m_transaction.oper_date_year = 2025         -- 🔒 Зафиксировать год
       AND m_transaction.oper_date_month = 7
       AND m_transaction.oper_date_day = 23
    LEFT JOIN nf_way4.opt_doc doc ON doc.id = m_transaction.doc__oid
    LEFT JOIN nf_way4.currency acc_curr ON acc_curr.code = account.curr AND acc_curr.amnd_state = 'A'
    LEFT JOIN nf_way4.currency doc_curr ON doc_curr.code = doc.trans_curr AND doc_curr.amnd_state = 'A'
    LEFT JOIN nf_way4.currency trans_curr ON trans_curr.code = m_transaction.trans_curr AND trans_curr.amnd_state = 'A'
    LEFT JOIN nf_way4.trans_subtype trans_subtype ON trans_subtype.id = m_transaction.trans_subtype
    LEFT JOIN rc ON rc.data_col = m_transaction.request_cat
    LEFT JOIN nf_way4.currency tc ON tc.code = doc.trans_curr AND tc.amnd_state = 'A'
    LEFT JOIN nf_way4.account ma ON m_transaction.target_account = ma.id
    WHERE (m_transaction.service_class NOT IN ('L', 'U') OR
          (m_transaction.service_class = 'u' AND
           m_transaction.trans_code LIKE 'uP%' AND
           ma.account_name LIKE 'Cl Unpaid%'))
      AND NOT (m_transaction.service_class IN ('d', 'D') AND
           m_transaction.trans_code <> 'D+')
      AND CASE WHEN m_transaction.service_class = 'D' THEN 'P' ELSE account.code END = account.code
      AND (account.Is_Am_Available = 'Y' OR
          account.code LIKE 'P%' OR account.code = 'F3')
      AND (account_type_.group_name <> 'TECHNICAL' OR
          (account_type_.group_name = 'TECHNICAL' AND
          account.code = 'P7'))
) t
WHERE (source_reg_num IS NULL OR
      (source_reg_num IS NOT NULL AND
      UPPER(source_reg_num) NOT LIKE 'WF_ARST%' AND
      UPPER(source_reg_num) NOT LIKE 'K2SL%')) 
  AND (TRANS_DETAILS IS NULL OR
      UPPER(TRANS_DETAILS) NOT LIKE 'WF_ARST%')
  AND NOT (REQUEST_CATEGORY <> 'R' AND service_class = 'A' AND
       code LIKE 'INST%')
  AND NOT (REQUEST_CATEGORY <> 'R' AND service_class = 'A' AND
       code IN ('L11', 'L12'))