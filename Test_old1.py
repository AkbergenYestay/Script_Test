with tab as
       (SELECT ID
          FROM ACNT_CONTRACT
         WHERE AMND_STATE = 'A'
           and v_scheme LIKE '%MULTI CURR%'
           and v_concat = 'A'
           AND LIAB_CONTRACT = (SELECT LIAB_CONTRACT
                                  FROM ACNT_CONTRACT
                                 WHERE AMND_STATE = 'A'
                                   AND CON_CAT = 'A'
                                   AND ID = IN_CARDID)
        union all
        SELECT ID
          FROM ACNT_CONTRACT
         WHERE AMND_STATE = 'A'
           and v_scheme LIKE '%MULTI CURR%'
           and v_concat != 'A'
           AND CONTRACT_NUMBER IN (SELECT CONTRACT_NUMBER
                                     FROM ACNT_CONTRACT
                                    WHERE AMND_STATE = 'A'
                                      AND CON_CAT = 'C'
                                      AND ID = IN_CARDID)
        union all
        select IN_CARDID
          from dual
         where v_scheme not LIKE '%MULTI CURR%')
      SELECT posting_date,
             card_id,
             trans_date,
             trans_amount,
             trans_curr,
             fee_amount,
             acc_amount,
             end_balance,
             acc_curr,
             Sub_Card,
             tr_type,
             TRANS_COUNTRY,
             TRANS_CITY,
             trans_details,
             mcc,
             mcc_name,
             auth_code,
             ret_ref_number,
             DCC
        FROM (select /* + ORDERED FIRST_ROWS*/
               entry.id entry_id,
               entry.posting_date,
               entry.contract_for card_id,
               nvl(doc.trans_date, m_transaction.posting_date) trans_date,
               doc.REQUEST_CATEGORY,
               trim(to_char(decode(m_transaction.service_class,
                                   'T',
                                   nvl(sign(entry.amount) *
                                       abs(case
                                             when doc.REQUEST_CATEGORY = 'J' then
                                              m_transaction.trans_amount
                                             else
                                              doc.trans_amount
                                           end),
                                       entry.amount),
                                   'M',
                                   decode(doc.trans_curr,
                                          '',
                                          entry.amount,
                                          sign(entry.amount) *
                                          abs(doc.trans_amount)),
                                   'A',
                                   nvl(sign(entry.amount) *
                                       abs(doc.trans_amount),
                                       entry.amount),
                                   'I',
                                   0,
                                   nvl(sign(entry.amount) *
                                       abs(doc.trans_amount),
                                       entry.amount)),
                            '999999999999990.99')) trans_amount,
               Decode(m_transaction.service_class,
                      'u',
                      acc_curr.name,
                      nvl(tc.name, doc.trans_curr)) trans_curr,
               ltrim(to_char(entry.fee_amount, '999999999999990.99')) fee_amount,
               ltrim(to_char(entry.amount, '999999999999990.99')) acc_amount,
               0 end_balance,
               nvl(acc_curr.name, account.curr) acc_curr,
               substr(epay.e_prt.mask_number_e(decode(entry.contract_for,
                                                      doc.target_contract,
                                                      decode(trim(doc.target_number),
                                                             trim(v_CardNumber),
                                                             doc.target_number,
                                                             epay.CARD_NUM_BY_ID(entry.contract_for)),
                                                      decode(trim(doc.source_number),
                                                             trim(v_CardNumber),
                                                             '',
                                                             doc.source_number)),
                                               'C'),
                      1,
                      24) Sub_Card,
               substr(decode(m_transaction.Service_Class,
                             'T',
                             trans_subtype.NAME,
                             'M',
                             trans_subtype.NAME,
                             'A',
                             trans_subtype.NAME,
                             nvl((select min(name)
                                   from dict
                                  where amnd_State = 'A'
                                    and code = m_transaction.trans_code),
                                 m_transaction.trans_code)) || ' ' ||
                      (rc.displ_col),
                      1,
                      32) Tr_Type,
               doc.TRANS_COUNTRY,
               doc.TRANS_CITY,
               case
                 when m_transaction.Service_Class = 'u' then
                  'Погашение задолженности'
                 when m_transaction.Service_Class = 'I' then
                  'Начисленное вознаграждение'
                 when doc.trans_type = 40402 and doc.trans_details is null then
                  'Погашение кредита'
                 when doc.source_number = '98128471' and
                      doc.trans_type = 1403 then
                  substr('Перевод между своими картами' ||
                         decode(upper(loc.extract_stnd_tag(doc.add_info,
                                                           'CPNA')),
                                null,
                                null,
                                ':' ||
                                upper(loc.extract_stnd_tag(doc.add_info,
                                                           'CPNA'))),
                         1,
                         255)
                 when doc.source_number = '98128471' and
                      doc.trans_type = 33675 then
                  'Перевод между своими картами'
                 when doc.source_number in
                      ('98128110', '98128100', '98128104') and
                      doc.trans_type = 1403 then
                  substr('Перевод на карту' ||
                         decode(upper(loc.extract_stnd_tag(doc.add_info,
                                                           'CPNA')),
                                null,
                                null,
                                ':' ||
                                upper(loc.extract_stnd_tag(doc.add_info,
                                                           'CPNA'))),
                         1,
                         255)
                 when doc.source_number in
                      ('98128110', '98128100', '98128104') and
                      doc.trans_type = 33675 then
                  'Перевод на карту'
                 when doc.source_number in ('98128111', '98128108') and
                      doc.trans_type = 1403 then
                  substr('Перевод по номеру телефона' ||
                         decode(upper(loc.extract_stnd_tag(doc.add_info,
                                                           'CPNA')),
                                null,
                                null,
                                ':' ||
                                upper(loc.extract_stnd_tag(doc.add_info,
                                                           'CPNA'))),
                         1,
                         255)
                 when doc.source_number in ('98128111', '98128108') and
                      doc.trans_type = 33675 then
                  'Перевод по номеру телефона'
                 when doc.source_number in ('98128477') then
                  'Перевод по номеру телефона на карту другого банка РК'
                 when doc.source_number in ('98128101') then
                  'Перевод на зарубежную карту'
                 when doc.trans_type = 1403 and
                      upper(doc.trans_details) not like 'PAYPAL%' and
                      INSTR(upper(doc.trans_details),
                            upper(loc.extract_stnd_tag(doc.add_info, 'CPNA'))) = 0 AND
                      INSTR(upper(loc.extract_stnd_tag(doc.add_info, 'CPNA')),
                            upper(doc.trans_details)) = 0 then
                  SUBSTR(upper(doc.trans_details) || ':' ||
                         upper(loc.extract_stnd_tag(doc.add_info, 'CPNA')),
                         1,
                         255)
                 when doc.trans_type = 15 and doc.sic_code = '6012' and
                      doc.source_channel = 'P' then
                  SUBSTR(upper(doc.trans_details) ||
                         (select distinct ':' ||
                                          upper(KZ2RUS(c.last_nam || ' ' ||
                                                       c.first_nam || ' ' ||
                                                       c.father_s_nam)) fio
                            From doc d, acnt_contract a, client c
                           where d.amnd_state = 'A'
                             and d.source_channel = 'P'
                             and d.trans_type = 5
                             and d.is_authorization = 'N'
                             and d.ret_ref_number = doc.ret_ref_number
                             AND d.source_number = doc.source_number
                             AND EXISTS (SELECT 'X'
                                    FROM m_transaction mt
                                   WHERE mt.doc__oid = d.id)
                             and d.target_contract = a.id
                             and a.amnd_state = 'A'
                             and a.pcat = 'C'
                             and a.client__id = c.id
                             and c.amnd_state = 'A'),
                         1,
                         255)
                 when upper(doc.trans_details) like 'NAME=%' then
                  epay.loc.extract_stnd_tag(replace(upper(doc.trans_details),
                                                    '"',
                                                    ''),
                                            'NAME')
                 else
                  upper(doc.trans_details)
               end trans_details,
               doc.sic_code mcc,
               xwdoc('SIC_CODE', doc.sic_code) mcc_name,
               doc.auth_code,
               doc.ret_ref_number,
               doc.source_reg_num,
               ma.account_name,
               ma.code,
               m_transaction.service_class,
               m_transaction.posting_db_date,
               trim(epay.loc.extract_field(acnt_contract.add_info_04,
                                           'MIGREQ')) MIGREQ,
               CASE
                 WHEN (doc.ADD_INFO LIKE '%POI_AMOUNT%' AND
                      doc.ADD_INFO LIKE '%POI_CURR%') OR
                      (doc.ADD_INFO LIKE '%DCC_IND=Y%') THEN
                  'Y'
                 ELSE
                  'N'
               END DCC
                from tab,
                     acnt_contract,
                     client,
                     acc_scheme,
                     serv_pack,
                     contr_subtype,
                     currency,
                     account,
                     account_type,
                     item,
                     entry,
                     m_transaction,
                     doc,
                     currency acc_curr,
                     currency trans_curr,
                     currency doc_curr,
                     trans_subtype,
                     (select displ_col || ' ' displ_col, data_col
                        from v_listboxes
                       where name = 'Request Category'
                         and Data_Col <> 'P') rc,
                     currency tc,
                     account ma
               where tab.id = acnt_contract.id -- тут 
                 and (m_transaction.service_class not in ('L', 'U') and
                     (m_transaction.service_class <> 'u' or
                     (m_transaction.trans_code like 'uP%' and
                     ma.account_name like 'Cl Unpaid%')))
                 and not (m_transaction.service_class in ('d', 'D') and
                      m_transaction.trans_code <> 'D+')
                 and decode(m_transaction.service_class,
                            'D',
                            'P',
                            account.code) = account.code
                 and client.id = acnt_contract.client__id
                 and serv_pack.id(+) = acnt_contract.serv_pack__id
                 and acc_scheme.id(+) = acnt_contract.acc_scheme__id
                 and contr_subtype.id(+) = acnt_contract.contr_subtype__id
                 and currency.code(+) = acnt_contract.curr
                 and currency.amnd_state(+) = 'A'
                 and account.acnt_contract__oid = acnt_contract.id
                 and account_type.id = account.account_type
                 and (Account.Is_Am_Available = 'Y' or
                     account.code like 'P%' or account.code = 'F3')
                 and (account_type.group_name <> 'TECHNICAL' or
                     (account_type.group_name = 'TECHNICAL' and
                     account.code = 'P7'))
                 and item.account__oid = account.id
                 and entry.item__id = item.id
                 and entry.posting_date >= v_FromDate --to_date('19.03.2023', 'dd.mm.yyyy')
                 and entry.posting_date <= v_ToDate -- to_date('20.04.2023', 'dd.mm.yyyy')
                 and m_transaction.id = entry.m_transaction__id
                 and doc.id(+) = m_transaction.doc__oid
                 and acc_curr.code(+) = account.curr
                 and acc_curr.amnd_state(+) = 'A'
                 and doc_curr.code(+) = doc.trans_curr
                 and doc_curr.amnd_state(+) = 'A'
                 and trans_curr.code(+) = m_transaction.trans_curr
                 and trans_curr.amnd_state(+) = 'A'
                 and trans_subtype.id(+) = m_transaction.trans_subtype
                 and rc.data_col(+) = m_transaction.request_cat
                 and tc.code(+) = doc.trans_curr
                 and tc.amnd_state(+) = 'A'
                 and m_transaction.target_account = ma.id(+))
       WHERE (source_reg_num IS NULL OR
             (source_reg_num IS NOT NULL AND
             upper(source_reg_num) NOT LIKE 'WF_ARST%' AND
             upper(source_reg_num) NOT LIKE 'K2SL%')) 
         AND (TRANS_DETAILS IS NULL OR
             UPPER(TRANS_DETAILS) NOT LIKE 'WF_ARST%')
         AND NOT (REQUEST_CATEGORY <> 'R' and service_class = 'A' and
              code like 'INST%')
         AND NOT (REQUEST_CATEGORY <> 'R' and service_class = 'A' and
              code in ('L11', 'L12'))
         AND ((posting_db_date >
             to_date('07042018 18:00:00', 'ddmmyyyy hh24:mi:ss') and
             MIGREQ = 'Y') or nvl(MIGREQ, '-') <> 'Y')
       order by entry_id desc;
  
