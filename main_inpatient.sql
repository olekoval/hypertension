WITH unid AS (
/*запит відбирає в групі унікального id, запис з самою ранньою датою
встановлення діагнозу - гіпертонія*/
    WITH idp AS (
    /* запит нумерує id пацієнта за датою встановлення діагнозу в кожній групі 
    унікального id пацієнта */
    SELECT p.id, 
           c.onset_date,
           ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY c.onset_date ASC) AS row_num 
      FROM core.dim_med_conditions AS c  
           INNER JOIN core.dim_med_conditions_code_coding AS cc ON c.id = cc.id
           INNER JOIN core.dim_med_patients AS p ON p.id = c.patient_id             
     WHERE cc.kwd_system = 'eHealth/ICPC2/condition_codes' AND cc.code IN ('K86', 'K87') AND p.is_current = 'Y'
           AND cc.is_current = 'Y' AND c.is_current = 'Y' AND (p.status = 'active' OR EXTRACT(YEAR FROM p.death_date) > 2022)  
           AND EXTRACT(YEAR FROM c.onset_date) < 2024
    )
SELECT id, onset_date -- де id це patient_id у таблицях events
  FROM idp
 WHERE row_num = 1   
)

SELECT v.registration_area,
       COUNT(DISTINCT e.patient_id) FILTER (WHERE ec.adrg = 'F67' AND e.principal_diagnosis IN('I10','I11.9')) AS F67,
       COUNT(DISTINCT e.patient_id) FILTER (WHERE ec.adrg = 'F62' AND e.principal_diagnosis IN('I11.0','I13.0','I13.2',
                                    'I50.0', 'I50.1','I50.9','R57.9','R57.0')) AS F62,
       COUNT(DISTINCT e.patient_id) FILTER (WHERE ec.adrg = 'L60' AND e.principal_diagnosis IN('I12.0','I13.1',
                                    'N18.4','N18.5','N19','R34')) AS L60,
       COUNT(DISTINCT e.patient_id) FILTER (WHERE ec.adrg = 'L67' AND e.principal_diagnosis = 'I12.9') AS L67,
       COUNT(DISTINCT e.patient_id) FILTER (WHERE ec.adrg = 'B69' AND e.principal_diagnosis 
                 IN('G45.1','G45.2','G45.8', 'G45.9','G46.0','G46.1', 'G46.2,I65.0','I65.1','I65.2','I65.3','I65.8','I65.9')) AS B69,
       COUNT(DISTINCT e.patient_id) FILTER (WHERE ec.adrg = 'B70' AND e.principal_diagnosis IN ('G13.1', 'G45.4', 'G46.3', 'G46.4', 'G46.5', 'G46.6', 'G46.7', 'G46.8', 'I60.0', 'I60.1', 'I60.2', 'I60.3', 'I60.4', 'I60.5', 'I60.6', 'I60.7', 'I60.8', 'I60.9', 'I61.0', 'I61.1', 'I61.2', 'I61.3', 'I61.4', 'I61.5', 'I61.6', 'I61.8', 'I61.9', 'I62.0', 'I62.1', 'I62.9', 'I63.0', 'I63.1', 'I63.2', 'I63.3', 'I63.4', 'I63.5', 'I63.6', 'I63.8', 'I63.9', 'I64', 'I67.6', 'I66.0', 'I66.1', 'I66.2', 'I66.3', 'I66.4', 'I66.8', 'I66.9', 'I67.0', 'I68.8')) AS B70
  FROM analytics.rds_pmg_events_2023 AS e
       INNER JOIN analytics.rds_pmg_events_checks_2023 AS ec ON e.id = ec.id 
       INNER JOIN unid ON unid.id = e.patient_id
       LEFT JOIN analytics.dwh_legal_entities_edrpou_view AS v ON e.edrpou = v.edrpou
 WHERE ec.is_correct AND ec.is_payment AND e.class = 'INPATIENT' AND e.starts >= unid.onset_date 
 GROUP BY v.registration_area 


