WITH all_id AS (
SELECT p.id, le.registration_area,
       ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY c.onset_date ASC, c.inserted_at ASC) AS row_num
  FROM core.dim_med_conditions AS c  
       INNER JOIN core.dim_med_conditions_code_coding AS cc ON c.id = cc.id
       INNER JOIN core.dim_med_patients AS p ON p.id = c.patient_id AND p.is_current = 'Y' 
       LEFT JOIN analytics.dwh_legal_entities_addresses_view AS le ON c.managing_organization_identifier_value = le.legal_entity_id
 WHERE cc.kwd_system = 'eHealth/ICPC2/condition_codes' AND c.verification_status = 'confirmed' AND cc.code IN ('K86', 'K87') AND cc.is_current = 'Y' AND c.is_current = 'Y' AND (p.status = 'active' OR EXTRACT(YEAR FROM p.death_date) > 2022) AND EXTRACT(YEAR FROM c.onset_date) < 2024)   

SELECT registration_area, COUNT(id) AS count_patients
  FROM all_id
 WHERE row_num = 1   
 GROUP BY registration_area 