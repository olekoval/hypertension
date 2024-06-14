SELECT registration_area, COUNT(id)
  FROM (
    SELECT DISTINCT ON (p.id) p.id, le.registration_area
      FROM core.dim_med_conditions AS c  
           INNER JOIN core.dim_med_conditions_code_coding AS cc ON c.id = cc.id
           AND cc.kwd_system = 'eHealth/ICPC2/condition_codes' AND cc.code IN ('K86', 'K87') 
           AND cc.is_current = 'Y' AND c.is_current = 'Y'
           INNER JOIN core.dim_med_patients AS p ON p.id = c.patient_id AND p.is_current = 'Y' 
           AND (p.status = 'active' OR EXTRACT(YEAR FROM p.death_date) > 2022)
           LEFT JOIN analytics.dwh_legal_entities_addresses_view AS le ON c.managing_organization_identifier_value = le.legal_entity_id
     ORDER BY p.id, c.onset_date , c.inserted_at
    ) AS tab
 GROUP BY registration_area	