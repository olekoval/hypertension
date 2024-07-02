SELECT patient_id, onset_date
  FROM (
        SELECT patient_id,
               onset_date,
               ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY c.onset_date ASC, c.inserted_at ASC) AS row_num
          FROM core.dim_med_conditions_code_coding cc
               JOIN core.dim_med_conditions c ON cc.id = c.id
         WHERE cc.code IN ('K86', 'K87')
           AND cc.kwd_system = 'eHealth/ICPC2/condition_codes'
           AND cc.is_current = 'Y'
           AND c.is_current = 'Y' 
           AND c.verification_status = 'confirmed' 
           AND EXTRACT(YEAR FROM c.onset_date) < 2024) med
  WHERE EXISTS (
        SELECT 1
          FROM core.dim_med_patients p
         WHERE p.id = med.patient_id
           AND p.is_current = 'Y' 
           AND (p.status = 'active' OR EXTRACT(YEAR FROM p.death_date) > 2022) 
  )           
 WHERE row_num = 1  