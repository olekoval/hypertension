SELECT patient_id, onset_date
  FROM (
        SELECT patient_id,
               onset_date,
               ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY onset_date ASC, inserted_at ASC) AS row_num
          FROM core.dim_med_conditions c
		 WHERE is_current = 'Y' 
		   AND verification_status = 'confirmed' 
		   AND EXTRACT(YEAR FROM onset_date) < 2024
	       AND EXISTS (SELECT 1
					     FROM core.dim_med_conditions_code_coding cc
					    WHERE cc.id = c.id
	                      AND code IN ('K86', 'K87')
                          AND kwd_system = 'eHealth/ICPC2/condition_codes'
                          AND is_current = 'Y')
        ) med
  WHERE row_num = 1
    AND EXISTS (
				SELECT 1
				  FROM core.dim_med_patients p
				 WHERE p.id = med.patient_id
				   AND is_current = 'Y' 
				   AND (status = 'active' OR EXTRACT(YEAR FROM death_date) > 2022))              