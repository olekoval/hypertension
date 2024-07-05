SELECT edrpou, patient_id, adrg, principal_diagnosis
  FROM (SELECT edrpou, patient_id, starts, adrg, principal_diagnosis  
		  FROM analytics.rds_pmg_events_2023 AS e
	           JOIN analytics.rds_pmg_events_checks_2023 ch ON ch.id = e.id
		 WHERE principal_diagnosis IN ( 
					 'I10','I11.9','I11.0','I13.0','I13.2','I50.0', 'I50.1','I50.9','R57.9','R57.0',
					 'I12.0','I13.1','N18.4','N18.5','N19','R34', 'I12.9', 'G45.1','G45.2','G45.8', 
					 'G45.9','G46.0','G46.1', 'G46.2,I65.0','I65.1','I65.2','I65.3','I65.8','I65.9',
					 'G13.1', 'G45.4', 'G46.3', 'G46.4', 'G46.5', 'G46.6', 'G46.7', 'G46.8', 'I60.0', 
					 'I60.1', 'I60.2', 'I60.3', 'I60.4', 'I60.5', 'I60.6', 'I60.7', 'I60.8', 'I60.9', 
					 'I61.0', 'I61.1', 'I61.2', 'I61.3', 'I61.4', 'I61.5', 'I61.6', 'I61.8', 'I61.9', 
					 'I62.0', 'I62.1', 'I62.9', 'I63.0', 'I63.1', 'I63.2', 'I63.3', 'I63.4', 'I63.5', 
					 'I63.6', 'I63.8', 'I63.9', 'I64', 'I67.6', 'I66.0', 'I66.1', 'I66.2', 'I66.3', 
					 'I66.4', 'I66.8', 'I66.9', 'I67.0', 'I68.8')
		   AND class = 'INPATIENT'		 
		   AND is_correct
		   AND is_payment
		   AND adrg IN ('F67', 'F62', 'L60', 'L67', 'B69', 'B70')
				) ev
 WHERE EXISTS (
	           SELECT 1
		         FROM (
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
									 ) md
				WHERE md.patient_id = ev.patient_id
				  AND starts >= onset_date
	   )



