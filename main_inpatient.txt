WITH unid AS (
/*запит відбирає в групі унікального id запис з самою ранньою датою
встановлення діагнозу - гіпертонія*/
    WITH idp AS (
    /* запит нумерує id пацієнта за датою встановлення діагнозу в кожній групі 
    унікального id пацієнта */
    SELECT p.id,
           c.onset_date,
           ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY c.onset_date ASC) AS row_num 
      FROM core.dim_med_conditions AS c  
           INNER JOIN core.dim_med_conditions_code_coding AS cc ON c.id = cc.id
           AND cc.kwd_system = 'eHealth/ICPC2/condition_codes' AND cc.code IN ('K86', 'K87') 
           AND cc.is_current = 'Y' AND c.is_current = 'Y'
           INNER JOIN core.dim_med_patients AS p ON p.id = c.patient_id AND p.is_current = 'Y' 
           AND (p.status = 'active' OR EXTRACT(YEAR FROM p.death_date) > 2022)
    )
SELECT id, onset_date
  FROM idp
 WHERE row_num = 1   
)

SELECT *
  FROM unid 