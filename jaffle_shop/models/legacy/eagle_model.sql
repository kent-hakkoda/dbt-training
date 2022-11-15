WITH MIN_DATE AS(
SELECT MIN(DATE::date) as min_date FROM "EAGLE_DB_ANALYTICS"."DEVELOPMENT"."EAGLE"
),
MAX_DATE AS(
SELECT MAX(DATE::date) as max_date FROM "EAGLE_DB_ANALYTICS"."DEVELOPMENT"."EAGLE")

, CTE AS(			
  --BASE STATEMENT
  SELECT 
       t1.DATE::date as DATE,
       t1.GCF,
       t1.HURDLE,
  
       --LP HURDLE
       CASE
         WHEN COALESCE(HURDLE,0) > 0
         THEN HURDLE
         ELSE 0
         END AS MAX_HURDLE,    
      CASE
          WHEN COALESCE(t1.GCF, 0) < COALESCE(MAX_HURDLE, 0)
          THEN t1.GCF
          ELSE MAX_HURDLE  END AS TO_LP_HURDLE_CALC,
        
       --CATCHUP
       0 AS CATCHUP_TO_GP,
       0 AS "80_TO_LP",
       0 AS "20_TO_GCP",
       TO_LP_HURDLE_CALC + "80_TO_LP" AS CURRENT_LP_DIST,
       TO_LP_HURDLE_CALC + "80_TO_LP" AS ROLLING_LP_DIST,
       CATCHUP_TO_GP + "20_TO_GCP" AS CURRENT_GP_DIST,
       TO_LP_HURDLE_CALC + "20_TO_GCP" AS ROLLING_GP_DIST

       
       FROM "EAGLE_DB_ANALYTICS"."DEVELOPMENT"."EAGLE" t1, min_date
       WHERE DATE = min_date  
			
UNION ALL
   
   SELECT
       t.DATE::date,
       t.GCF,
       t.HURDLE,
//  
       --LP HURDLE
       CASE
         WHEN COALESCE(t.HURDLE,0) > 0
         THEN t.HURDLE
         ELSE 0
         END AS R_MAX_HURDLE,    
      CASE
          WHEN COALESCE(t.GCF, 0) < COALESCE(R_MAX_HURDLE, 0)
          THEN t.GCF
          ELSE R_MAX_HURDLE END AS R_TO_LP_HURDLE_CALC,
        
       --CATCHUP
       GREATEST(0,LEAST(t.GCF - R_TO_LP_HURDLE_CALC,cte.ROLLING_LP_DIST * 0.2 / 0.8 - cte.ROLLING_GP_DIST)) AS R_CATCHUP_TO_GP,
       
       (t.GCF - R_TO_LP_HURDLE_CALC - R_CATCHUP_TO_GP)*8 AS "R_80_TO_LP",
       t.GCF - R_TO_LP_HURDLE_CALC - R_CATCHUP_TO_GP - "80_TO_LP" AS "R_20_TO_GCP",
       R_TO_LP_HURDLE_CALC + "R_80_TO_LP" AS R_CURRENT_LP_DIST,
       cte.ROLLING_LP_DIST + R_CURRENT_LP_DIST AS R_ROLLING_LP_DIST,
       CATCHUP_TO_GP + "R_20_TO_GCP" AS R_CURRENT_GP_DIST,
       cte.ROLLING_GP_DIST + R_CURRENT_GP_DIST AS R_ROLLING_GP_DIST
   FROM "EAGLE_DB_ANALYTICS"."DEVELOPMENT"."EAGLE" as t INNER JOIN CTE
    on t.date::date = last_day(dateadd(month,1,cte.date))
   join
   max_date 
  
    WHERE t.date != last_day(dateadd(month,1,max_date))
  
  )
    
    select
        DATE,
        GCF,
        HURDLE,
        TO_LP_HURDLE_CALC,
        CATCHUP_TO_GP,
        "80_TO_LP",
        "20_TO_GCP",
        CURRENT_LP_DIST,
        CURRENT_GP_DIST
        --,
        --ROLLING_LP_DIST,
        --ROLLING_GP_DIST
        
        from cte
        ORDER BY DATE ASC