DECLARE fields STRING;
DECLARE updates STRING;
DECLARE fields_hist STRING;
DECLARE fields_hist_value STRING;
EXECUTE IMMEDIATE (
     "SELECT STRING_AGG(column_name) FROM `airflow-341215.af_data_warehouse`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'crime_data_curr'"
  ) INTO fields;
EXECUTE IMMEDIATE (
    """WITH t AS (SELECT column_name FROM `airflow-341215.af_data_warehouse`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'crime_data_curr')
       SELECT STRING_AGG("t."||column_name ||" = "|| "s."||column_name) from t join t as s using(column_name)"""
  ) INTO updates;


  EXECUTE IMMEDIATE """
  MERGE `airflow-341215.af_data_warehouse.crime_data_curr` T
  USING `airflow-341215.af_data_lake.crime_data` S
    ON T.id = S.id
  WHEN MATCHED and s.updated_on > t.updated_on THEN 
    UPDATE SET """||updates||"""
  WHEN NOT MATCHED THEN
    INSERT ("""||fields||""") VALUES ("""||fields||""")""";


-- Hist Table 
-- Stage 1, to insert new record and expire existing record.
EXECUTE IMMEDIATE (
     "SELECT STRING_AGG(column_name) FROM `airflow-341215.af_data_warehouse`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'crime_data_curr'"
  ) INTO fields;
EXECUTE IMMEDIATE (
    """WITH t AS (SELECT column_name FROM `airflow-341215.af_data_warehouse`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'crime_data_curr')
       SELECT STRING_AGG("t."||column_name ||" = "|| "s."||column_name) from t join t as s using(column_name)"""
  ) INTO updates;

set fields_hist = concat(fields,",is_active,record_expr_date");

set fields_hist_value = concat(fields,",True,'2099-12-31'");
  EXECUTE IMMEDIATE """
  MERGE `airflow-341215.af_data_warehouse.crime_data_hist` T
  USING `airflow-341215.af_data_warehouse.crime_data_curr` S
    ON T.id = S.id
  WHEN MATCHED and s.updated_on > t.updated_on THEN 
    UPDATE SET record_expr_date = DATETIME_SUB(S.updated_on,INTERVAL 1 SECOND),
              is_active = False
  WHEN NOT MATCHED THEN
    INSERT ("""||fields_hist||""") VALUES ("""||fields_hist_value||""")""";

    -- Stage 2
EXECUTE IMMEDIATE (
     "SELECT STRING_AGG(column_name) FROM `airflow-341215.af_data_warehouse`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'crime_data_curr'"
  ) INTO fields;
EXECUTE IMMEDIATE (
    """WITH t AS (SELECT column_name FROM `airflow-341215.af_data_warehouse`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'crime_data_curr')
       SELECT STRING_AGG("t."||column_name ||" = "|| "s."||column_name) from t join t as s using(column_name)"""
  ) INTO updates;

set fields_hist = concat(fields,",is_active,record_expr_date");

set fields_hist_value = concat(fields,",True,'2099-12-31'");
  EXECUTE IMMEDIATE """
  MERGE `airflow-341215.af_data_warehouse.crime_data_hist` T
  USING `airflow-341215.af_data_warehouse.crime_data_curr` S
    ON T.id = S.id and T.updated_on = S.updated_on
  WHEN NOT MATCHED THEN
    INSERT ("""||fields_hist||""") VALUES ("""||fields_hist_value||""")""";
