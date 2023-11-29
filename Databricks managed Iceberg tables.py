# Databricks notebook source
dbutils.fs.ls('/FileStore/tables/nyc_taxi_csv')

# COMMAND ----------

# DBTITLE 1,Delete the existing files
# dbutils.fs.rm('/FileStore/tables/nyc_taxi_csv', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema default

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema dlt_loan_demo_gbell

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dbt_glue_db 
# MAGIC     COMMENT 'This is schema/database for DBT-Glue' 
# MAGIC     LOCATION 'dbfs:/user/hive/warehouse/dbt_glue.db'
# MAGIC     WITH DBPROPERTIES (Author='lchawathe', Usage='Dev/Test');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema dbt_glue_db

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbt_glue_db.nyc_taxi_csv limit 5;

# COMMAND ----------

csv_s3_path = 's3://rvo-prevention-coaching-app-stage-us-east-1/testing/dbt-glue-iceberg/csv/nyc_taxi.csv'

# COMMAND ----------

# DBTITLE 1,Copy the file from DBFS to S3
dbutils.fs.cp('dbfs:/FileStore/tables/nyc_taxi_csv/nyc_taxi.csv', csv_s3_path)

# COMMAND ----------

# DBTITLE 1,Verify file on S3
dbutils.fs.ls(csv_s3_path)

# COMMAND ----------

# DBTITLE 1,Create folder to store AWS Athena results
athena_results_s3_path = 's3://rvo-prevention-coaching-app-stage-us-east-1/testing/athena_results/'
dbutils.fs.mkdirs(athena_results_s3_path)

# COMMAND ----------

# DBTITLE 1,Confirm s3 folder for Athena results
dbutils.fs.ls(athena_results_s3_path)

# COMMAND ----------

# DBTITLE 1,Create External table (with underlying CSV data from S3)
# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS
# MAGIC   dbt_glue_db.nyc_taxi_csv_on_s3
# MAGIC   (
# MAGIC         vendorid bigint,
# MAGIC         tpep_pickup_datetime timestamp,
# MAGIC         tpep_dropoff_datetime timestamp,
# MAGIC         passenger_count double,
# MAGIC         trip_distance double,
# MAGIC         ratecodeid double,
# MAGIC         store_and_fwd_flag string,
# MAGIC         pulocationid bigint,
# MAGIC         dolocationid bigint,
# MAGIC         payment_type bigint,
# MAGIC         fare_amount double,
# MAGIC         extra double,
# MAGIC         mta_tax double,
# MAGIC         tip_amount double,
# MAGIC         tolls_amount double,
# MAGIC         improvement_surcharge double,
# MAGIC         total_amount double,
# MAGIC         congestion_surcharge double,
# MAGIC         airport_fee double
# MAGIC   )
# MAGIC   ROW FORMAT DELIMITED
# MAGIC   FIELDS TERMINATED BY ','
# MAGIC   STORED AS TEXTFILE
# MAGIC   LOCATION 's3://rvo-prevention-coaching-app-stage-us-east-1/testing/dbt-glue-iceberg/csv/';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dbt_glue_db.nyc_taxi_csv_on_s3 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select day(tpep_pickup_datetime) as PICKUP_DAY, count(*) as PICKUP_COUNT from dbt_glue_db.nyc_taxi_csv_on_s3 group by day(tpep_pickup_datetime) order by 1;

# COMMAND ----------

iceberg_table_base_s3_path = 's3://rvo-prevention-coaching-app-stage-us-east-1/testing/dbt-glue-iceberg/nyc_taxi_iceberg/'
dbutils.fs.rm(iceberg_table_base_s3_path, True)

print(f'Cleaned Iceberg table from {iceberg_table_base_s3_path}')

# COMMAND ----------

# DBTITLE 1,Create Iceberg table
# MAGIC %sql
# MAGIC /*
# MAGIC -- Delta does not support expression in partitionby
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC   dbt_glue_db.nyc_taxi_iceberg 
# MAGIC   (
# MAGIC         vendorid bigint,
# MAGIC         tpep_pickup_datetime timestamp,
# MAGIC         tpep_dropoff_datetime timestamp,
# MAGIC         passenger_count double,
# MAGIC         trip_distance double,
# MAGIC         ratecodeid double,
# MAGIC         store_and_fwd_flag string,
# MAGIC         pulocationid bigint,
# MAGIC         dolocationid bigint,
# MAGIC         payment_type bigint,
# MAGIC         fare_amount double,
# MAGIC         extra double,
# MAGIC         mta_tax double,
# MAGIC         tip_amount double,
# MAGIC         tolls_amount double,
# MAGIC         improvement_surcharge double,
# MAGIC         total_amount double,
# MAGIC         congestion_surcharge double,
# MAGIC         airport_fee double
# MAGIC   )
# MAGIC   PARTITIONED BY (day(tpep_pickup_datetime))
# MAGIC   LOCATION 's3://rvo-prevention-coaching-app-stage-us-east-1/testing/dbt-glue-iceberg/nyc_taxi_iceberg/'
# MAGIC   TBLPROPERTIES ( 'table_type' ='ICEBERG'  );
# MAGIC */
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC   dbt_glue_db.nyc_taxi_iceberg 
# MAGIC   (
# MAGIC         vendorid bigint,
# MAGIC         tpep_pickup_datetime timestamp,
# MAGIC         tpep_dropoff_datetime timestamp,
# MAGIC         passenger_count double,
# MAGIC         trip_distance double,
# MAGIC         ratecodeid double,
# MAGIC         store_and_fwd_flag string,
# MAGIC         pulocationid bigint,
# MAGIC         dolocationid bigint,
# MAGIC         payment_type bigint,
# MAGIC         fare_amount double,
# MAGIC         extra double,
# MAGIC         mta_tax double,
# MAGIC         tip_amount double,
# MAGIC         tolls_amount double,
# MAGIC         improvement_surcharge double,
# MAGIC         total_amount double,
# MAGIC         congestion_surcharge double,
# MAGIC         airport_fee double
# MAGIC   )
# MAGIC   LOCATION 's3://rvo-prevention-coaching-app-stage-us-east-1/testing/dbt-glue-iceberg/nyc_taxi_iceberg/'
# MAGIC   TBLPROPERTIES ( 'table_type' ='ICEBERG'  );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dbt_glue_db.nyc_taxi_iceberg
# MAGIC   SELECT * FROM dbt_glue_db.nyc_taxi_csv_on_s3;

# COMMAND ----------

# DBTITLE 1,Confirm loading of Iceberg table
iceberg_load_df = spark.sql('select * from dbt_glue_db.nyc_taxi_iceberg')
display(iceberg_load_df.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbt_glue_db.nyc_taxi_iceberg WHERE day(tpep_pickup_datetime) =  5 limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- add pickup_day column to represent day(tpep_pickup_datetime)
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC   dbt_glue_db.nyc_taxi_iceberg_data_manipulation 
# MAGIC   (
# MAGIC         vendorid bigint,
# MAGIC         pickup_day INT,
# MAGIC         tpep_pickup_datetime timestamp,
# MAGIC         tpep_dropoff_datetime timestamp,
# MAGIC         passenger_count double,
# MAGIC         trip_distance double,
# MAGIC         ratecodeid double,
# MAGIC         store_and_fwd_flag string,
# MAGIC         pulocationid bigint,
# MAGIC         dolocationid bigint,
# MAGIC         payment_type bigint,
# MAGIC         fare_amount double,
# MAGIC         extra double,
# MAGIC         mta_tax double,
# MAGIC         tip_amount double,
# MAGIC         tolls_amount double,
# MAGIC         improvement_surcharge double,
# MAGIC         total_amount double,
# MAGIC         congestion_surcharge double,
# MAGIC         airport_fee double
# MAGIC   )
# MAGIC   PARTITIONED BY (pickup_day)
# MAGIC   LOCATION 's3://rvo-prevention-coaching-app-stage-us-east-1/testing/dbt-glue-iceberg/nyc_taxi_iceberg_data_manipulation/'
# MAGIC   TBLPROPERTIES ( 'table_type' ='ICEBERG'  );

# COMMAND ----------

# DBTITLE 1,Insert into another Iceberg table for data manipulation
# MAGIC %sql
# MAGIC INSERT INTO dbt_glue_db.nyc_taxi_iceberg_data_manipulation
# MAGIC     SELECT
# MAGIC         vendorid,
# MAGIC         day(tpep_pickup_datetime) as pickup_day,
# MAGIC         tpep_pickup_datetime,
# MAGIC         tpep_dropoff_datetime,
# MAGIC         passenger_count,
# MAGIC         trip_distance,
# MAGIC         ratecodeid,
# MAGIC         store_and_fwd_flag,
# MAGIC         pulocationid,
# MAGIC         dolocationid,
# MAGIC         payment_type,
# MAGIC         fare_amount,
# MAGIC         extra,
# MAGIC         mta_tax,
# MAGIC         tip_amount,
# MAGIC         tolls_amount,
# MAGIC         improvement_surcharge,
# MAGIC         total_amount,
# MAGIC         congestion_surcharge,
# MAGIC         airport_fee
# MAGIC     FROM dbt_glue_db.nyc_taxi_iceberg;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check query from table that supports partitioning
# MAGIC SELECT * FROM dbt_glue_db.nyc_taxi_iceberg_data_manipulation WHERE pickup_day =  5 limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 161 records with passenger_count: Null or 0
# MAGIC -- 2991 records with passenger_count = 1
# MAGIC SELECT passenger_count, count(*) as trips 
# MAGIC FROM dbt_glue_db.nyc_taxi_iceberg_data_manipulation 
# MAGIC WHERE (vendorid = 2 and year(tpep_pickup_datetime) = 2022 and month(tpep_pickup_datetime) = 1 and pickup_day = 31)
# MAGIC GROUP BY passenger_count
# MAGIC ORDER BY passenger_count ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update the passenger_count to fill NULL or 0
# MAGIC UPDATE dbt_glue_db.nyc_taxi_iceberg_data_manipulation 
# MAGIC SET passenger_count = 1
# MAGIC WHERE (vendorid = 2 and year(tpep_pickup_datetime) = 2022 and month(tpep_pickup_datetime) = 1 and pickup_day = 31) AND (passenger_count is NULL OR passenger_count = 0)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- confirm the updates
# MAGIC -- 3152 (2991 + 160 + 1) records with passenger_count = 1
# MAGIC SELECT passenger_count, count(*) as trips 
# MAGIC FROM dbt_glue_db.nyc_taxi_iceberg_data_manipulation 
# MAGIC WHERE (vendorid = 2 and year(tpep_pickup_datetime) = 2022 and month(tpep_pickup_datetime) = 1 and pickup_day = 31)
# MAGIC GROUP BY passenger_count
# MAGIC ORDER BY passenger_count ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Earliest time is 2023-11-29 02:41:54
# MAGIC SELECT * FROM dbt_glue_db.nyc_taxi_iceberg_data_manipulation FOR SYSTEM_TIME AS OF TIMESTAMP '2023-11-29 03:00:00' WHERE vendorid = 2 and year(tpep_pickup_datetime)= 2022 limit 10; 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe FORMATTED dbt_glue_db.nyc_taxi_iceberg_data_manipulation;

# COMMAND ----------

# MAGIC %sql
# MAGIC /* 
# MAGIC -- Error: UC_NOT_ENABLED
# MAGIC select
# MAGIC     h.made_current_at,
# MAGIC     s.operation,
# MAGIC     h.snapshot_id,
# MAGIC     h.is_current_ancestor,
# MAGIC     s.summary['spark.app.id']
# MAGIC from dbt_glue_db.nyc_taxi_iceberg_data_manipulation.history h
# MAGIC join dbt_glue_db.nyc_taxi_iceberg_data_manipulation.snapshots s
# MAGIC   on h.snapshot_id = s.snapshot_id
# MAGIC order by made_current_at
# MAGIC */

# COMMAND ----------

modified_iceberg_table_s3_path = 's3://rvo-prevention-coaching-app-stage-us-east-1/testing/dbt-glue-iceberg/nyc_taxi_iceberg_data_manipulation/'

#
# Looks like the table is NOT Iceberg, but just Delta
#

# iceberg_df = spark.read.format("iceberg").load(modified_iceberg_table_s3_path)
iceberg_df = spark.read.format("delta").load(modified_iceberg_table_s3_path)

iceberg_df.printSchema()


