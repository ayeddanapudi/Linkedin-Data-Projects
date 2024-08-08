# -*- coding: utf-8 -*-
"""
Created on Mon Aug  5 09:12:39 2024

@author: ayedd
"""

import os
import subprocess
import sys
import time

try:
    import pyspark
except (ModuleNotFoundError, ImportError):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark"])


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, year, explode, array, lag, current_timestamp, expr, split, row_number, coalesce, current_date
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("LinkedIn Data Processing") \
    .config("spark.executor.memory", "64g") \
    .config("spark.driver.memory", "64g") \
    .config("spark.executor.cores", "10") \
    .config("spark.executor.instances", "16") \
    .config("spark.driver.maxResultSize", "8g") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()
    
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")


#%% Script
def main():
    base_path = "/labs/bharadwajlab/linkedin/"
    folder1 = "individual_position/"
    folder2 = 'individual_education/'
    parquet_files = os.listdir(base_path + folder1)
    parquet_files = [file for file in parquet_files if file.endswith('.parquet')]
    parquet_files = [base_path + folder1 + i for i in parquet_files]
    
    educ_files = os.listdir(base_path + folder2)
    educ_files = [file for file in educ_files if file.endswith('.parquet')]
    educ_files = [base_path + folder2 + i for i in educ_files]

    print(f"Found {len(parquet_files)} Parquet files", flush=True)
    
    print(f"Found {len(educ_files)} Education Parquet files", flush=True)

    print("Setting up Spark session...", flush=True)

    print("Spark session setup complete", flush=True)

    print("Reading in Parquet Files", flush=True)
    # Read Parquet files
    try:
        india_df = spark.read.parquet(*parquet_files)
        print("Line 1 done", flush=True)
        india_user_df = india_df.filter(col('country') == 'India')
        print("Line 2 done", flush=True)
        us_user_df = india_df.filter(col('country') == 'United States')
        print("Line 3 done", flush=True)
        migrant_users_df = india_user_df.join(us_user_df, on='user_id', how='inner')
        print("Line 4 done", flush=True)
        india_migrant_users = migrant_users_df.select('user_id').rdd.flatMap(lambda row: row).collect()
        print("Line 5 done", flush=True)
        
        del india_user_df, us_user_df
        print("Line 6 done", flush=True)
                
        india_df = india_df.filter(col('user_id').isin(india_migrant_users))
        print("Line 7 done", flush=True)
    except Exception as e:
        print(f"Error reading Parquet files: {e}", flush=True)
        sys.exit(1)
    
    india_educ_df = spark.read.parquet(*educ_files)
    india_educ_df = india_educ_df.filter(col('university_country') == 'India')
    india_educ_df = india_educ_df.withColumnRenamed('enddate', 'college_enddate')
    india_educ_df = india_educ_df.withColumn('college_enddate', to_date(col('college_enddate')))    
    india_educ_df = india_educ_df.select('user_id', 'college_enddate', 'university_location')
    window_spec = Window.partitionBy('user_id').orderBy(col('college_enddate').desc())  # Define a window spec partitioned by 'user_id' and ordered by 'enddate' descending
    india_educ_df = india_educ_df.withColumn('row_number', row_number().over(window_spec)) # Add a row number to each partition
    india_educ_df = india_educ_df.filter(col('row_number') == 1).drop('row_number') # Keep only the most recent 'enddate' for each 'user_id'
    
    india_df = india_df.join(india_educ_df, on = 'user_id', how = 'left')

    print("Finished reading Parquet Files", flush=True)

    print("Starting data processing...", flush=True)
    start = time.time()

    # Compute frame concat
    india_df = india_df.withColumn('start_date_dt', to_date(col('startdate')))
    india_df = india_df.withColumn('end_date_dt', to_date(col('enddate')))
    india_df = india_df.orderBy('user_id', 'start_date_dt')
    
    window_spec = Window.partitionBy('user_id').orderBy('start_date_dt')
    india_df = india_df.withColumn('previous_end_date_dt', lag('end_date_dt').over(window_spec))
    india_df = india_df.withColumn('start_date_dt', when(col('start_date_dt').isNull(), col('previous_end_date_dt')).otherwise(col('start_date_dt')))
    india_df = india_df.drop('previous_end_date_dt')
    
    india_df = india_df.withColumn('end_date_dt', when(col('end_date_dt').isNull(), current_date()).otherwise(col('end_date_dt')))
    
    india_df = india_df.withColumn('start_date_flag', when(col('start_date_dt').isNull(), 1).otherwise(0))
    # Update start_date_dt using college_enddate or 3 years before end_date_dt if both are null
    india_df = india_df.withColumn('start_date_dt', 
                                   when(col('start_date_dt').isNull(), 
                                        coalesce(col('college_enddate'), col('end_date_dt') - expr('INTERVAL 3 YEARS'))).otherwise(col('start_date_dt'))) 
    
    india_df = india_df.withColumn('start_year', year(col('start_date_dt')))
    india_df = india_df.withColumn('end_year', year(col('end_date_dt')))
    
    india_df = india_df.withColumn('year', expr('sequence(start_year, end_year - 1)'))
    india_df = india_df.withColumn('year', explode(col('year')))
    
    india_df = india_df.filter(col('country') == 'United States')
    
    #india_df = india_df.withColumn('onet_code_f2', split(col('onet_code'), '-').getItem(0))

    india_df.createOrReplaceTempView("india_df")
    
    result = spark.sql("""
        SELECT 
            onet_code, 
            university_location, 
            year,
            AVG(salary) AS mean_salary, 
            COUNT(salary) AS n_individuals
        FROM india_df
        GROUP BY onet_code, university_location, year
    """)
    
    end = time.time()
    time_elapsed = end - start
    print(f"Data Processing Elapsed Time: {time_elapsed:.2f} seconds", flush=True)

    # Save if needed
    #save_dir_files = os.listdir("/labs/bharadwajlab/ayeddana/")
    #if "india_migrats_educ_origin.csv" not in save_dir_files:
    #    india_df.write.csv("/labs/bharadwajlab/ayeddana/india_migrats_educ_origin.csv", header=True)
    
    print("Performing 3 way aggregation...", flush=True)
    
    result.write.csv("/labs/bharadwajlab/ayeddana/india_migrats_educ_origin.csv", header=True)

    print("Result saved to: /labs/bharadwajlab/ayeddana/", flush=True)
    
    spark.stop()
#%%

if __name__ == "__main__":
    main()
