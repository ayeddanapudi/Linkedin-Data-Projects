# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 10:09:32 2024

@author: ayedd
"""

import os
import subprocess
import sys
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, year, explode, array, lag, current_timestamp, expr
from pyspark.sql.window import Window

try:
    import pyspark
except (ModuleNotFoundError, ImportError):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark"])

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LinkedIn Data Processing") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "4") \
    .getOrCreate()

#%% Script
def main():
    base_path = "/labs/bharadwajlab/linkedin/"
    folder1 = "individual_position/"
    parquet_files = os.listdir(base_path + folder1)
    parquet_files = [file for file in parquet_files if file.endswith('.parquet')]
    parquet_files = [base_path + folder1 + i for i in parquet_files]

    print(f"Found {len(parquet_files)} Parquet files", flush=True)

    print("Setting up Spark session...", flush=True)

    print("Spark session setup complete", flush=True)

    print("Reading in Parquet Files", flush=True)
    # Read Parquet files
    india_df = spark.read.parquet(*parquet_files)
    india_df = india_df.filter(col('country') == 'India')

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
    
    india_df = india_df.withColumn('end_date_dt', when(col('end_date_dt').isNull(), current_timestamp()).otherwise(col('end_date_dt')))
    
    india_df = india_df.withColumn('start_date_flag', when(col('start_date_dt').isNull(), 1).otherwise(0))
    india_df = india_df.withColumn('start_date_dt', when(col('start_date_dt').isNull(), col('end_date_dt') - expr('INTERVAL 3 YEARS')).otherwise(col('start_date_dt')))
    
    india_df = india_df.withColumn('start_year', year(col('start_date_dt')))
    india_df = india_df.withColumn('end_year', year(col('end_date_dt')))
    
    india_df = india_df.withColumn('year', expr('sequence(start_year, end_year - 1)'))
    india_df = india_df.withColumn('year', explode(col('year')))
    
    india_df.createOrReplaceTempView("india_df")
    
    result = spark.sql("""
        SELECT 
            onet_code, 
            state, 
            year, 
            AVG(salary) AS mean_salary, 
            COUNT(salary) AS n_individuals
        FROM india_df
        GROUP BY onet_code, state, year
    """)
    
    end = time.time()
    time_elapsed = end - start
    print(f"Data Processing Elapsed Time: {time_elapsed:.2f} seconds", flush=True)

    # Save if needed
    save_dir_files = os.listdir("/labs/bharadwajlab/ayeddana/")
    if "india_linkedin_1pct.csv" not in save_dir_files:
        india_df.write.csv("/labs/bharadwajlab/ayeddana/india_linkedin_1pct.csv", header=True)
    
    print("Performing 3 way aggregation...", flush=True)
    
    result.write.csv("/labs/bharadwajlab/ayeddana/india_collapsed_result_1pct.csv", header=True)

    print("Result saved to: /labs/bharadwajlab/ayeddana/", flush=True)
#%%
if __name__ == "__main__":
    main()
