# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 10:07:32 2024

@author: ayedd
"""

import os
import subprocess
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_date, when, year, month, concat_ws, explode, sequence, lag, current_timestamp, expr, split, current_date

try:
    import pyspark
except (ModuleNotFoundError, ImportError):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark"])

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LinkedIn Data Processing") \
    .config("spark.executor.memory", "32g") \
    .config("spark.executor.cores", "10") \
    .config("spark.executor.instances", "16") \
    .getOrCreate()

def main():
    base_path = "/labs/bharadwajlab/linkedin/"
    folder1 = "individual_position/"
    parquet_dir = base_path + folder1

    if not os.path.isdir(parquet_dir):
        print(f"Directory does not exist: {parquet_dir}", flush=True)
        sys.exit(1)

    parquet_files = [file for file in os.listdir(parquet_dir) if file.endswith('.parquet')]
    parquet_files = [parquet_dir + file for file in parquet_files]

    print(f"Found {len(parquet_files)} Parquet files", flush=True)

    print("Setting up Spark session...", flush=True)

    print("Spark session setup complete", flush=True)

    print("Reading in Parquet Files", flush=True)
    try:
        india_df = spark.read.parquet(*parquet_files)
        india_df = india_df.filter(col('country') == 'India')
    except Exception as e:
        print(f"Error reading Parquet files: {e}", flush=True)
        sys.exit(1)

    print("Finished reading Parquet Files", flush=True)

    print("Starting data processing...", flush=True)
    start = time.time()

    india_df = india_df.withColumn('start_date_dt', to_date(col('startdate')))
    india_df = india_df.withColumn('end_date_dt', to_date(col('enddate')))
    india_df = india_df.orderBy('user_id', 'start_date_dt')
    
    window_spec = Window.partitionBy('user_id').orderBy('start_date_dt')
    india_df = india_df.withColumn('previous_end_date_dt', lag('end_date_dt').over(window_spec))
    india_df = india_df.withColumn('start_date_dt', when(col('start_date_dt').isNull(), col('previous_end_date_dt')).otherwise(col('start_date_dt')))
    india_df = india_df.drop('previous_end_date_dt')
    
    india_df = india_df.withColumn('end_date_dt', when(col('end_date_dt').isNull(), current_date()).otherwise(col('end_date_dt')))
        
    india_df = india_df.withColumn(
        'start_date_flag',
        when(col('start_date_dt').isNull(), 1).otherwise(0)
    )
    
    india_df = india_df.withColumn(
        'start_date_dt',
        when(
            col('start_date_dt').isNull(),
            expr('date_sub(end_date_dt, 1095)')  # 3 years * 365 days/year = 1095 days
        ).otherwise(col('start_date_dt'))
    )
        
    india_df = india_df.withColumn(
        'start_year_month',
        concat_ws('-', year(col('start_date_dt')), month(col('start_date_dt')))
    )
    india_df = india_df.withColumn(
        'end_year_month',
        concat_ws('-', year(col('end_date_dt')), month(col('end_date_dt')))
    )
        
    india_df = india_df.withColumn(
        'start_year_month',
        expr("to_date(start_year_month, 'yyyy-M')")
    )
    india_df = india_df.withColumn(
        'end_year_month',
        expr("to_date(end_year_month, 'yyyy-M')")
    )
        
    india_df = india_df.filter(col('end_year_month') >= col('start_year_month'))
        
    india_df = india_df.withColumn(
        'year_month',
        explode(sequence(col('start_year_month'), col('end_year_month'), expr('INTERVAL 1 MONTH')))
    )
    
    india_df = india_df.withColumn(
        'onet_code_f2',
        split(col('onet_code'), '-').getItem(0)
    )

    india_df.createOrReplaceTempView("india_df")

    result = spark.sql("""
        SELECT 
            onet_code_f2,
            state, 
            date_format(year_month, "yyyy-MM") as year_month, 
            AVG(salary) AS mean_salary, 
            COUNT(salary) AS n_individuals
        FROM india_df
        GROUP BY onet_code_f2, state, date_format(year_month, "yyyy-MM")
    """)
        
    end = time.time()
    time_elapsed = end - start
    print(f"Data Processing Elapsed Time: {time_elapsed:.2f} seconds", flush=True)

    save_dir = "/labs/bharadwajlab/ayeddana/"
    if not os.path.exists(save_dir):
        print(f"Save directory does not exist: {save_dir}", flush=True)
        sys.exit(1)

    #save_file = "india_linkedin_1pct_ym.csv"
    #save_file_path = os.path.join(save_dir, save_file)

    #save_dir_files = os.listdir(save_dir)
    #if save_file not in save_dir_files:
    #    df7.to_csv(save_file_path, header=True)
    #    print(f"Saved file: {save_file_path}", flush=True)

    print("Performing 3 way aggregation...", flush=True)
    result_file = "india_collapsed_result_1pct_ym.parquet"
    result_file_path = os.path.join(save_dir, result_file)
    result.write.parquet(result_file_path)

    print(f"Result saved to: {result_file_path}", flush=True)
    spark.stop()

#%%
if __name__ == "__main__":
    main()