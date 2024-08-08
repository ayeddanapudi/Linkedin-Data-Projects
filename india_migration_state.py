# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 15:34:53 2024

@author: ayedd
"""

import os
import subprocess
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_date, desc, when, year, month, concat_ws, explode, sequence, lag, current_timestamp, expr, split, current_date, row_number, last

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
        
    india_df = india_df.withColumn('start_year', year(col('start_date_dt')))
    india_df = india_df.withColumn('end_year', year(col('end_date_dt')))
    
    india_df = india_df.withColumn('year', expr('sequence(start_year, end_year - 1)'))
        
    us_df = india_df.filter(col('country') == 'United States') 
    india_only = india_df.filter(col('country') == 'India') 
    
    india_only = india_only.orderBy(['user_id', 'end_date_dt'])

    # Define window specification
    window_spec = Window.partitionBy('user_id').orderBy(desc('end_date_dt'))
    india_only = india_only.withColumn('last_location_in_india', last('state').over(window_spec))
    
    # Find the last location in India
    india_only = india_only.withColumn('rank', row_number().over(window_spec))
    india_only = india_only.filter(col('rank') == 1).select('user_id', 'state').withColumnRenamed('state', 'last_location_in_india')
    
    india_only = india_only.dropDuplicates(['user_id']).select('user_id', 'last_location_in_india')

    us_df = us_df.join(india_only, on='user_id', how='left')

    us_df = us_df.withColumn('year', explode(col('year')))
    
    us_df = us_df.filter(col('end_year') >= col('start_year'))
   
    us_df.createOrReplaceTempView("us_df")

    result = spark.sql("""
        SELECT 
            onet_code,
            last_location_in_india, 
            year, 
            AVG(salary) AS mean_salary, 
            COUNT(salary) AS n_individuals
        FROM us_df
        GROUP BY onet_code, last_location_in_india, year
    """)
        
    end = time.time()
    time_elapsed = end - start
    print(f"Data Processing Elapsed Time: {time_elapsed:.2f} seconds", flush=True)

    save_dir = "/labs/bharadwajlab/ayeddana/"
    if not os.path.exists(save_dir):
        print(f"Save directory does not exist: {save_dir}", flush=True)
        sys.exit(1)

    print("Performing 3 way aggregation...", flush=True)
    result_file = "india_migration_state_origin.csv"
    result_file_path = os.path.join(save_dir, result_file)
    pandas_result = result.toPandas()
    pandas_result.to_csv(result_file_path, header=True)

    print(f"Result saved to: {result_file_path}", flush=True)
    spark.stop()
#%%
if __name__ == "__main__":
    main()
