# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 23:58:45 2024

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
    
    
spark = SparkSession.builder \
    .appName("LinkedIn Data Processing") \
    .config("spark.executor.memory", "64g") \
    .config("spark.driver.memory", "64g") \
    .config("spark.executor.cores", "10") \
    .config("spark.executor.instances", "16") \
    .config("spark.driver.maxResultSize", "8g") \
    .getOrCreate()

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")


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

    india_df.write.parquet("/labs/bharadwajlab/linkedin/india_migrant_position_df.parquet")
    spark.stop()
    
    
if __name__ == "__main__":
    main()