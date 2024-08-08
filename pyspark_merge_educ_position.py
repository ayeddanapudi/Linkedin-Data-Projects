# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 16:30:31 2024

@author: ayedd
"""

import os
import subprocess
import sys
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, year, explode, array, lag, current_timestamp, expr, split, row_number, coalesce
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
    .config("spark.ui.port", "4042").getOrCreate()
    

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
    india_df = spark.read.parquet(*parquet_files)
    india_df = india_df.filter(col('country') == 'India')
    
    india_educ_df = spark.read.parquet(*educ_files)
    india_educ_df = india_educ_df.filter(col('university_country') == 'India')
    india_educ_df = india_educ_df.withColumnRenamed('enddate', 'college_enddate')
    india_educ_df = india_educ_df.withColumn('college_enddate', to_date(col('college_enddate')))    
    india_educ_df = india_educ_df.select('user_id', 'college_enddate', 'university_location')
    window_spec = Window.partitionBy('user_id').orderBy(col('college_enddate').desc())  # Define a window spec partitioned by 'user_id' and ordered by 'enddate' descending
    india_educ_df = india_educ_df.withColumn('row_number', row_number().over(window_spec)) # Add a row number to each partition
    india_educ_df = india_educ_df.filter(col('row_number') == 1).drop('row_number') # Keep only the most recent 'enddate' for each 'user_id'
    
    india_df = india_df.join(india_educ_df, on = 'user_id', how = 'left')
    
    india_df.write.csv("/labs/bharadwajlab/ayeddana/india_position_educ.csv", header=True)
#%%
if __name__ == "__main__":
    main()