# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 12:34:44 2024

@author: ayedd
"""

try:
    import pyspark
except (ModuleNotFoundError, ImportError):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark"])


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, when, first
from pyspark.sql import Window
from pyspark.sql.types import StringType
import os

spark = SparkSession.builder \
    .appName("LinkedIn Data Processing") \
    .config("spark.executor.memory", "64g") \
    .config("spark.driver.memory", "64g") \
    .config("spark.executor.cores", "10") \
    .config("spark.executor.instances", "16") \
    .config("spark.driver.maxResultSize", "64g") \
    .config("spark.ui.port", "4043") \
    .getOrCreate()

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

#%%
def main():
    base_path = "/labs/bharadwajlab/linkedin/"
    folder2 = 'individual_education/'
    
    educ_files = os.listdir(base_path + folder2)
    educ_files = [file for file in educ_files if file.endswith('.parquet')]
    educ_files = [base_path + folder2 + i for i in educ_files]
    
    print("loading educ dataframe")
    educ_df = spark.read.parquet(*educ_files)
    print("loaded educ dataframe")
        
    # Get unique degrees using DataFrame API
    degrees_df = educ_df.select('degree').distinct()
    degrees = [row['degree'] for row in degrees_df.collect()]
    
    print("define categories")
    # Define categories and map degrees to education levels
    categories = {'High School': 'High School', 'Associate': 'Associate', 'Bachelor': 'Bachelor'}
    
    print("Add any degrees not in categories as 'Post Graduate'")
    for degree in degrees:
        if degree not in categories:
            categories[degree] = 'Post Graduate'
    
    print("Create a UDF to map degrees to Educ_Level")
    def map_degree(degree):
        return categories.get(degree, 'Post Graduate')
    
    map_degree_udf = udf(map_degree, StringType())
    
    print("Apply the UDF to create the Educ_Level column")
    educ_df = educ_df.withColumn(
        'Educ_Level',
        map_degree_udf(col('degree'))
    )
    
    print("Create temporary DataFrame with new columns")
    educ_df_temp = educ_df.withColumn(
        'high_school_location_raw',
        when(col('Educ_Level') == 'High School', col('university_location')).otherwise(lit(None))
    ).withColumn(
        'high_school_country',
        when(col('Educ_Level') == 'High School', col('university_country')).otherwise(lit(None))
    ).withColumn(
        'high_school_name',
        when(col('Educ_Level') == 'High School', col('university_name')).otherwise(lit(None))
    ).withColumn(
        'high_school_enddate',
        when(col('Educ_Level') == 'High School', col('enddate')).otherwise(lit(None))
    ).withColumn(
        'associates_location_raw',
        when(col('Educ_Level') == 'Associate', col('university_location')).otherwise(lit(None))
    ).withColumn(
        'associates_country',
        when(col('Educ_Level') == 'Associate', col('university_country')).otherwise(lit(None))
    ).withColumn(
        'associates_name',
        when(col('Educ_Level') == 'Associate', col('university_name')).otherwise(lit(None))
    ).withColumn(
        'associates_enddate',
        when(col('Educ_Level') == 'Associate', col('enddate')).otherwise(lit(None))
    ).withColumn(
        'college_location_raw',
        when(col('Educ_Level') == 'Bachelor', col('university_location')).otherwise(lit(None))
    ).withColumn(
        'college_country',
        when(col('Educ_Level') == 'Bachelor', col('university_country')).otherwise(lit(None))
    ).withColumn(
        'college_field',
        when(col('Educ_Level') == 'Bachelor', col('field')).otherwise(lit(None))
    ).withColumn(
        'college_name',
        when(col('Educ_Level') == 'Bachelor', col('university_name')).otherwise(lit(None))
    ).withColumn(
        'college_enddate',
        when(col('Educ_Level') == 'Bachelor', col('enddate')).otherwise(lit(None))
    ).withColumn(
        'post_grad_location_raw',
        when(col('Educ_Level') == 'Post Graduate', col('university_location')).otherwise(lit(None))
    ).withColumn(
        'post_grad_country',
        when(col('Educ_Level') == 'Post Graduate', col('university_country')).otherwise(lit(None))
    ).withColumn(
        'post_grad_field',
        when(col('Educ_Level') == 'Post Graduate', col('field')).otherwise(lit(None))
    ).withColumn(
        'post_grad_name',
        when(col('Educ_Level') == 'Post Graduate', col('university_name')).otherwise(lit(None))
    ).withColumn(
        'post_grad_enddate',
        when(col('Educ_Level') == 'Post Graduate', col('enddate')).otherwise(lit(None))
    )
    
    print("Select relevant columns")
    educ_df_temp = educ_df_temp.select(
        'user_id', 'enddate', 'Educ_Level', 'high_school_location_raw', 'high_school_country', 'high_school_name', 'high_school_enddate',
        'associates_location_raw', 'associates_country', 'associates_name', 'associates_enddate', 'college_location_raw', 'college_country', 
        'college_field', 'college_name', 'college_enddate','post_grad_location_raw', 'post_grad_country', 'post_grad_field', 'post_grad_name', 
        'post_grad_enddate'
    )
    
    print("Define a window specification for grouping by user_id")
    window_spec = Window.partitionBy('user_id').orderBy(col('enddate').desc())
    
    print("Aggregate to get the first non-null values")
    collapsed_df = educ_df_temp.groupBy('user_id').agg(
        first(col('high_school_location_raw'), ignorenulls=True).alias('high_school_location_raw'),
        first(col('high_school_country'), ignorenulls=True).alias('high_school_country'),
        first(col('high_school_name'), ignorenulls=True).alias('high_school_name'),
        first(col('high_school_enddate'), ignorenulls=True).alias('high_school_enddate'),
        first(col('associates_location_raw'), ignorenulls=True).alias('associates_location_raw'),
        first(col('associates_country'), ignorenulls=True).alias('associates_country'),
        first(col('associates_name'), ignorenulls=True).alias('associates_name'),
        first(col('associates_enddate'), ignorenulls=True).alias('associates_enddate'),
        first(col('college_location_raw'), ignorenulls=True).alias('college_location_raw'),
        first(col('college_country'), ignorenulls=True).alias('college_country'),
        first(col('college_field'), ignorenulls=True).alias('college_field'),
        first(col('college_name'), ignorenulls=True).alias('college_name'),
        first(col('college_enddate'), ignorenulls=True).alias('college_enddate'),
        first(col('post_grad_location_raw'), ignorenulls=True).alias('post_grad_location_raw'),
        first(col('post_grad_country'), ignorenulls=True).alias('post_grad_country'),
        first(col('post_grad_field'), ignorenulls=True).alias('post_grad_field'),
        first(col('post_grad_name'), ignorenulls=True).alias('post_grad_name'),
        first(col('post_grad_enddate'), ignorenulls=True).alias('post_grad_enddate')
    )

    columns = collapsed_df.columns
    collapsed_df = collapsed_df.select([when(col(c) == "empty", None).otherwise(col(c)).alias(c) for c in columns])
    collapsed_df.write.parquet("/labs/bharadwajlab/ayeddana/user_id_education_wide(1).parquet")

#%%
if __name__ == '__main__':
    main()
