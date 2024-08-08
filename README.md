# Linkedin-Data-Projects

This repo contains the associated code and selected data products for use with the Revellio Linkedin education and position datasets. See the list below for short descriptions of each code file:

- **_akm_merge_script.py_** : utilizes dask's distributed capacities to compile each of the .parquet position files into single dataframe that can then be used for the AKM model (this particular script works well on a local machine however stalls when run on the SSRDE SLURM cluster so is recommended to be used for local computations on samples of the data only).
    - _outputs_: locally stored composite dataframe - can be saved by adding the command _dd.to_csv(/path/to/folder/)_ or alternatives for .parquet, .xlsx, etc. file types

-  **_india_linkedin_pyspark_ym_** : utilizes pyspark to filter the data to only those individuals employed India. Matches individual work experience by user_id to generate work history for each individual for every year in which they were part of the labor market and aggregates the result by year-month pair, state of employment (in India) and the 2-digit onet code. 
    - _outputs_: aggregated .parquet files by year-month pair, state of employment (in India) and the 2-digit onet code
    - _note_: to alter this code to apply to labor in any country in the dataset, change line 50 to state
      ```
      india_df = india_df.filter(col('country') == <name_of_country>)
      ```
      where <name_of_country> is the desired country for the analysis

-   
