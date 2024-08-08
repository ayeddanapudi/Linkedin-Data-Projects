# Linkedin-Data-Projects

This repo contains the associated code and selected data products for use with the Revellio Linkedin education and position datasets. See the list below for short descriptions of each code file:

## Code written to use Dask
- **_akm_merge_script.py_** : compiles each of the .parquet position files into single dataframe that can then be used for the AKM model (this particular script works well on a local machine however stalls when run on the SSRDE SLURM cluster so is recommended to be used for local computations on samples of the data only).
    - _outputs_: locally stored composite dataframe - can be saved by adding the command _dd.to_csv(/path/to/folder/)_ or alternatives for .parquet, .xlsx, etc. file types

## Code written to use Pyspark
-  **_india_linkedin_pyspark_ym.py_** : filters the data to only those individuals employed India. Matches individual work experience by user_id to generate work history for each individual for every year in which they were part of the labor market and aggregates the result by year-month pair, state of employment (in India) and the 2-digit onet code. 
    - _outputs_: aggregated .parquet files of average salary and the number of individuals by year-month pair, state of employment (in India) and the 2-digit onet code
    - _note_: to alter this code to apply to labor in any country in the dataset, change line 50 to state
      ```
      india_df = india_df.filter(col('country') == <name_of_country>)
      ```
      where _<name_of_country>_ is the desired country for the analysis

-   **_india_migration_educ.py_** : Considers only migrants to the US, from India, by user_id. Migrants are identified, by user_id, as any individual who appears in the Linkedin dataset in both India and later the US. The working dataset for this code uses identified user_id's of migrants to construct an India-US migrant dataset. Similar to the **_india_linkedin_pyspark_ym.py_** script, individual work experience, for Indian migrants within the US, is matched by user_id to generate work history for each individual in every year in which they were pat of the labor market using Linkedin job position data. This is merged with Linkedin education data to estimate when individuals entered the labor market. Code aggregates the epanded migrant dataset by 6-digit onet code, location of the university and year
      - _outputs_ : aggregated .csv files of average salary and the number of individuals by 6-digit onet code, location of university and year. Note that university location is not in terms of state or country but rather scraped strings from the data
      - _note_: to alter this code to apply to migrant labor from any country in the dataset to the US, change line 63 to state
        ```
        india_user_df = india_df.filter(col('country') == <name_of_origin_country>)
        ```
        and line 82 to state
        ```
        india_educ_df = india_educ_df.filter(col('university_country') == <name_of_origin_country>)
        ```

        where _<name_of_origin_country>_ is the desired country of origin of the migrants. To alter this code to apply to migrant labor to a desired destination country, change line 65 to state
        ```
        us_user_df = india_df.filter(col('country') == <name_of_destination_country>)
        ```
        and line 121 to state
        ```
        india_df = india_df.filter(col('country') == <name_of_destination_country>)
        ```
        where _<name_of_destination_country>_ is the desired destination country of the migrants. Note also that line 34 is essential to ensure that pyspark will read the education dates as is without generating an ancient date error

-   **_india_migration_educ_pre1990.py_**: Does the same as **_india_migration_educ.py_** with the only difference being that it aggregates the data for those individuals who completed their college education prior to Jan. 1st 1990.
    - _outputs_: aggregated .csv files of average salary and the number of individuals by 6-digit onet code, location of university and year. Note that university location is not in terms of state or country but rather scraped strings from the data (pre-1990 educated labor only)
    - _notes_: to change the time cutoff for labor considered, alter line 90 to state
      ```
      india_educ_df = india_educ_df.filter(col('college_enddate') < <desired_date>)
      ```
      where _<desired_date>_ takes the form a "YYYY-MM-DD" string (i.e. "1970-12-09", "1971-10-24, "2000-10-04", etc.). Note also that line 34 is essential to ensure that pyspark will read the education dates as is without generating an ancient date error  
      
-   
