# Linkedin Data Projects

This repo contains the associated code and selected data products for use with the Revellio Linkedin education and position datasets. See the list below for short descriptions of each code file. To match package versions to this code, use the _requirements.txt_ file as follows:
```
conda activate <environment_name>

pip install -r requirements.txt
```
where _<environment_name>_ is the name of your python environment. 


See the _Bash Scripts_ folder for a template bash (.sh) script that can be used with the attached .py files in this repository. Comments on the template detail the user specifics that need to be adjusted for personal use. 

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
      
- **_india_linkedin_pyspark.py_** : Does the same as **_india_linkedin_pyspark.py_** however aggregates the data by year, state of employment (in India) and the 6-digit onet code.
    - _outputs_: .csv file of average salary and the number of individuals by year-month pair, state of employment (in India) and the 2-digit onet code
    - _note_: to alter this code to apply to labor in any country, change line 47 to state
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
      where _<desired_date>_ takes the form a "YYYY-MM-DD" string (i.e. "1970-12-09", "1971-10-24, "2000-10-04", etc.). To alter this code to apply to migrant labor from any country in the dataset to the US, change line 63 to state
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
        where _<name_of_destination_country>_ is the desired destination country of the migrants.Note also that line 34 is essential to ensure that pyspark will read the education dates as is without generating an ancient date error  
      
-   **_india_migration_state.py_** : Considers only migrants to the US, from India, by user_id. Migrants are identified, by user_id, as any individual who appears in the Linkedin dataset in both India and later the US. The working dataset for this code uses identified user_id's of migrants to construct an India-US migrant dataset. Similar to the **_india_linkedin_pyspark_ym.py_** script, individual work experience, for Indian migrants within the US, is matched by user_id to generate work history for each individual in every year in which they were pat of the labor market using Linkedin job position data. Unlike the **_india_migration_educ.py_** and **_india_migration_educ_pre1990.py_** files, this code does not require the Revellio Linkedin education data so individuals are not determined to have joined the labor market when they graduate. Rather, the entry date to the labor market, for an individual, is set to be 3 years before the end date of the first position _if_ the start date is missing. All other start and end dates are filled iteratively. Code aggregates the expanded migrant dataset by 6-digit onet code, state of origin in India, and year. State of origin in india is determined to be the state in which the individual last worked in India prior to migrating to the US.
    - _outputs_: aggregated .csv files of average salary and the number of individuals by 6-digit onet code, state of origin in India, and year. Note that state of origin is taken from the Revellio Linkedin job position data
    - _note_: to alter this code to apply to migrant labor from any country in the dataset to the US, change line 52 to state
        ```
        india_user_df = india_df.filter(col('country') == <name_of_origin_country>)
        ```
        and line 105 to state
        ```
        india_only = india_df.filter(col('country') == <name_of_origin_country>) 
        ```
        where _<name_of_origin_country>_ is the desired country of origin of the migrants. To alter this code to apply to migrant labor to a desired destination country, change line 54 to state
        ```
        us_user_df = india_df.filter(col('country') == <name_of_destination_country>)
        ```
        and line 104 to state
        ```
        us_df = india_df.filter(col('country') == <name_of_destination_country>)
        ```
        where _<name_of_destination_country>_ is the desired destination country of the migrants.

-   **_merge_city_mapper.py_** : merges all csv files in the city mapper dataset to be a singular parquet file for use with the AKM model.
    - _outputs_: .parquet file of merged city mapper csv files

- **_collapse_education_data.py_** : converts Revellio Linkedin Education Data into a wide format (by user_id) for use as controls for individual education, universities, year of education etc.
      - _outputs_: .parquet file with 1 row per user_id detailing each level of education that an individual has attained in the dataset (grouped by High School, Associates Degrees, Bachelors Degrees and Post Graduate Degrees)
