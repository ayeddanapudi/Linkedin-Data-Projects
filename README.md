# Linkedin-Data-Projects

This repo contains the associated code and selected data products for use with the Revellio Linkedin education and position datasets. See the list below for short descriptions of each code file:

- **_akm_merge_script.py_** : utilizes dask's distributed capacities to compile each of the .parquet position files into single dataframe that can then be used for the AKM model (this particular script works well on a local machine however stalls when run on the SSRDE SLURM cluster so is recommended to be used for local computations on samples of the data only).
    - _outputs_: locally stored composite dataframe - can be saved by adding the command _dd.to_csv(/path/to/folder/)_ or alternatives for .parquet, .xlsx, etc. file types

-  
