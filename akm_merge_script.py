# -*- coding: utf-8 -*-
"""
Created on Fri Jun 28 11:36:41 2024

@author: ayedd
"""

#%%
import os
import subprocess
import sys
import time

try:
    import dask
    import dask.dataframe as dd
    from dask.distributed import Client
    from dask_jobqueue import SLURMCluster

except (ModuleNotFoundError, ImportError):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "dask[distributed]"])
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'distributed==2024.6.1'])
    subprocess.check_call([sys.executable, "-m", "pip", "install", "dask_jobqueue"])
finally:
    import dask
    import dask.dataframe as dd
    from dask.distributed import Client
    from dask_jobqueue import SLURMCluster

#%% Parquet Data
base_path = "/labs/bharadwajlab/"
folder1 = "abhalothia/sample_pos.parquet/"
parquet_files = os.listdir(base_path+folder1)
parquet_files = [file for file in parquet_files if file.endswith('.parquet')]
print(f"Found {len(parquet_files)} Parquet files", flush= True)

#%% City Data
folder2 = "gaurav_city_mapper/"
location_files = os.listdir(base_path+folder2)
print(f"Found {len(location_files)} City data files", flush= True)

#%% Setup Dask Local Cluster and Client
cluster = SLURMCluster(queue = 'regular', 
                       account = "myaccount",
                       cores = 1,
                       memory = '250 GB')
client = Client(cluster)
client.dashboard_link

print("Dask Client setup complete", flush= True)

#%%

@dask.delayed(nout = 0)
def process_files(root_path, parquet_folder, location_folder, parquet_file, loc_files):
    """
    Parameters
    ----------
    parquet_file : path
        Path from list of paths to the individual parquet files
    loc_files : list of paths
        Path from list of paths to the individual location files

    Returns
    -------
    None. - process is intended to read and save files not to return an output

    """
    test_parquet = dd.read_parquet(root_path + parquet_folder + parquet_file)

    frames_with_cities = []
    frames_wo_cities = []

    start_time = time.time()

    i = 0
    while i in range(0, len(loc_files), 1):
        if i == 0:
            # read in location maps
            location_ddf = dd.read_csv(root_path + location_folder + loc_files[i])
            # merge parquet file with individual location files
            ddf = test_parquet.merge(location_ddf, left_on = ['location_raw', 'metro_area'], right_on = ['location', 'metro_area'], how = 'left')
            # get truth series - has a location and city been identified for each row
            rows_w_location = (~ddf['location'].isna()) & (ddf['location'] != 'empty') & (~ddf['city'].isna()) & (ddf['city'] != 'empty')
            # partition into 2 dataframes
            ddf_rows_with_location = ddf[rows_w_location]
            ddf_rows_without_location = ddf[~rows_w_location]
            # save frame with location and cities identified
            frames_with_cities.append(ddf_rows_with_location)
            meta = ddf_rows_without_location.drop(columns=['location', 'city'])._meta
            ddf_rows_without_location = ddf_rows_without_location.map_partitions(lambda df: df.drop(columns=['location', 'city']), meta=meta)

            i += 1
        else:
            location_ddf = dd.read_csv(root_path + location_folder + loc_files[i])
            ddf = ddf_rows_without_location.merge(location_ddf, left_on = ['location_raw', 'metro_area'], right_on = ['location', 'metro_area'], how = 'left')
            rows_w_location = (~ddf['location'].isna()) & (ddf['location'] != 'empty') & (~ddf['city'].isna()) & (ddf['city'] != 'empty')
            ddf_rows_with_location = ddf[rows_w_location]
            ddf_rows_without_location = ddf[~rows_w_location]
            frames_with_cities.append(ddf_rows_with_location)
            meta = ddf_rows_without_location.drop(columns=['location', 'city'])._meta
            ddf_rows_without_location = ddf_rows_without_location.map_partitions(lambda df: df.drop(columns=['location', 'city']), meta=meta)

            if i == len(loc_files) - 1:
                frames_wo_cities.append(ddf_rows_without_location)

            i += 1

    end_time = time.time()
    time_elapsed = end_time - start_time

    print(f"While loop Elapsed Time: {time_elapsed:.2f} seconds", flush= True)

    ddf_concatenated = dd.concat(frames_with_cities)

    start_time = time.time()
    df_concatenated = ddf_concatenated.compute()
    df_concatenated.to_parquet(root_path + 'akm_parquet/' + parquet_file.split(".")[0] + '_' + parquet_file.split(".")[1] + '_test.parquet')
    end_time = time.time()
    time_elapsed = end_time - start_time

    start_time = time.time()
    dropped_ids = frames_wo_cities[0]['user_id'].compute()
    dropped_ids = list(dropped_ids.values)

    dropped_path = root_path +'akm_parquet/' + parquet_file.split(".")[0] + '_' + parquet_file.split(".")[1] + '_dropped_users.txt'
    with open(dropped_path, 'w') as file:
        for i in dropped_ids:
            file.write(str(i) + '\n')

    end_time = time.time()
    print(f"Dropped Users Elapsed Time: {time_elapsed:.2f} seconds", flush= True)

#%%
print("Creating Dask tasks...", flush= True)
tasks = [process_files(base_path, folder1, folder2, parquet_file, location_files) for parquet_file in parquet_files]
print("Computing tasks...", flush= True)
dask.compute(*tasks)

#%%
print("Closing Dask Client and Cluster...", flush= True)

client.close()
cluster.close()

print("Client and Cluster closed", flush= True)

#%%