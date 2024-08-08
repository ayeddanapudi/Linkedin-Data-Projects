# -*- coding: utf-8 -*-
"""
Created on Thu Jul 11 11:41:11 2024

@author: ayedd
"""

import dask.dataframe as dd
import os

#%% City Data
base_path = "/labs/bharadwajlab/"
folder2 = "gaurav_city_mapper/"
location_files = os.listdir(base_path+folder2)
city_mapper_files = [base_path + folder2 + i for i in location_files if i.endswith(".csv")]
print(f"Found {len(location_files)} City data files", flush= True)
loc_frames = []
for i in city_mapper_files:
    loc_frames.append(dd.read_csv(i))
ddf_concat_loc = dd.concat(loc_frames)
print("Got all city mapper files and starting computation", flush = True)
df_concat_loc = ddf_concat_loc.compute()
print("Finished computation", flush = True)
print("Starting to drop duplicates...", flush = True)
df_concat_loc = df_concat_loc.drop_duplicates(keep = 'first')
print("Finished dropping duplicates", flush = True)
print("Saving to folder ...", flush = True)
df_concat_loc.to_parquet("city_mapper_merged.parquet")
print("Saved to folder")
