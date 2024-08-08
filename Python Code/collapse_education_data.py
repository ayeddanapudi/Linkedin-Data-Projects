# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 12:34:44 2024

@author: ayedd
"""

import pandas as pd 
import numpy as np
import glob
import os

#%%
def main():
    path = "/labs/bharadwajlab/linkedin/individual_education/"
    files = glob.glob(os.join.path(path, "/*.parquet"))
    lst = []
    
    for file in files:
        df = pd.read_parquet(file)
        lst.append(df)
        
    educ_df = pd.concat(lst, axis = 0, ignore_index = True)
    
    educ_df['degree'].unique().tolist()
    
    educ_df = educ_df[educ_df['degree'] != 'empty']
    
    degrees = educ_df['degree'].unique().tolist()
    
    categories = {'High School': 'High School', 'Associate' : 'Associate', 'Bachelor' : 'Bachelor'}
    
    for i in degrees: 
        if i not in categories:
            categories[i] = 'Post Graduate'
            
            
    educ_df['Educ_Level'] = educ_df['degree'].map(categories)
    
    educ_df_temp = educ_df[['user_id', 'school', 'field', 'university_country', 'university_location', 'Educ_Level', 'enddate']]
    
    # get locations for each of the categories
    educ_df_temp['high_school_location_raw'] = np.where(educ_df_temp['Educ_Level'] == 'High School', educ_df_temp['university_location'], np.nan)
    educ_df_temp['high_school_country'] = np.where(educ_df_temp['Educ_Level'] == 'High School', educ_df_temp['university_country'], np.nan)
    
    
    educ_df_temp['associates_location_raw'] = np.where(educ_df_temp['Educ_Level'] == 'Associate', educ_df_temp['university_location'], np.nan)
    educ_df_temp['associates_country'] = np.where(educ_df_temp['Educ_Level'] == 'Associate', educ_df_temp['university_country'], np.nan)
    
    
    educ_df_temp['college_location_raw'] = np.where(educ_df_temp['Educ_Level'] == 'Bachelor', educ_df_temp['university_location'], np.nan)
    educ_df_temp['college_country'] = np.where(educ_df_temp['Educ_Level'] == 'Bachelor', educ_df_temp['university_country'], np.nan)
    educ_df_temp['college_field'] = np.where(educ_df_temp['Educ_Level'] == 'Bachelor', educ_df_temp['field'], np.nan)
    
    
    
    educ_df_temp['post_grad_location_raw'] = np.where(educ_df_temp['Educ_Level'] == 'Post Graduate', educ_df_temp['university_location'], np.nan)
    educ_df_temp['post_grad_country'] = np.where(educ_df_temp['Educ_Level'] == 'Post Graduate', educ_df_temp['university_country'], np.nan)
    educ_df_temp['post_grad_field'] = np.where(educ_df_temp['Educ_Level'] == 'Post Graduate', educ_df_temp['field'], np.nan)
    
    
    
    educ_df_temp = educ_df_temp[['user_id', 'enddate', 'Educ_Level', 'high_school_location_raw', 'high_school_country',
                                 'associates_location_raw', 'associates_country', 'college_location_raw','college_country', 'college_field',
                                 'post_grad_location_raw', 'post_grad_country', 'post_grad_field']]
    
    
    def first_non_nan(series):
        return series.dropna().iloc[0] if not series.dropna().empty else np.nan
    
    
    educ_df_temp = educ_df_temp.sort_values(by=['user_id', 'enddate'], ascending=[True, False])
    
    collapsed_df = educ_df_temp.groupby('user_id').agg(first_non_nan).reset_index()
    
    collapsed_df = collapsed_df[['user_id', 'high_school_location_raw', 'high_school_country',
                                 'associates_location_raw', 'associates_country', 'college_location_raw','college_country', 'college_field',
                                 'post_grad_location_raw', 'post_grad_country', 'post_grad_field']]



#%%
if __name__ == '__main__':
    main()