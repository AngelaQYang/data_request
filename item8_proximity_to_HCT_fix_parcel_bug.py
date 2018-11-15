import pandas as pd 
import numpy as np 
import os 
import datetime
output_path = 'U:\\angela\\job_housing\\soundcast_2050\\job_housing_commute\\vision_long_list'

## 1. urbansim parcel to population file (2014 data)
model_path = r'L:\vision2050\soundcast\integrated\final_runs\base_year\2014'
parcel_file_name = 'inputs\scenario\landuse\parcels_urbansim.txt' 
parcel_urbansim = pd.read_csv(os.path.join(model_path, parcel_file_name), sep = ' ')

## 2. parcel to census look up tables
parcel_tract_id = 'U:\\angela\\job_housing\\soundcast_2050\\inputs\\accessibility\\parcel_geographies_to_tract_id.csv'
parcel_tract_id = pd.read_csv(parcel_tract_id)
tract_id_to_census_tract = 'U:\\angela\\job_housing\\soundcast_2050\\inputs\\accessibility\\tract_id_to_census_tract.csv'
tract_id_to_census_tract = pd.read_csv(tract_id_to_census_tract)
my_parcel_census_tract = pd.merge(parcel_tract_id, tract_id_to_census_tract, left_on='census_tract_id', right_on='census_tract_id', how='outer')
# check if there is any mismatched raw 
print my_parcel_census_tract.isnull().sum()

## 3. attach census id with household population information 
parcel_census_urbansim = pd.merge(parcel_urbansim, my_parcel_census_tract, left_on = 'PARCELID', right_on = 'parcel_id')
# check if there is any mismatch 
print parcel_census_urbansim.isnull().sum()
parcel_census_urbansim = parcel_census_urbansim[['parcel_id', 'geoid10', 'HH_P']]


# 4. the second clip of HTC (with household information)
HCT_buffer = pd.read_csv('U:\\angela\\job_housing\\soundcast_2050\\job_housing_commute\\vision_long_list\\input\\2017HTC_parcels.txt')
# make sure data types are same, so merge would be all matched
## wried rounding problem (5.0 will become 4). Solved by using .round(0)astype(int)
HCT_buffer['PSRC_ID'] = HCT_buffer['PSRC_ID'].round(0).astype(int)

## 5. merge HCT buffer with census tract 
HCT_buffer_census = pd.merge(HCT_buffer, parcel_census_urbansim, left_on = 'PSRC_ID', right_on = 'parcel_id', how = 'left')
print HCT_buffer_census.isnull().sum()


## 6. groupby 
HCT_buffer_census = HCT_buffer_census[['geoid10', 'parcel_id', 'PSRC_ID', 'HH_P_x', 'HH_P_y']]
print HCT_buffer_census['HH_P_y'].sum()
HCT_buffer_census_groupby = HCT_buffer_census.groupby(['geoid10'])[['HH_P_y']].sum()
HCT_buffer_census_groupby.reset_index(inplace=True)
print HCT_buffer_census_groupby['HH_P_y'].sum()


## 7. get all census tract 
census_df = tract_id_to_census_tract[['geoid10']]
final = pd.merge(HCT_buffer_census_groupby, census_df, left_on='geoid10', right_on='geoid10', how='right')
final = final.fillna(0)
## 7. write out data
today = datetime.date.today()
file_name = 'item8_proximity_to_HTC_' + str(today) + '.csv'
final.to_csv(os.path.join(output_path, file_name), index=False)








