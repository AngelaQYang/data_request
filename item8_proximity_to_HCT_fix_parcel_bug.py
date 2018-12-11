import pandas as pd 
import numpy as np 
import os 
import datetime
output_path = 'U:\\angela\\job_housing\\soundcast_2050\\job_housing_commute\\vision_long_list'
tract_id_to_census_tract_file = 'U:\\angela\\job_housing\\soundcast_2050\\inputs\\accessibility\\tract_id_to_census_tract.csv'
parcel_tract_id_file = 'U:\\angela\\job_housing\\soundcast_2050\\inputs\\accessibility\\parcel_geographies_to_tract_id.csv'

 #buffer_file = 'U:\\angela\\job_housing\\soundcast_2050\\job_housing_commute\\vision_long_list\\input\\2017HTC_parcels.txt'
 ## buffer for both HTC and BRT
buffer_file = 'T:\\2018December\\Angela\\2018Network\\ALL_STOPS_2017_parcel_clip.csv'



## 1. urbansim parcel to population file (2014 data)
model_path = r'L:\vision2050\soundcast\integrated\final_runs\base_year\2014'
parcel_file_name = 'inputs\scenario\landuse\parcels_urbansim.txt' 
parcel_urbansim = pd.read_csv(os.path.join(model_path, parcel_file_name), sep = ' ')

## 2. parcel to census look up tables
parcel_tract_id = pd.read_csv(parcel_tract_id_file)
tract_id_to_census_tract = pd.read_csv(tract_id_to_census_tract_file)
my_parcel_census_tract = pd.merge(parcel_tract_id, tract_id_to_census_tract, left_on='census_tract_id', right_on='census_tract_id', how='outer')
# check if there is any mismatched raw 
print my_parcel_census_tract.isnull().sum()

## 3. attach census id with household population information 
parcel_census_urbansim = pd.merge(parcel_urbansim, my_parcel_census_tract, left_on = 'PARCELID', right_on = 'parcel_id')
# check if there is any mismatch 
print parcel_census_urbansim.isnull().sum()
'''
attention: if it is HTC, then the HH_P suppose to be HH_P_x and HH_P_y. Same as all HH_P below. they could be HH_P_x. 
There is no big different, either x or y works. 
'''
parcel_census_urbansim = parcel_census_urbansim[['parcel_id', 'geoid10', 'HH_P']]
# GET THE TOTAL HH POPULATION
parcel_census_urbansim_HH_P = parcel_census_urbansim.groupby(['geoid10'])[['HH_P']].sum()
parcel_census_urbansim_HH_P.reset_index(inplace=True)
hh_p_dict = parcel_census_urbansim_HH_P.set_index(['geoid10']).to_dict()['HH_P']


# 4. the second clip of HTC (with household information)
buffer = pd.read_csv(buffer_file)
# make sure data types are same, so merge would be all matched
## wried rounding problem (5.0 will become 4). Solved by using .round(0)astype(int)
buffer['PSRC_ID'] = buffer['PSRC_ID'].round(0).astype(int)

## 5. merge HCT buffer with census tract 
buffer_census = pd.merge(buffer, parcel_census_urbansim, left_on = 'PSRC_ID', right_on = 'parcel_id', how = 'left')
print buffer_census.isnull().sum()


## 6. groupby the buffer area's household population
buffer_census = buffer_census[['geoid10', 'parcel_id', 'PSRC_ID', 'HH_P']]
print buffer_census['HH_P'].sum()
buffer_census_groupby = buffer_census.groupby(['geoid10'])[['HH_P']].sum()
buffer_census_groupby.reset_index(inplace=True)
print buffer_census_groupby['HH_P'].sum()


## 7. get all census tract 
census_df = tract_id_to_census_tract[['geoid10']]
final = pd.merge(buffer_census_groupby, census_df, left_on='geoid10', right_on='geoid10', how='right')

# 8. map the totalhousehold population into the final table
final['HH_P_TOT'] = final['geoid10'].map(hh_p_dict)
final = final.fillna(0)
final['buffer_proportion'] = final['HH_P'] / final['HH_P_TOT']

## 9. write out data
today = datetime.date.today()
file_name = 'item8_proximity_to_' + str(today) + '.csv'
final.to_csv(os.path.join(output_path, file_name), index=False)








