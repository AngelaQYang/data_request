import pandas as pd
import numpy as np
import os
import h5py
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

## 3. attach parcel id and census tract id 
my_parcel_urbansim = pd.merge(parcel_urbansim, my_parcel_census_tract, left_on='PARCELID', right_on='parcel_id', how='outer')

## 4. daysim file for population information 
daysim_path = r'L:\vision2050\soundcast\integrated\final_runs\base_year\2014\outputs\daysim'
daysim = h5py.File(daysim_path + r'\daysim_outputs.h5', "r+")
hh = pd.DataFrame(data={'hhno': daysim['Household']['hhno'][:],
                        'hhsize': daysim['Household']['hhsize'][:],
                        'hhparcel': daysim['Household']['hhparcel'][:],
                        'hhexpfac': daysim['Household']['hhexpfac'][:]})


'''
ps = pd.DataFrame(data={'hhno': daysim['Person']['hhno'][:],
                        'id': daysim['Person']['id'][:],
                        'pno': daysim['Person']['pno'][:],
                        'pspcl': daysim['Person']['pspcl'][:],
                        'psexpfac': daysim['Person']['psexpfac'][:]})
'''

hh['hhsize_fac'] = hh['hhsize'] * hh['hhexpfac']
hh_groupby = hh.groupby(['hhparcel'])[['hhsize_fac']].sum()
hh_groupby.reset_index(inplace=True)
parcel_hh_size_dict = hh_groupby.set_index(['hhparcel']).to_dict()['hhsize_fac']

## 5. put all info together: parcel, population, census 
my_parcel_urbansim = my_parcel_urbansim[['geoid10', 'parcel_id', 'HH_P', 'TAZ_P', 'XCOORD_P', 'YCOORD_P']]
my_parcel_urbansim['hhsize_fac'] = my_parcel_urbansim['parcel_id'].map(parcel_hh_size_dict)
my_parcel_urbansim['hhsize_fac'] = my_parcel_urbansim['hhsize_fac'].fillna(0)

## 6. validation 
print my_parcel_urbansim['hhsize_fac'].sum()
print my_parcel_urbansim['HH_P'].sum()
print len(np.unique(my_parcel_urbansim['geoid10']))
print len(np.unique(my_parcel_urbansim['parcel_id']))
print len(my_parcel_urbansim)


## 7. write out 
my_parcel_urbansim.to_csv('T:\\2018November\\Angela\\my_parcel_urbansim.txt', index=False)

test = pd.read_csv('T:\\2018November\\Angela\\my_parcel_urbansim.txt')