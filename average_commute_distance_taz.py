import pandas as pd 
import numpy as np 
import os 
import h5py



year = '2014'
scenario = 'stc' # stc, dug, h202, non_integrated

output_path = r'U:\\angela\job_housing\soundcast_2050\job_housing_commute\final_set' 
# input for job/housing accessibility
job_path = r'U:\\angela\job_housing\soundcast_2050\job_housing_commute'
job_file_name = 'taz' + '_auto_' + year + '_' + time + 'min_' + scenario + '.csv' 
# input for household and commute
if year == '2014':
    relative_path = r'\\license\Model Archive\T2040\soundcast_2014\outputs'
    daysim_path = r'\\license\Model Archive\T2040\soundcast_2014\outputs\daysim'

geo_boundry = {'county': 'county_id',
               'city': 'city_id', 
               'taz': 'TAZ_P',
               'region': 'region_id',
               'bin': 'bin'}

daysim = h5py.File(daysim_path + r'\daysim_outputs.h5', "r+")
trip = pd.DataFrame(data={'hhno': daysim['Trip']['hhno'][:],
                            'mode': daysim['Trip']['mode'][:],
                            'trexpfac': daysim['Trip']['trexpfac'][:],
                            'otaz': daysim['Trip']['otaz'][:],
                            'opurp': daysim['Trip']['opurp'][:],
                            'dpurp': daysim['Trip']['dpurp'][:],
                            'travdist': daysim['Trip']['travdist'][:],
                            'travtime': daysim['Trip']['travtime'][:]})

trip['hbw_trips'] = 0
trip.ix[(((trip['opurp']==0) & (trip['dpurp']==1)) | ((trip['opurp']==1) & (trip['dpurp']==0))),'hbw_trips']= 1
hbw_trip = trip[trip['hbw_trips'] == 1]
avg_commute = hbw_trip[['travdist', 'travtime', 'otaz']].groupby(['otaz']).mean().reset_index(False)

avg_commute.to_csv(r'T:\2018October\Angela\avg_acommute_trips_taz.csv')