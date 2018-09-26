# a data request - to see all downtown trips departure between 6am and 9am. 
# model run: 2040 constrained 
# departuren time: 360 - 540
# downtown area: downtown TAZ 
import pandas as pd 
import numpy as np 
import os 

model_path = r'\\license\Model Archive\T2040\soundcast_2040_constrained'
trip = pd.read_csv(os.path.join(model_path,r'outputs/daysim/') + r'_trip.tsv', sep='\t')

trip_am = trip[(trip['deptm']>= 360) & (trip['deptm'] < 540)]

downtown_path = r'T:\2018September\Angela\Downtown'
zone_dt = pd.read_csv(os.path.join(downtown_path, r'city_center_zones.csv'))
trip_am_dt = pd.merge(zone_dt, trip_am, left_on='TAZ_abs', right_on='otaz', how='left')
trip_am_dt.to_csv(os.path.join(downtown_path, r'trip_6_9_downtown.csv'))
