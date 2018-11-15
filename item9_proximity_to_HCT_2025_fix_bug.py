import pandas as pd
import numpy as np
import os
import datetime

year = 2014
scenario = 'loads'

output_path = 'U:\\angela\job_housing\soundcast_2050\job_housing_commute\\vision_long_list'
geo_path = 'U:\\angela\job_housing\soundcast_2050\inputs\\accessibility'
## below two files are census id look up tables from Brian, SQL quaried from land use files
parcel_tract_id = 'parcel_geographies_to_tract_id.csv'
tract_id_to_census_tract = 'tract_id_to_census_tract.csv'
## below two files are clipped from parcel GIS file: J:\Projects\UrbanSim\NEW_DIRECTORY\GIS\Shapefiles\Parcels\Region\2014\gapwork\prcl15_4kpt.shp
HCT_area_file_name = 'parcel_HCT_area_2025.txt'
parcel_area_file_name = 'parcel_area.txt'

if year == 2014:
    if scenario == 'loads':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2014'
        parcel_file_name = 'inputs\scenario\landuse\parcels_urbansim.txt' 

geo_boundry = {'region' : 'region_id',
               'county' : 'county_id',
               'city' : 'city_id', 
               'taz' : 'TAZ_P',
               'tract' : 'census_tract',
               'parcel' : 'parcel_id'}

## 1. parcel 
'''
Got parcel file from Peter 
Parcel point - J:\Projects\UrbanSim\NEW_DIRECTORY\GIS\Shapefiles\Parcels\Region\2014\gapwork\prcl15_4kpt.shp
parcel id is: PSRC_ID
parcel size: Shape_Area (The unit of measure for the shape_area should be square feet.)
'''
parcel_area = pd.read_csv(os.path.join(output_path, parcel_area_file_name), sep = ',')
# fixed the bug
parcel_area['PSRC_ID'] = parcel_area['PSRC_ID'].round(0).astype(int)

## 2. parcel to census look up tables
parcel_tract_id = pd.DataFrame.from_csv(os.path.join(geo_path, parcel_tract_id), sep = ',', index_col = None)
tract_id_to_census_tract = pd.DataFrame.from_csv(os.path.join(geo_path, tract_id_to_census_tract), sep = ',', index_col = None)
my_parcel_census_tract = pd.merge(parcel_tract_id, tract_id_to_census_tract, left_on='census_tract_id', right_on='census_tract_id', how='outer')

parcel_area_geo = pd.merge(parcel_area, my_parcel_census_tract, left_on = 'PSRC_ID', right_on = 'parcel_id', how='left')

# HCT area (no water)
'''
the parcel file was preprocessed in Arc GIS, clipped with HCT buffer area
'''
HCT_area = pd.read_csv(os.path.join(output_path, HCT_area_file_name), sep = ',')
# fixed the bug
HCT_area['PSRC_ID'] = HCT_area['PSRC_ID'].round(0).astype(int)
HCT_area['HCT_Area'] = HCT_area['Shape_Area']
HCT_area = HCT_area[[ 'PSRC_ID', 'HCT_Area']]

# areas in HCT parcel area
all_parcel_HTC = pd.merge(HCT_area, parcel_area_geo, left_on='PSRC_ID', right_on='parcel_id', how='right')
all_parcel_HTC = all_parcel_HTC[['parcel_id', 'geoid10', 'HCT_Area', 'Shape_Area']]
all_parcel_HTC = all_parcel_HTC.groupby(['geoid10'])[['HCT_Area', 'Shape_Area']].sum()
all_parcel_HTC.reset_index(inplace=True)
all_parcel_HTC = all_parcel_HTC.fillna(0)

# percentage of areas in HCT tract area
all_parcel_HTC['percent_area'] = all_parcel_HTC['HCT_Area'] / all_parcel_HTC['Shape_Area']
today = datetime.date.today()
file_name = 'tract_area_2025_' + str(today) + '.csv'
all_parcel_HTC.to_csv(os.path.join(output_path, file_name), index=False)

# the region
p = all_parcel_HTC['HCT_Area'] .sum()/all_parcel_HTC['Shape_Area'].sum()
print p 