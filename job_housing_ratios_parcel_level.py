'''
Task:  Calculate job/housing ratio (if it is a prepraration step fpr 'job housing commute', then the geo-setting must be TAZ level)
inputs: urbansim inputs txt file, 7am-8am traffic bank, geographic boundry look up file
outputs: geo-level calsses, jobs within ## mins, hh info within ## mins, job/housing ratio, j-h classes
Author: AngelaY. 
Developed Time: 10/2018
update log: 
1/30/2019, run scenario rug, add some comments
1/31 update other scenarios newest locations
2/6 update the AM (7am-8am) auto time to be AM PM (5pm-6pm) averaged: am+(pm.transpose)/2 
'''

import inro.emme.database.emmebank as _eb # you need an EMME license 
import pandas as pd
import numpy as np
import os

year = 2014
scenario = 'Current' # stc, dug, non_integrated, h202
geo = 'taz'
transit_time_max = 30
mode_meansurement = 'auto'
time_period = 'ampm' 
if time_period == 'am':
    BANK_TOD_AM = '7to8'
    BANK_TOD_PM = '7to8'
if time_period == 'ampm':  ##### test out AM + pm / 2 
    BANK_TOD_AM = '7to8'
    BANK_TOD_PM = '17to18'
   

parcel_attributes_list = ['EMPTOT_P', 'HH_P_test']

## OUTPUT

output_path = 'S:\\angela\job_housing\soundcast_2050\data_request_01_2019_JHratio' # make sure S drive still is modelsrv7 
output_file_name = geo + '_' + mode_meansurement + '_' + str(year) + '_' + str(transit_time_max) + 'min_' + time_period + '_' + scenario + '.csv'

## INPUT 
if year == 2014:
    if scenario == 'loads':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2014' 
        parcel_file_name = 'inputs\scenario\landuse\parcels_urbansim.txt' 
    if scenario == 'Current':
        model_path = r'L:\vision2050\soundcast\integrated\final_runs\base_year\2014'
        parcel_file_name =  r'inputs\\scenario\\landuse\\parcels_urbansim.txt'

if year == 2050:
    if scenario == 'dug':
        model_path = r'L:\\vision2050\soundcast\integrated\dug\2050'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt' 
    if scenario == 'stc':
        model_path = r'L:\\vision2050\soundcast\integrated\stc\2050'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt' 
    if scenario == 'non_integrated':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\updated_tod_top_down_2050'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt' 
    if scenario == 'h2o2':
        model_path = r'L:\\vision2050\soundcast\integrated\h2o2\2050'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt'
    # below are for 1/2019 run 
    if scenario == 'rug':
        model_path = r'L:\\vision2050\soundcast\integrated\final_runs\rug\rug_run_5.run_2018_10_25_09_07\2050'
        parcel_file_name =  r'inputs\\scenario\\landuse\\parcels_urbansim.txt'
    if scenario == 'Transit Forcused Growth':
        model_path = r'L:\\vision2050\soundcast\integrated\final_runs\tod\tod_run_8.run_2018_10_29_15_01\2050'
        parcel_file_name =  r'inputs\\scenario\\landuse\\parcels_urbansim.txt'
    if scenario == 'Stay the Course':
        model_path = r'L:\\vision2050\soundcast\integrated\final_runs\stc\stc_run_6.run_2018_10_23_11_15\2050'
        parcel_file_name =  r'inputs\\scenario\\landuse\\parcels_urbansim.txt'







geo_path = 'S:\\angela\job_housing\soundcast_2050\inputs\\accessibility' # make sure geo file still there, S drive still modelsrv 7 
geo_file_name = 'parcel_tract_county.csv'
geo_boundry = {'county': 'county_id',
               'city': 'city_id',
               'city_name': 'city_name', 
               'taz': 'TAZ_P',
               'region': 'region_id'}

'''
## create the parcel census and county look up geo information file 
parcel_tract = 'parcels_to_tract.csv'
parcel_county = 'parcels_suzanne.csv'
parcel_tract = pd.DataFrame.from_csv(os.path.join(geo_path, parcel_tract), sep = ',', index_col = None )
parcel_county = pd.DataFrame.from_csv(os.path.join(geo_path, parcel_county), sep = ',', index_col = None )

parcel_tract_county = pd.merge(parcel_tract, parcel_county, on = 'parcel_id', how = 'outer')
parcel_tract_county['county_id'] = parcel_tract_county['county_id_x']
parcel_tract_county['city_id'] = parcel_tract_county['city_id_x']
cols = [u'parcel_id', u'census_tract', u'city_id', u'county_id', 
       u'city_name', u'fips_rgs_id', u'rgs_id', u'rg_proposed']
parcel_tract_county = parcel_tract_county[cols]
parcel_tract_county.to_csv(os.path.join(r'S:\angela\job_housing\soundcast_2050\inputs\accessibility\parcel_tract_county.csv'), index=False)

'''

# get transit information
def get_transit_information(bank):
    bus_time = bank.matrix('auxwa').get_numpy_data() + bank.matrix('twtwa').get_numpy_data() + bank.matrix('ivtwa').get_numpy_data() 
    rail_time = bank.matrix('auxwr').get_numpy_data() + bank.matrix('twtwr').get_numpy_data() + bank.matrix('ivtwr').get_numpy_data() 
    transit_time = np.minimum(bus_time, rail_time)
    transit_time = transit_time[0:3700, 0:3700]
    transit_time_df = pd.DataFrame(transit_time)
    transit_time_df['from'] = transit_time_df.index
    transit_time_df = pd.melt(transit_time_df, id_vars= 'from', value_vars=list(transit_time_df.columns[0:3700]), var_name = 'to', value_name='transit_time')
    # add 1 into zone id before join with parcel data
    transit_time_df['to'] = transit_time_df['to'] + 1 
    transit_time_df['from'] = transit_time_df['from'] + 1
    return transit_time_df


# get auto information
def get_auto_information(bank_am, bank_pm):
    # get the averaged time from AM and PM
    time_am = bank_am.matrix('svtl2t').get_numpy_data()
    time_pm = bank_pm.matrix('svtl2t').get_numpy_data()
    time_pm2 = time_pm.transpose()
    time = (time_am + time_pm2)/2
    #time = bank.matrix('svtl2t').get_numpy_data() 
    veh_time = time[0:3700, 0:3700]
    veh_time_df = pd.DataFrame(veh_time)
    veh_time_df['from'] = veh_time_df.index
    veh_time_df = pd.melt(veh_time_df, id_vars= 'from', value_vars=list(veh_time_df.columns[0:3700]), var_name = 'to', value_name='transit_time')
    # add 1 into zone id before join with parcel data
    veh_time_df['to'] = veh_time_df['to'] + 1 
    veh_time_df['from'] = veh_time_df['from'] + 1
    return veh_time_df



def process_transit_attribute(transit_time_data, transit_time_max, parcel_taz_id, transit_taz_id, attr_list, origin_df, dest_df):
    # get destination information
    transit = transit_time_data[transit_time_data.transit_time <= transit_time_max]
    transit = transit.merge(dest_df, how = 'left', left_on = 'to', right_on = parcel_taz_id)
    # groupby destination information by origin TAZ id 
    transit_emp = pd.DataFrame(transit.groupby(transit_taz_id)[attr_list].sum())
    print transit_emp.head()
    print '2.1'
    transit_emp.reset_index(inplace=True)
    print transit_emp.head()
    print '2.2'
    transit_df = pd.merge(transit_emp, origin_df, left_on = transit_taz_id, right_on = parcel_taz_id)
    print transit_df.head()
    print '2.3'
    return transit_df


def get_average_jobs(transit_data, geo_attr, parcel_attributes_list):
    for res_name in parcel_attributes_list: 
        print 'process attribute: ', res_name
        # creat weighted employment
        weighted_res_name = 'HHweighted_' + res_name
        transit_data[weighted_res_name] = transit_data['HH_P']*transit_data[res_name]
    print transit_data.head()
    print '1.1'
    # look into the specific geographic level
    transit_data_groupby = transit_data.groupby([geo_attr]).sum()
    transit_data_groupby.reset_index(inplace = True)
    for res_name in parcel_attributes_list: 
        weighted_res_name = 'HHweighted_' + res_name
        averaged_res_name = 'HHaveraged_' + res_name
        transit_data_groupby[averaged_res_name] = transit_data_groupby[weighted_res_name]/transit_data_groupby['HH_P']
    print transit_data_groupby.head()
    print '1.2'
    return transit_data_groupby

def main():

    # get origin information
    parcel_df = pd.read_csv(os.path.join(model_path, parcel_file_name), sep = ' ')
    print 'here0'
    parcel_df['HH_P_test'] = parcel_df['HH_P']
    geo_df = pd.DataFrame.from_csv(os.path.join(geo_path, geo_file_name), sep = ',', index_col = None )
    geo_df = pd.merge(parcel_df, geo_df, left_on = 'PARCELID', right_on = 'parcel_id')
    city_dict = geo_df.set_index(['TAZ_P']).to_dict()['city_id']
    county_dict = geo_df.set_index(['TAZ_P']).to_dict()['county_id']
    city_name_dict = geo_df.set_index(['TAZ_P']).to_dict()['city_name']
    print 'here1'
    # organize origin information
    origin_df = pd.DataFrame(geo_df.groupby(['TAZ_P'])['HH_P'].sum())
    origin_df.reset_index(inplace=True)
    origin_df['city_id'] = origin_df['TAZ_P'].map(city_dict)
    origin_df['county_id'] = origin_df['TAZ_P'].map(county_dict)
    origin_df['city_name'] = origin_df['TAZ_P'].map(city_name_dict)
    origin_df['region_id'] = 1
    # orgnize destination information
    dest_df = pd.DataFrame(geo_df.groupby(['TAZ_P'])[parcel_attributes_list].sum())
    dest_df.reset_index(inplace=True)
    # process transit time
    #bank = _eb.Emmebank(os.path.join(model_path, 'Banks', bank_tod, 'emmebank'))
    bank_am = _eb.Emmebank(os.path.join(model_path, 'Banks', BANK_TOD_AM, 'emmebank'))
    bank_pm = _eb.Emmebank(os.path.join(model_path, 'Banks', BANK_TOD_PM, 'emmebank'))  
    if mode_meansurement == 'auto':
        print mode_meansurement
        transit_time_df = get_auto_information(bank_am, bank_pm) # will have to fix this code later 
    else:
        print mode_meansurement
        transit_time_df = get_transit_information(bank)
    #transit_time_df = get_auto_information(bank)
    #transit_time_df = get_transit_information(bank)
    print transit_time_df.head()
    transit_df = process_transit_attribute(transit_time_df, transit_time_max, 'TAZ_P', 'from', parcel_attributes_list, origin_df, dest_df)
    print transit_df.head()
    # calculate jobs on transit time
    average_jobs_df = get_average_jobs(transit_df, geo_boundry[geo], parcel_attributes_list) 
    print average_jobs_df
    average_jobs_df.to_csv(os.path.join(output_path, output_file_name), index=False)
    print 'output file name is: ', output_file_name
    #return average_jobs_df
    print year, scenario, transit_time_max
    print 'done'

if __name__ == '__main__':
    main()

    

########### validation ############ 
#average_jobs_df['HH_P'].sum()
#average_jobs_df['EMPTOT_P'].sum()




