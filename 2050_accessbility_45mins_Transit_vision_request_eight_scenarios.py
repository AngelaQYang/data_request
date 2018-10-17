import inro.emme.database.emmebank as _eb
import pandas as pd
import numpy as np
import os

year = 2050
scenario = 'dw-jh'
transit_time_max = 45
bank_tod = '7to8'
geo_list = ['region', 'minority', 'poverty', 'tract']
parcel_attributes_list = ['EMPTOT_P']
# parcel_attributes_list = ['EMPTOT_P', 'HH_P_TEST']
output_path = 'S:\\angela\job_housing\soundcast_2050\job_housing_commute\\vision_request_eight_scenarios'
geo_path = 'S:\\angela\job_housing\soundcast_2050\inputs\\accessibility'
geo_file_name = 'parcel_tract_county.csv'
minority_file_name = '2016-5yr-ACS-Equity-Populations.csv'

if year == 2014:
    if scenario == 'loads':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2014'
        parcel_file_name = 'inputs\scenario\landuse\parcels_urbansim.txt' 

if year == 2050:
    if scenario == 'stc':
        model_path = r'L:\\vision2050\soundcast\integrated\draft_runs\stc\stc_run_3_2018_08_17_13_06\2050'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt'
    if scenario == 'dug':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\draft_versions\dug\2050_dug_b_181005_run_4_2018_10_02_11_57'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt'  
    if scenario == 'dug-jh':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\draft_versions\dug\2050_dug_jobhh_balance_181004_run1_2018_10_01_20_37_'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt' 
    if scenario == 'tod':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\draft_versions\tod\2050_tod_181004_run_3_2018_10_02_14_30_'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt' 
    if scenario == 'tod-jh':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\draft_versions\tod\2050_tod_b_181008_run_12_2018_10_05_15_04'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt' 
    if scenario == 'h2o2':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\draft_versions\h2o2\2050_h2o2_181005_run_6_2018_10_02_12_01'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt'
    if scenario == 'h2o2-jh':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\draft_versions\h2o2\2050_h2o2_b_181008_run_2_2018_10_05_14_50'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt'
    if scenario == 'dw':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\draft_versions\dug_water\2050_dug_water'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt'
    if scenario == 'dw-jh':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\draft_versions\dug_water\2050_dug_water_jobhh_balance_181014'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt'


geo_boundry = {'region' : 'region_id',
               'county' : 'county_id',
               'city' : 'city_id', 
               'taz' : 'TAZ_P',
               'tract' : 'census_tract',
               'parcel' : 'parcel_id', 
               'fips_rgs' : 'fips_rgs_id', 
               'rgs' : 'rgs_id', 
               'rg_proposed' : 'rg_proposed',
               'minority': 'minority_geog',
               'poverty': 'poverty_geog'}

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



def process_transit_attribute(transit_time_data, transit_time_max,  attr_list, origin_df, dest_df, tract_dict, taz_dict):
    # get transit information
    transit = transit_time_data[transit_time_data.transit_time <= transit_time_max]
    # delete transit opportunities for internal zone travel, we assume all people won't take transit if it is internal zone
    transit = transit[transit['from'] != transit['to']]
    #prepare orgin and destination information
    origin_transit = origin_df.merge(transit, left_on = 'taz_id', right_on = 'from', how = 'left')
    transit_df = origin_transit.merge(dest_df, left_on='to', right_on='TAZ_P', how='left') 
    # groupby destination information by origin geo id 
    transit_emp = pd.DataFrame(transit_df.groupby('parcel_id')[attr_list].sum())
    transit_emp.reset_index(inplace=True)
    # get the origin geo level household info
    transit_hh = pd.DataFrame(origin_df.groupby('parcel_id')['HH_P'].sum())
    transit_hh.reset_index(inplace=True)
    print '2', 'total household: ', transit_hh['HH_P'].sum()
    transit_hh_emp = transit_hh.merge(transit_emp, on = 'parcel_id', how='left')
    transit_hh_emp['census_tract'] = transit_hh_emp['parcel_id'].map(tract_dict)
    transit_hh_emp['taz_id'] = transit_hh_emp['parcel_id'].map(taz_dict)
    transit_hh_emp['region_id'] = 1
    print transit_hh_emp.head(2)
    return transit_hh_emp


def get_average_jobs(transit_data, geo_attr, parcel_attributes_list):
    for res_name in parcel_attributes_list: 
        print 'process attribute: ', res_name
        # creat weighted employment
        weighted_res_name = 'HHweighted_' + res_name
        transit_data[weighted_res_name] = transit_data['HH_P']*transit_data[res_name]
    # look into the specific geographic level
    transit_data_groupby = transit_data.groupby([geo_attr]).sum()
    transit_data_groupby.reset_index(inplace = True)
    for res_name in parcel_attributes_list: 
        weighted_res_name = 'HHweighted_' + res_name
        averaged_res_name = 'HHaveraged_' + res_name
        transit_data_groupby[averaged_res_name] = transit_data_groupby[weighted_res_name]/transit_data_groupby['HH_P']
    print transit_data_groupby.head(2)
    return transit_data_groupby

def main():

    # get geo location information
    parcel_df = pd.read_csv(os.path.join(model_path, parcel_file_name), sep = ' ')
    parcel_df['HH_P_test'] = parcel_df['HH_P']
    minority_df = pd.DataFrame.from_csv(os.path.join(geo_path, minority_file_name), sep = ',', index_col = None)
    geo_df = pd.DataFrame.from_csv(os.path.join(geo_path, geo_file_name), sep = ',', index_col = None )
    geo_df = pd.merge(parcel_df, geo_df, left_on = 'PARCELID', right_on = 'parcel_id')
    tract_dict = geo_df.set_index(['parcel_id']).to_dict()['census_tract']
    taz_dict = geo_df.set_index(['parcel_id']).to_dict()['TAZ_P']

    # organize origin information
    origin_df = pd.DataFrame(geo_df.groupby(['parcel_id'])['HH_P'].sum())
    origin_df.reset_index(inplace=True)
    origin_df['taz_id'] = origin_df['parcel_id'].map(taz_dict) #need TAZ to join with transit time table 
    # orgnize destination information
    dest_df = pd.DataFrame(geo_df.groupby(['TAZ_P'])[parcel_attributes_list].sum())
    dest_df.reset_index(inplace=True)

    # process transit time
    bank = _eb.Emmebank(os.path.join(model_path, 'Banks', bank_tod, 'emmebank'))
    transit_time_df = get_transit_information(bank)
    transit_hh_emp = process_transit_attribute(transit_time_df, transit_time_max, parcel_attributes_list, origin_df, dest_df, tract_dict, taz_dict)
    # flag the minority tracts
    transit_hh_emp = transit_hh_emp.merge(minority_df, left_on = 'census_tract', right_on = 'GEOID10', how = 'left')
    print transit_hh_emp.head()
    # calculate jobs on transit time
    for geo in geo_list:
        print geo
        average_jobs_df = get_average_jobs(transit_hh_emp, geo_boundry[geo], parcel_attributes_list) 
        output_file_name = str(year) + '_' + scenario + '_' + geo +  '_transit_' +  str(transit_time_max) + '_' + 'min.csv'
        print output_file_name
        average_jobs_df.to_csv(os.path.join(output_path, output_file_name), index=False)
    print scenario, 'done'

if __name__ == '__main__':
    main()

    




