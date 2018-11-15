import inro.emme.database.emmebank as _eb
import pandas as pd
import numpy as np
import os
import datetime

year = 2014
scenario = 'loads'
mode = 'transit'
time_max = 45
bank_tod = '7to8'
geo_list = ['tract']
parcel_attributes_list = ['EMPTOT_P']
# parcel_attributes_list = ['EMPTOT_P', 'HH_P_TEST']
output_path = 'U:\\angela\job_housing\soundcast_2050\job_housing_commute\\vision_long_list'
geo_path = 'U:\\angela\job_housing\soundcast_2050\inputs\\accessibility'
geo_file_name = 'parcel_tract_county.csv'

if year == 2014:
    if scenario == 'loads':
        model_path = r'L:\\vision2050\\soundcast\\integrated\\final_runs\\base_year\\2014'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt' 


geo_boundry = {'region' : 'region_id',
               'taz' : 'TAZ_P',
               'tract' : 'census_tract',
               'parcel' : 'parcel_id'}

# get transit information
def get_transit_information(bank):
    bus_time = bank.matrix('auxwa').get_numpy_data() + bank.matrix('twtwa').get_numpy_data() + bank.matrix('ivtwa').get_numpy_data() 
    rail_time = bank.matrix('auxwr').get_numpy_data() + bank.matrix('twtwr').get_numpy_data() + bank.matrix('ivtwr').get_numpy_data() 
    transit_time = np.minimum(bus_time, rail_time)
    transit_time = transit_time[0:3700, 0:3700]
    transit_time_df = pd.DataFrame(transit_time)
    transit_time_df['from'] = transit_time_df.index
    transit_time_df = pd.melt(transit_time_df, id_vars= 'from', value_vars=list(transit_time_df.columns[0:3700]), var_name = 'to', value_name='travel_time')
    # add 1 into zone id before join with parcel data
    transit_time_df['to'] = transit_time_df['to'] + 1 
    transit_time_df['from'] = transit_time_df['from'] + 1
    return transit_time_df


# get auto information
def get_auto_information(bank):
    time = bank.matrix('svtl2t').get_numpy_data() 
    veh_time = time[0:3700, 0:3700]
    veh_time_df = pd.DataFrame(veh_time)
    veh_time_df['from'] = veh_time_df.index
    veh_time_df = pd.melt(veh_time_df, id_vars= 'from', value_vars=list(veh_time_df.columns[0:3700]), var_name = 'to', value_name='travel_time')
    # add 1 into zone id before join with parcel data
    veh_time_df['to'] = veh_time_df['to'] + 1 
    veh_time_df['from'] = veh_time_df['from'] + 1
    return veh_time_df



def process_transit_attribute(transit_time_data, transit_time_max,  attr_list, origin_df, dest_df, tract_dict, taz_dict):
    # get transit information
    transit = transit_time_data[transit_time_data.travel_time <= transit_time_max]
    # delete transit opportunities for internal zone travel, we assume all people won't take transit if it is internal zone
    transit = transit[transit['from'] != transit['to']]
    #prepare orgin and destination information
    dest_transit = transit.merge(dest_df, left_on = 'to', right_on = 'TAZ_P', how = 'left')
    dest_transit = pd.DataFrame(dest_transit.groupby(dest_transit['from'])['EMPTOT_P'].sum())
    dest_transit.reset_index(inplace=True)
    origin_dest = origin_df.merge(dest_transit, left_on = 'taz_id', right_on = 'from', how = 'left') 
    # groupby destination information by origin geo id 
    origin_dest_emp = pd.DataFrame(origin_dest.groupby('parcel_id')[attr_list].sum())
    origin_dest_emp.reset_index(inplace=True)
    # get the origin geo level household info
    transit_hh = pd.DataFrame(origin_df.groupby('parcel_id')['HH_P'].sum())
    transit_hh.reset_index(inplace=True)
    print '2', 'total household: ', transit_hh['HH_P'].sum()
    transit_hh_emp = transit_hh.merge(origin_dest_emp, on = 'parcel_id', how='left')
    transit_hh_emp['census_tract'] = transit_hh_emp['parcel_id'].map(tract_dict)
    transit_hh_emp['taz_id'] = transit_hh_emp['parcel_id'].map(taz_dict)
    transit_hh_emp['region_id'] = 1
    return transit_hh_emp

def process_auto_attribute(auto_time_data, time_max,  attr_list, origin_df, dest_df, tract_dict, taz_dict):
    # get transit information
    auto = auto_time_data[auto_time_data.travel_time <= time_max]
    #prepare orgin and destination information
    dest_auto = auto.merge(dest_df, left_on = 'to', right_on = 'TAZ_P', how = 'left')
    dest_auto = pd.DataFrame(dest_auto.groupby(dest_auto['from'])['EMPTOT_P'].sum())
    dest_auto.reset_index(inplace=True)
    origin_dest = origin_df.merge(dest_auto, left_on = 'taz_id', right_on = 'from', how = 'left')
    # groupby destination information by origin geo id 
    origin_dest_emp = pd.DataFrame(origin_dest.groupby('parcel_id')[attr_list].sum())
    origin_dest_emp.reset_index(inplace=True)
    # get the origin geo level household info
    transit_hh = pd.DataFrame(origin_df.groupby('parcel_id')['HH_P'].sum())
    transit_hh.reset_index(inplace=True)
    print '2', 'total household: ', transit_hh['HH_P'].sum()
    origin_dest_emp_hh = transit_hh.merge(origin_dest_emp, on = 'parcel_id', how='left')
    origin_dest_emp_hh['census_tract'] = origin_dest_emp_hh['parcel_id'].map(tract_dict)
    origin_dest_emp_hh['region_id'] = 1
    return origin_dest_emp_hh

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
    return transit_data_groupby

def main():

    # get geo location information
    parcel_df = pd.read_csv(os.path.join(model_path, parcel_file_name), sep = ' ')
    parcel_df['HH_P_test'] = parcel_df['HH_P']
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

    # process transit/auto time
    bank = _eb.Emmebank(os.path.join(model_path, 'Banks', bank_tod, 'emmebank'))
    if mode == 'transit':
        time_df = get_transit_information(bank)
        travel_df = process_transit_attribute(time_df, time_max, parcel_attributes_list, origin_df, dest_df, tract_dict, taz_dict)

    if mode == 'auto':
        time_df = get_auto_information(bank)
        travel_df = process_auto_attribute(time_df, time_max, parcel_attributes_list, origin_df, dest_df, tract_dict, taz_dict)

    # calculate jobs on transit time
    for geo in geo_list:
        print geo
        average_jobs_df = get_average_jobs(travel_df, geo_boundry[geo], parcel_attributes_list) 
        output_file_name = str(year) + '_' + scenario + '_' + geo +  '_' + mode + '_' +  str(time_max) + '_' + 'min.csv'
        print output_file_name
        average_jobs_df.to_csv(os.path.join(output_path, output_file_name), index=False)
    print scenario, 'done'

if __name__ == '__main__':
    main()


  



