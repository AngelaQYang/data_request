# run this scripts on model server 2, be aware the folder directory names are different among servers
import inro.emme.database.emmebank as _eb
import pandas as pd
import numpy as np
import os

year = 2014
scenario = 'Current' # stc, dug, non_integrated, h202, base_year
geo = 'taz'
travel_time_max = 30
bank_tod = '7to8'
parcel_attributes_list = ['EMPTOT_P', 'HH_P_test']
mode = 'auto'

taz_sample_id = 2403
taz_sample_output_file = 'S:\angela\job_housing\soundcast_2050\job_housing_commute\final_set\sample_taz_2403.CSV'
output_path = 'S:\\angela\job_housing\soundcast_2050\job_housing_commute\data request_aviation_02222019'
transit_time_df = r'S:\angela\job_housing\soundcast_2050\job_housing_commute\data request_aviation_02222019\transit_time_2403.csv'
auto_time_df = r'S:\angela\job_housing\soundcast_2050\job_housing_commute\data request_aviation_02222019\veh_time_2403.csv'
taz_parcel_file = r'1414_taz_sample_output_file.csv'
from_sample_taz_file = r'from_2403_taz.csv'
to_sample_taz_file = r'to_2403_taz.csv'

if year == 2014:
    if scenario == 'Current':
        model_path = r'L:\\vision2050\soundcast\integrated\final_runs\base_year\2014'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt' #for 2014


        '''
if year == '2014':
    #relative_path = r'\\license\Model Archive\T2040\soundcast_2014\outputs'
    #daysim_path = r'\\license\Model Archive\T2040\soundcast_2014\outputs\daysim'
    if scenario == 'Current':
        relative_path = r'L:\vision2050\soundcast\integrated\final_runs\base_year\2014\outputs' 
        daysim_path =  r'L:\vision2050\soundcast\integrated\final_runs\base_year\2014\outputs\daysim'
        '''

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


geo_path = 'S:\\angela\job_housing\soundcast_2050'
geo_file_name = 'inputs\\accessibility\\parcels_suzanne.csv'
geo_boundry = {'county': 'county_id',
               'city': 'city_id',
               'city_name': 'city_name', 
               'taz': 'TAZ_P',
               'region': 'region_id'}

# get transit information
def get_transit_information(bank):
    bus_time = bank.matrix('auxwa').get_numpy_data() + bank.matrix('twtwa').get_numpy_data() + bank.matrix('ivtwa').get_numpy_data() 
    rail_time = bank.matrix('auxwr').get_numpy_data() + bank.matrix('twtwr').get_numpy_data() + bank.matrix('ivtwr').get_numpy_data() 
    transit_time = np.minimum(bus_time, rail_time)
    transit_time = transit_time[0:3700, 0:3700]
    transit_time_df = pd.DataFrame(transit_time)
    transit_time_df['from'] = transit_time_df.index
    transit_time_df = pd.melt(transit_time_df, id_vars= 'from', value_vars=list(transit_time_df.columns[0:3700]), var_name = 'to', value_name='time')
    # add 1 into zone id before join with parcel data
    transit_time_df['to'] = transit_time_df['to'] + 1 
    transit_time_df['from'] = transit_time_df['from'] + 1
    print transit_time_df.to_csv(transit_time_df)
    return transit_time_df


# get auto information
def get_auto_information(bank):
    time = bank.matrix('svtl2t').get_numpy_data() 
    veh_time = time[0:3700, 0:3700]
    veh_time_df = pd.DataFrame(veh_time)
    veh_time_df['from'] = veh_time_df.index
    veh_time_df = pd.melt(veh_time_df, id_vars= 'from', value_vars=list(veh_time_df.columns[0:3700]), var_name = 'to', value_name='time')
    # add 1 into zone id before join with parcel data
    veh_time_df['to'] = veh_time_df['to'] + 1 
    veh_time_df['from'] = veh_time_df['from'] + 1
    print veh_time_df.to_csv(auto_time_df)
    return veh_time_df



def process_attribute(time_data, time_max, parcel_taz_id, transit_taz_id, attr_list, origin_df, dest_df):
    # get destination information
    time_df = time_data[time_data.time <= time_max]
    

    #### from taz sample, where we can arrive in 30 mins auto
    time_df_from_sample_taz = time_df.merge(dest_df, how = 'left', left_on = 'to', right_on = parcel_taz_id)
    # groupby destination information by origin TAZ id 
    sample_parcel = time_df_from_sample_taz[time_df_from_sample_taz['from'] == taz_sample_id]
    sample_parcel.to_csv(taz_parcel_file)

    # define the unique to-taz
    list = np.unique(sample_parcel['to'])
    sample_taz = pd.DataFrame(list, columns = ['to'])
    sample_taz['from'] = taz_sample_id
    sample_taz.to_csv(from_sample_taz_file)


    #### to taz file, how far we can departure in 30 mins auto
    time_df_to_sample_taz = time_df.merge(origin_df, how = 'left', left_on = 'from', right_on = parcel_taz_id)
    # groupby destination information by destination TAZ id 
    sample_parcel = time_df_to_sample_taz[time_df_to_sample_taz['to'] == taz_sample_id]
    #sample_parcel.to_csv(taz_parcel_file)
    # define the unique to-taz
    list = np.unique(sample_parcel['from'])
    sample_taz = pd.DataFrame(list, columns = ['from'])
    sample_taz['to'] = taz_sample_id
    sample_taz.to_csv(to_sample_taz_file)


    print 'yes, printed the sample TAZ', taz_sample_id
    time_df_emp = pd.DataFrame(time_df.groupby(transit_taz_id)[attr_list].sum())
    time_df_emp.reset_index(inplace=True)
    print time_df_emp.head()
    time_df_df = pd.merge(time_df_emp, origin_df, left_on = transit_taz_id, right_on = parcel_taz_id)
    print time_df_df.head()
    print '2.3'
    return time_df_df


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
    parcel_df['HH_P_test'] = parcel_df['HH_P']
    geo_df = pd.DataFrame.from_csv(os.path.join(geo_path, geo_file_name), sep = ',', index_col = None )
    geo_df = pd.merge(parcel_df, geo_df, left_on = 'PARCELID', right_on = 'parcel_id')
    city_dict = geo_df.set_index(['TAZ_P']).to_dict()['city_id']
    county_dict = geo_df.set_index(['TAZ_P']).to_dict()['county_id']
    city_name_dict = geo_df.set_index(['TAZ_P']).to_dict()['city_name']
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
    # process transit time or auto time
    bank = _eb.Emmebank(os.path.join(model_path, 'Banks', bank_tod, 'emmebank'))
    if mode == 'auto':
        time_df = get_auto_information(bank)
    if mode == 'transit':
        time_df = get_transit_information(bank)
    my_df = process_attribute(time_df, travel_time_max, 'TAZ_P', 'from', parcel_attributes_list, origin_df, dest_df)
    # calculate jobs on transit time
    #average_jobs_df = get_average_jobs(my_df, geo_boundry[geo], parcel_attributes_list) 
    #output_file_name = geo + '_auto_' + str(year) + '_' + str(travel_time_max) + 'min_' + scenario + '.csv'
    #average_jobs_df.to_csv(os.path.join(output_path, output_file_name), index=False)
    #print 'output file name is: ', output_file_name
    #return average_jobs_df
    print year, scenario, travel_time_max
    print 'done'

if __name__ == '__main__':
    main()

    

########### validation ############ 
#average_jobs_df['HH_P'].sum()
#average_jobs_df['EMPTOT_P'].sum()




