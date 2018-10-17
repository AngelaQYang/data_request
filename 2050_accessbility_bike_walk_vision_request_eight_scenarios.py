import pandas as pd 
import numpy as np 
import pandana as pdna
import os 
import re 
import sys
from pyproj import Proj, transform
sys.path.append(os.getcwd())
#from accessibility_configuration import *
#from emme_configuration import *
#from input_configuration import *

year = 2050
scenario = 'dw-jh'
geo_list = ['region', 'minority', 'poverty', 'tract']
parcel_attributes = {"sum": ['EMPTOT_P']}
res_name_list = []
distances = { # in meters; 
              # keys correspond to suffices of the resulting parcel columns
              # ORIGINAL VALUES in feet!!
             #1: 2640, # 0.5 mile
             1: 5280, # 1 mile
             3: 15840 # 3 miles
             }

geo_path = 'S:\\angela\job_housing\soundcast_2050\inputs\\accessibility'
nodes_file_name = r'S:\\angela\\job_housing\soundcast_2050\\inputs\\accessibility\\all_streets_nodes_2014.csv'
links_file_name = r'S:\\angela\\job_housing\soundcast_2050\\inputs\\accessibility\\all_streets_links_2014.csv'
geo_file_name = r'S:\\angela\\job_housing\soundcast_2050\\inputs\\accessibility\\parcel_tract_county.csv'
minority_file_name = '2016-5yr-ACS-Equity-Populations.csv'
output_path = 'S:\\angela\\job_housing\\soundcast_2050\\job_housing_commute\\vision_request_eight_scenarios'


if year == 2014:
    if scenario == 'loads':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2014'
        parcel_file_name = 'inputs\scenario\landuse\parcels_urbansim.txt' 

if year == 2050:
    if scenario == 'stc':
        model_path = r'L:\\vision2050\soundcast\integrated\draft_runs\stc\stc_run_3_2018_08_17_13_06\2050'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt'
    if scenario == 'dug-jh':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\draft_versions\dug\2050_dug_jobhh_balance_181004_run1_2018_10_01_20_37_'
        parcel_file_name = 'inputs\\scenario\\landuse\\parcels_urbansim.txt' 
    if scenario == 'dug':
        model_path = r'L:\\vision2050\soundcast\non_integrated\2050\draft_versions\dug\2050_dug_b_181005_run_4_2018_10_02_11_57'
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


geo_boundry = {'county': 'county_id',
               'city': 'city_id', 
               'taz': 'TAZ_P',
               'parcel': 'PARCELID',
               'region': 'region_id',
               'tract': 'census_tract',
               'minority': 'minority_geog',
               'poverty': 'poverty_geog'}
 

def assign_nodes_to_dataset(dataset, network, column_name, x_name, y_name):
    """Adds an attribute node_ids to the given dataset."""
    dataset[column_name] = network.get_node_ids(dataset[x_name].values, dataset[y_name].values)

 
def process_net_attribute(network, attr, fun):
    print "Processing %s" % attr
    newdf = None
    for dist_index, dist in distances.iteritems():        
        res_name = "%s_%s" % (re.sub("_?p$", "", attr), dist_index) # remove '_p' if present
        print res_name
        res_name_list.append(res_name)
        aggr = network.aggregate(dist, type=fun, decay="flat", name=attr)
        if newdf is None:
            newdf = pd.DataFrame({res_name: aggr, "node_ids": aggr.index.values})
        else:
            newdf[res_name] = aggr
    return newdf

# get household weighted/averaged jobs 
def get_weighted_jobs(household_data, new_column_name):
    for res_name in res_name_list:
          weighted_res_name = new_column_name + res_name
          household_data[weighted_res_name] = household_data[res_name]*household_data['HH_P']
          print weighted_res_name
    return household_data

def get_average_jobs(household_data, geo_boundry, new_columns_name):
    data = household_data.groupby([geo_boundry]).sum()
    data.reset_index(inplace = True)
    for res_name in res_name_list: 
         weighted_res_name = 'HHweighted_' + res_name
         averaged_res_name = new_columns_name + res_name
         data[averaged_res_name] = data[weighted_res_name]/data['HH_P']
    return data

def main():

    #read in data
    parcel_df = pd.read_csv(os.path.join(model_path, parcel_file_name), sep = ' ')
    # NOTICE: somehow nodes and links can only read by 'from_csv(path)' script. If used os.path.join, the script would exit by itself. 
    nodes = pd.DataFrame.from_csv(nodes_file_name, sep = ',')
    links = pd.DataFrame.from_csv(links_file_name, sep = ',', index_col = None )
    geo_df = pd.DataFrame.from_csv(geo_file_name, sep = ',', index_col = None )
    minority_df = pd.DataFrame.from_csv(os.path.join(geo_path, minority_file_name), sep = ',', index_col = None )
    #transit_df = pd.read_csv(os.path.join(model_path, r'inputs\accessibility\transit_stops_2014.csv'), sep = ',')

    #check for missing data!
    for col_name in parcel_df.columns:
        # daysim does not use EMPRSC_P
        if col_name <> 'EMPRSC_P':
            if parcel_df[col_name].sum() == 0:
                print col_name + ' column sum is zero! Exiting program.'
                sys.exit(1)
    parcel_df = pd.merge(parcel_df, geo_df, left_on = 'PARCELID', right_on='parcel_id', how = 'left')
    parcel_df['region_id'] = 1
    # assign impedance
    imp = pd.DataFrame(links.Shape_Length)
    imp = imp.rename(columns = {'Shape_Length':'distance'})

    # create pandana network
    net = pdna.network.Network(nodes.x, nodes.y, links.from_node_id, links.to_node_id, imp)

    for dist in distances:
        print dist
        net.precompute(dist)

    # assign network nodes to parcels, for buffer variables
    assign_nodes_to_dataset(parcel_df, net, 'node_ids', 'XCOORD_P', 'YCOORD_P')
    x, y = parcel_df.XCOORD_P, parcel_df.YCOORD_P
    parcel_df['node_ids'] = net.get_node_ids(x, y)

    # start processing attributes
    newdf = None
    for fun, attrs in parcel_attributes.iteritems():    
        for attr in attrs:
            net.set(parcel_df.node_ids, variable=parcel_df[attr], name=attr)    
            res = process_net_attribute(net, attr, fun)
            if newdf is None:
                newdf = res
            else:
                newdf = pd.merge(newdf, res, on="node_ids", copy=False)

    # parcel level jobs - weighted
    parcel_df = parcel_df.merge(minority_df, left_on = 'census_tract', right_on = 'GEOID10', how = 'left')
    new_parcel_df = pd.merge(newdf, parcel_df[['node_ids', 'HH_P', 'census_tract', 'minority_geog', 'poverty_geog', 'region_id']], on="node_ids", copy=False)

    for geo in geo_list:
        print 'processing', geo
        new_parcel_df = get_weighted_jobs(new_parcel_df, 'HHweighted_')
        # flag the minority tracts
        #new_parcel_df = new_parcel_df.merge(minority_df, left_on = 'census_tract', right_on = 'GEOID10', how = 'left')
        print new_parcel_df.head()
        new_parcel_df_groupby = get_average_jobs(new_parcel_df, geo_boundry[geo], 'HHaveraged_')
        output_file_name = str(year) + '_' + scenario + '_' + geo +'_walk0.5_bike3_miles.csv'  
        new_parcel_df_groupby.to_csv(os.path.join(output_path, output_file_name), index=False)
        print scenario, 'done'


if __name__ == '__main__':
    main()
