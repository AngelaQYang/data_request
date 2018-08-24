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

model_path = '\\\license\Model Archive\T2040\soundcast_2014'
output_path = 'U:\\angela\job_housing\job_w_3_miles_distance'
parcel_file_name = 'inputs\\accessibility\\parcels_urbansim.txt'
nodes_file_name = 'inputs\\accessibility\\all_streets_nodes_2014.csv'
links_file_name = 'inputs\\accessibility\\all_streets_links_2014.csv'
geo_file_name = 'parcels_suzanne.csv'
geo = 'city'
year = 2014
res_name_list = []
distances = { # in meters; 
              # keys correspond to suffices of the resulting parcel columns
              # ORIGINAL VALUES in feet!!
             1: 2640, # 0.5 mile
             #2: 5280, # 1 mile
             3: 15840 # 3 miles
             }

geo_boundry = {'county': 'county_id',
               'city': 'city_id', 
               'taz': 'TAZ_P'}

parcel_attributes = {
              "sum": ["EMPTOT_P"],
              #"ave": [ "PPRICDYP", "PPRICHRP"]
              }
 
'''      
parcel_attributes = {
              "sum": ["HH_P", "STUGRD_P", "STUHGH_P", "STUUNI_P", 
                      "EMPMED_P", "EMPOFC_P", "EMPEDU_P", "EMPFOO_P", "EMPGOV_P", "EMPIND_P", 
                      "EMPSVC_P", "EMPOTH_P", "EMPTOT_P", "EMPRET_P",
                      "PARKDY_P", "PARKHR_P", "NPARKS", "APARKS", "daily_weighted_spaces", "hourly_weighted_spaces"],
              "ave": [ "PPRICDYP", "PPRICHRP"],
              }
'''

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
    nodes = pd.DataFrame.from_csv(os.path.join(model_path, nodes_file_name), sep = ',')
    links = pd.DataFrame.from_csv(os.path.join(model_path, links_file_name), sep = ',', index_col = None )
    geo_df = pd.DataFrame.from_csv(os.path.join(output_path, geo_file_name), sep = ',', index_col = None )
    #transit_df = pd.read_csv(os.path.join(model_path, r'inputs\accessibility\transit_stops_2014.csv'), sep = ',')

    #check for missing data!
    for col_name in parcel_df.columns:
        # daysim does not use EMPRSC_P
        if col_name <> 'EMPRSC_P':
            if parcel_df[col_name].sum() == 0:
                print col_name + ' column sum is zero! Exiting program.'
                sys.exit(1)
    parcel_df = pd.merge(parcel_df, geo_df, left_on = 'PARCELID', right_on='parcel_id', how = 'left')

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
    new_parcel_df = pd.merge(newdf, parcel_df[['node_ids', 'HH_P', geo_boundry[geo], 'PARCELID']], on="node_ids", copy=False)
    new_parcel_df = get_weighted_jobs(new_parcel_df, 'HHweighted_')
    # other geographic level jobs - weighted & averaged
    new_parcel_df_groupby = get_average_jobs(new_parcel_df, geo_boundry[geo], 'HHaveraged_')
    
    output_file_name = geo + str(year) +'_0.5_3_miles.csv'  
    new_parcel_df_groupby.to_csv(os.path.join(output_path, output_file_name))
    #return new_parcel_df_groupby

if __name__ == '__main__':
    main()
    

############# validate for the whole region ###########
file_name = geo + str(year) +'_0.5_3_miles.csv'
new_parcel_df_groupby = pd.DataFrame.from_csv(os.path.join(output_path, file_name), sep = ',', index_col = None )
total_HH_P = new_parcel_df_groupby['HH_P'].sum()
total_average_emp_p_1 = new_parcel_df_groupby['HHweighted_EMPTOT_P_1'].sum() / new_parcel_df_groupby['HH_P'].sum()
total_average_emp_p_3 = new_parcel_df_groupby['HHweighted_EMPTOT_P_3'].sum() / new_parcel_df_groupby['HH_P'].sum()

print 'total hosuehold numers in the region is ', total_HH_P
print 'average job numbers within 0.5 mile: ', total_average_emp_p_1
print 'average job numbers within 3 mile: ',  total_average_emp_p_3

