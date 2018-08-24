import pandas as pd 
import numpy as np 
import pandana as pdna 
import sys
import os 
import re 
from pyproj import Proj, transform
sys.path.append(os.getcwd())
model_path = '\\\license\Model Archive\T2040\soundcast_2014'
output_path = 'U:\\angela\job_housing'

parcel_file = 'inputs\\accessibility\\parcels_urbansim.txt'
household_file = 'outputs\\daysim\\_household.tsv'
nodes_file_name = 'inputs\\accessibility\\all_streets_nodes_2014.csv'
links_file_name = 'inputs\\accessibility\\all_streets_links_2014.csv'

res_name_list = []

distances = { # in meters; 
              # keys correspond to suffices of the resulting parcel columns
              # ORIGINAL VALUES in feet!!
             1: 2640, # 0.5 mile
             #2: 5280, # 1 mile
             3: 15840 # 3 miles
             }

parcel_attributes = {
              "sum": ["HH_P" ,"hhincome"],
              #"ave": [ "PPRICDYP", "PPRICHRP"]
              }

household_df = pd.read_csv(os.path.join(model_path, household_file), sep = '\t')
household_df = household_df.groupby(['hhparcel'])[['hhincome']].sum()
household_df.reset_index(inplace = True)
parcel_df = pd.read_csv(os.path.join(model_path, parcel_file), sep = ' ')
parcel_df = pd.merge(parcel_df, household_df, left_on = 'PARCELID', right_on='hhparcel', how = 'left' )

for col_name in parcel_df.columns:
        # daysim does not use EMPRSC_P
        if col_name <> 'EMPRSC_P':
            if parcel_df[col_name].sum() == 0:
                print col_name + ' column sum is zero! Exiting program.'
                sys.exit(1)

nodes = pd.DataFrame.from_csv(os.path.join(model_path, nodes_file_name), sep = ',')
links = pd.DataFrame.from_csv(os.path.join(model_path, links_file_name), sep = ',', index_col = None )
imp = pd.DataFrame(links.Shape_Length)
imp = imp.rename(columns = {'Shape_Length':'distance'})
# create pandana network
net = pdna.network.Network(nodes.x, nodes.y, links.from_node_id, links.to_node_id, imp)

for dist in distances:
    print dist
    net.precompute(dist)

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


assign_nodes_to_dataset(parcel_df, net, 'node_ids', 'XCOORD_P', 'YCOORD_P')
x, y = parcel_df.XCOORD_P, parcel_df.YCOORD_P
parcel_df['node_ids'] = net.get_node_ids(x, y)

newdf = None
for fun, attrs in parcel_attributes.iteritems():    
     for attr in attrs:
         net.set(parcel_df.node_ids, variable=parcel_df[attr], name=attr)    
         res = process_net_attribute(net, attr, fun)
         if newdf is None:
             newdf = res
         else:
             newdf = pd.merge(newdf, res, on="node_ids", copy=False)


new_parcel_df = pd.merge(newdf, parcel_df[['node_ids', 'HH_P', 'PARCELID']], on="node_ids", copy=False)
# city? county? 
#new_parcel_df = new_parcel_df.groupby(geo_id).sum()
#new_parcel_df.reset_index(inplace = True)
new_parcel_df['average_income_1'] = new_parcel_df['hhincome_1']/new_parcel_df['HH_P_1']
new_parcel_df['average_income_3'] = new_parcel_df['hhincome_3']/new_parcel_df['HH_P_3']
new_parcel_df.to_csv(os.path.join(output_path, 'parcel_2014_w_0.5_3_miles_income.csv'))


############## for the whole region ###########
# average income per household 
average_income = parcel_df['hhincome'].sum()/parcel_df['HH_P'].sum()
print 'average income per household :', average_income

'''
# buffer area mean doesn't work
average_income_w_halfmile = new_parcel_df['hhincome_1'].sum()/new_parcel_df['HH_P_1'].sum()
print 'average income per household 0.5 miles buffer :', average_income_w_halfmile
average_income_w_3mile = new_parcel_df['hhincome_3'].sum()/new_parcel_df['HH_P_3'].sum()
print 'average income per household 3 miles buffer :', average_income_w_3mile
'''

