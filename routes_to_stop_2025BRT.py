'''
Date: 12/5
This data request is to get all BRT stop ids / names. 
INPUTS: BRT route shapefile, all stops shapefile. stops information has routes ID attached
Method: look up BRT routes IDs, and find out their stops from stop shapefile. 
Difficulties: the columns and data value don't have a good uniformed standard. 
Especially for text values, I have to reorganize some values to find the route ID 
Final output: a list of unique stop ids, which are on BRT routes. 
'''

import pandas as pd 
import numpy as np 


path = r'\\AWS-Prod-File01\temp\2018December\Angela\1_2025 Network'
routes_file = 'BRT-Routes-2025-Final.txt'
stops_file = 'stops.txt'
routes = pd.read_csv(os.path.join(path, routes_file))
stops = pd.read_csv(os.path.join(path, stops_file))


## make BRT routes id ready -> routes[0]
routes_line =  routes['line_name'].str.split(' ', expand=True)
routes = pd.merge(routes, routes_line[[0]], left_index=True, right_index=True)


## get all stop name with all route id 
# split the original one column into a few columns
stops_line =  stops['lines'].str.split(', ', expand=True)
stops_line = stops_line.fillna(0)
# replace 
for c in stops_line.columns:
    print c
    print stops_line[c]
    stops_line[c] =  stops_line[c].str.split(' ', expand=True)[0]
    stops_line[c] =  stops_line[c].str.split('-', expand=True)[0]
stops_line = stops_line.fillna(0)


# stack all line names together 
stops_line_stack = pd.DataFrame(stops_line.stack())
stops_line_stack.reset_index(level=1, inplace=True)
## merge to get stop name
my_stops_name = stops[['stop_name', 'stop_id', 'lines']]
my_linestack_stopname = pd.merge(my_stops_name, stops_line_stack, left_index=True, right_index=True)
my_linestack_stopname = my_linestack_stopname[['stop_name','stop_id', 'lines', 0]]


#get BRT stop names 
brt_stop_list = []
for r in routes[0]:
    stop_df = my_linestack_stopname[my_linestack_stopname[0] == r]
    brt_stop_list = brt_stop_list + list(np.unique(stop_df['stop_id']))

brt_stop_list_df = pd.DataFrame(BRT_STOP_list)
FINAL_BRT_STOP_LIST_DF = pd.DataFrame(list(np.unique(BRT_stop[0])))

output_file = 'BRT_STOP_LIST.csv'
FINAL_BRT_STOP_LIST_DF.to_csv(os.path.join(path, output_file))
