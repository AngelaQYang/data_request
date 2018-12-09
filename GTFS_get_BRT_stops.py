import pandas as pd
import numpy as np 
import os as os

dir = 'R:/Angela/transit_routes_2018/'
trips = pd.read_csv(dir + 'emme_gtfs_sum/' + 'trips.txt')
agency = pd.read_csv(dir + 'emme_gtfs_sum/' + 'agency.txt')

service_date = 20180417
start_date = 20180416
end_date = 20180418
day_of_week= 'tuesday'
#feed_list = ['KC', 'PT', 'ET', 'CT', 'KT', 'FERRY']
feed_list = ['CT', 'KC']
feed_dict = {}

def get_service_ids(calendar, calendar_dates, day_of_week, service_date):
    regular_service_dates = calendar[(calendar['start_date']<= service_date) & (calendar['end_date'] >= service_date) & (calendar[day_of_week] == 1)]['service_id'].tolist()
    exceptions_df = calendar_dates[calendar_dates['date'] == service_date]
    add_service = exceptions_df.loc[exceptions_df['exception_type'] == 1]['service_id'].tolist() 
    remove_service = exceptions_df[exceptions_df['exception_type'] == 2]['service_id'].tolist()
    service_id_list = [x for x in (add_service + regular_service_dates) if x not in remove_service]
    return service_id_list

def create_id(df, feed, id_name, psrc_id_name):
    df[psrc_id_name] = feed + '_' + df[id_name].astype(str)
    return df   

for feed in feed_list: 
    print feed
    feed_dict[feed] = {}
    # read data
    calendar = pd.read_csv(dir + feed + '_gtfs/' + 'calendar.txt')
    if os.path.exists(dir + feed + '_gtfs/'+ 'calendar_dates.txt') is False:
        calendar_dates = pd.DataFrame(columns=['service_id', 'date', 'exception_type']) 
    else:
        calendar_dates = pd.read_csv(dir + feed + '_gtfs/'+ 'calendar_dates.txt')
    trips = pd.read_csv(dir + feed + '_gtfs/' + 'trips.txt')
    stops = pd.read_csv(dir + feed + '_gtfs/' + 'stops.txt')
    stop_times = pd.read_csv(dir + feed + '_gtfs/' + 'stop_times.txt')
    routes = pd.read_csv(dir + feed + '_gtfs/' + 'routes.txt')
    shapes = pd.read_csv(dir + feed + '_gtfs/' + 'shapes.txt')
    fare_rules = pd.read_csv(dir + feed + '_gtfs/' + 'fare_rules.txt')
    fare_attributes = pd.read_csv(dir + feed + '_gtfs/' + 'fare_attributes.txt')
    agency = pd.read_csv(dir + feed + '_gtfs/' + 'agency.txt')

    # create new IDs 
    trips = create_id(trips, feed, 'trip_id', 'trip_id')
    trips = create_id(trips, feed, 'route_id', 'route_id')
    trips = create_id(trips, feed, 'shape_id', 'shape_id')
    stop_times = create_id(stop_times, feed, 'trip_id', 'trip_id')
    stop_times = create_id(stop_times, feed, 'stop_id', 'stop_id')
    stops = create_id(stops, feed, 'stop_id', 'stop_id')
    routes = create_id(routes, feed, 'route_id', 'route_id')
    shapes = create_id(shapes, feed, 'shape_id', 'shape_id')
    fare_rules = create_id(fare_rules, feed, 'fare_id', 'fare_id')
    fare_rules = create_id(fare_rules, feed, 'route_id', 'route_id')
    fare_attributes = create_id(fare_attributes, feed, 'fare_id', 'fare_id')

    # get service_id
    service_id_list = get_service_ids(calendar, calendar_dates, day_of_week, service_date)
    # calendar
    calendar_df = calendar.loc[calendar['service_id'].isin(service_id_list)]
    for ele in ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
        calendar_df[ele] = 1
    calendar_df['start_date'] = start_date
    calendar_df['end_date'] = end_date
    # calendar_dates
    calendar_dates_df = calendar_dates.loc[calendar_dates['service_id'].isin(service_id_list)]
    calendar_dates_df['service_id'] = 1
    # trips
    trips_df = trips.loc[trips['service_id'].isin(service_id_list)]
    trips_df['service_id'] = 1
    trip_id_list = np.unique(trips_df['trip_id'].tolist())
    route_id_list = np.unique(trips_df['route_id'].tolist())
    shape_id_list = np.unique(trips_df['shape_id'].tolist())
    # stop times
    stop_times_df = stop_times.loc[stop_times['trip_id'].isin(trip_id_list)]
    stop_id_list = np.unique(stop_times_df['stop_id'].tolist())
    # stops
    stops_df = stops.loc[stops['stop_id'].isin(stop_id_list)]
    # routes
    routes_df = routes.loc[routes['route_id'].isin(route_id_list)]
    routes_df['route_short_name'].fillna(routes_df['route_id'], inplace=True)
    # shapes
    shapes_df = shapes.loc[shapes['shape_id'].isin(shape_id_list)]
    # fare rules 
    fare_rules_df = fare_rules.loc[fare_rules['route_id'].isin(route_id_list)]
    fare_id_list = np.unique(fare_rules_df['fare_id'].tolist())
    # fare attributes
    fare_attributes_df = fare_attributes.loc[fare_attributes['fare_id'].isin (fare_id_list)]

    # pass data to the dictionary
    feed_dict[feed]['agency_df'] = agency
    feed_dict[feed]['calendar_df'] = calendar_df
    feed_dict[feed]['calendar_dates_df'] = calendar_dates_df
    feed_dict[feed]['trips_df'] = trips_df
    feed_dict[feed]['stop_times_df'] = stop_times_df
    feed_dict[feed]['stops_df'] = stops_df
    feed_dict[feed]['routes_df'] = routes_df
    feed_dict[feed]['shapes_df'] = shapes_df
    feed_dict[feed]['fare_rules_df'] = fare_rules_df
    feed_dict[feed]['fare_attributes_df'] = fare_attributes_df
    feed_dict[feed]['service_id_df'] = pd.DataFrame({'service_id': service_id_list})




## get route id with stop id 

def get_stops_from_routes(trips_df, stop_times_df, stops_df, seed_routes_RR):
    route_id_dict = trips_df.set_index(['trip_id']).to_dict()['route_id']
    stop_times_df['route_id'] = stop_times_df['trip_id'].map(route_id_dict)
    stop_lat_dict = stops_df.set_index(['stop_id']).to_dict()['stop_lat']
    stop_lon_dict = stops_df.set_index(['stop_id']).to_dict()['stop_lon']
    #stop_times_df['stop_lat'] = stop_times_df['stop_id'].map(stop_lat_dict)
    #stop_times_df['stop_lon'] = stop_times_df['stop_id'].map(stop_lon_dict)
    ## get KCM RR stop x,y
    seed_routes_RR_list = list(np.unique(seed_routes_RR['route_id']))
    seed_stop_times_df = stop_times_df[stop_times_df['route_id'].isin(seed_routes_RR_list)]
    seed_stop_id_list = list(np.unique(seed_stop_times_df['stop_id']))
    ## output 
    seed_final = pd.DataFrame(seed_stop_id_list, columns=['stop_id'])
    seed_final['stop_lat'] = seed_final['stop_id'].map(stop_lat_dict)
    seed_final['stop_lon'] = seed_final['stop_id'].map(stop_lon_dict)
    return seed_final

####### KCM rapid rides 
KCM_routes = feed_dict['KC']['routes_df']
KCM_routes_RR = KCM_routes[KCM_routes['route_short_name'].isin(['A Line', 'B Line', 'C Line', 'D Line', 'E Line', 'F Line'])]
KCM_trips_df = feed_dict['KC']['trips_df']
KCM_stop_times_df = feed_dict['KC']['stop_times_df']
KCM_stops_df = feed_dict['KC']['stops_df']
KCM_final = get_stops_from_routes(KCM_trips_df, KCM_stop_times_df, KCM_stops_df, KCM_routes_RR)


## CT rapid rides
CT_routes = feed_dict['CT']['routes_df']
CT_routes_RR = CT_routes[CT_routes['route_short_name'] == 'Swift']
CT_trips_df = feed_dict['CT']['trips_df']
CT_stop_times_df = feed_dict['CT']['stop_times_df']
CT_stops_df = feed_dict['CT']['stops_df']
CT_final = get_stops_from_routes(CT_trips_df, CT_stop_times_df, CT_stops_df, CT_routes_RR)

FINAL = pd.concat([KCM_final, CT_final])
output_path = r'T:\2018December\Angela\2018Network'

FINAL.to_csv(os.path.join(output_path, 'BRT_STOP_LIST_2018.csv'), index=False)
