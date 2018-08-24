import inro.emme.database.emmebank as _eb
import pandas as pd
import numpy as np
import os

transit_time_max = 60
#drive_time_max = 30
year = 2014
model_path = r'N:\T2040\soundcast_2014'
bank_tod = '7to8'
output_dir = r'S:\angela\job_housing'

parcel_df = pd.read_csv(os.path.join(model_path, r'inputs\accessibility\parcels_urbansim.txt'), sep = ' ')
parcel_df = parcel_df[['EMPTOT_P', 'TAZ_P', 'HH_P']]
parcel_df = pd.DataFrame(parcel_df.groupby('TAZ_P').sum())
parcel_df.reset_index(inplace=True)

bank = _eb.Emmebank(os.path.join(model_path, 'Banks', bank_tod, 'emmebank'))

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
transit_time_df = transit_time_df[transit_time_df.transit_time <= transit_time_max]
transit_time_df = transit_time_df.merge(parcel_df, how = 'left', left_on = 'to', right_on = 'TAZ_P')
#get the destination employment & household info 
transit_time_max_emp = pd.DataFrame(transit_time_df.groupby('from')['HH_P','EMPTOT_P'].sum())
transit_time_max_emp.reset_index(inplace=True)
transit_time_max_emp.to_csv(os.path.join(output_dir, 'transit_' + str(year) + '_' + str(transit_time_max) + '_' + 'min.csv'), index=False)
