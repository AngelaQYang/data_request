import pandas as pd 
import numpy as np 
import os 
#from statsmodels.graphics.mosaicplot import mosaic

year = '2014'
time = '30'
scenario = 'non_integrated' # stc, dug, non_integrated
geo = 'bin' #taz, bin, county, region
hh_tot = 'hh_' + year
geo_boundry = {'county': 'county_id',
               'city': 'city_id', 
               'taz': 'TAZ_P',
               'region': 'region_id',
               'bin': 'bin'}

output_path = r'U:\\angela\job_housing\soundcast_2050\job_housing_commute' 


# input for job/housing accessibility
'''
The job / housing ratios are calculated by diff senarios!!!
the output table are named by year, time buffer, scenario 
'''
job_path = r'U:\\angela\job_housing\soundcast_2050\job_housing_commute'
job_file_name = 'taz' + '_auto_' + year + '_' + time + 'min_' + scenario + '.csv' 
print job_file_name
job = pd.read_csv(os.path.join(job_path, job_file_name))

# input for household and commute
if year == '2014':
    relative_path = r'\\license\Model Archive\T2040\soundcast_2014\outputs'
    model_path = r'\\license\Model Archive\T2040\soundcast_2014'
if year == '2050':
    if scenario == 'dug':
        relative_path = r'L:\\vision2050\soundcast\integrated\dug\2050\outputs'
        model_path = r'L:\\vision2050\soundcast\integrated\dug\2050'
    if scenario == 'stc':
         relative_path = r'L:\\vision2050\soundcast\integrated\stc\2050\outputs'
         model_path = r'L:\\vision2050\soundcast\integrated\stc\2050'
    if scenario == 'non_integrated':
        relative_path = r'L:\\vision2050\soundcast\non_integrated\2050\updated_tod_top_down_2050\outputs' 
        model_path =  r'L:\\vision2050\soundcast\non_integrated\2050\updated_tod_top_down_2050'

person = pd.read_csv(os.path.join(relative_path, r'daysim/') + r'_person.tsv', sep='\\t')
hh = pd.read_csv(os.path.join(relative_path, r'daysim/') + r'_household.tsv', sep='\\t')
person_hh = pd.merge(person, hh, how = 'left', left_on='hhno', right_on='hhno')

########## get household number from scenario files 
'''
 get the updated household number from daysim file (because hosuehold numbers are distributed differently based on diff scnearios)
 '''
hh_size = hh.groupby(['hhtaz']).size().reset_index(False)
# create a new column in job file for the updated household numbers
hh_size.columns = ['hhtaz', hh_tot]
jobs = hh_size.merge(job, left_on = 'hhtaz',  right_on='TAZ_P', how = 'outer')
# to avoid the job/housing ratio become infinity, replace 0 household number to nan
jobs[hh_tot] = jobs[hh_tot].replace(0, np.nan)
print jobs.head(2)

# calculate job/housing ratio
'''
emp_name = 'EMPTOT_P' + year + '_' + time + 'mins'
hh_name = 'HH_' + year + '_' + time + 'mins'
'''
emp_name = 'HHaveraged_EMPTOT_P'
hh_name = 'HHaveraged_HH_P_test'
jobs['job_housing_ratio'] = jobs[emp_name]/jobs[hh_name]

#category ratios to bins
jobs.ix[(jobs['job_housing_ratio'] <= 0.8) & (jobs['job_housing_ratio'] >= 0) , 'bin'] = 'Very Housing Rich'
jobs.ix[(jobs['job_housing_ratio'] > 0.8) & (jobs['job_housing_ratio'] <= 1.2), 'bin'] = 'Housing Rich'
jobs.ix[(jobs['job_housing_ratio'] > 1.2) & (jobs['job_housing_ratio'] <= 1.8), 'bin'] = 'Balanced'
jobs.ix[(jobs['job_housing_ratio'] > 1.8) & (jobs['job_housing_ratio'] <= 2.0), 'bin'] = 'Jobs Rich'
jobs.ix[jobs['job_housing_ratio'] > 2.0, 'bin'] = 'Very Jobs Rich'

bin_dict = {'Very Housing Rich': 1, 'Housing Rich': 2, 'Balanced': 3, 'Jobs Ric':4, 'Very Jobs Rich':5}
jobs['bin_id'] = jobs['bin'].map(bin_dict)

# get commute distance & time from scenario files 
workers1 = person_hh.loc[person['pwaudist']>0]
workers2 = person_hh.loc[person['pwautime']>0]
workers_groupby1 = workers1.groupby('hhtaz')['pwaudist'].agg(['count','mean']).reset_index()
workers_groupby1.rename(columns = {'mean':'mean_dist'}, inplace = True)
workers_groupby2 = workers2.groupby('hhtaz')['pwautime'].agg(['mean']).reset_index()
workers_groupby2.rename(columns = {'mean':'mean_time'}, inplace = True)
workers_groupby = workers_groupby1.merge(workers_groupby2, how = 'left', on='hhtaz')
'''
NOTE: exclude the TAZ with 0 workers/commuters, use left join to worker groups
'''
jobs_commute = workers_groupby.merge(jobs, how='left', left_on = 'hhtaz', right_on='TAZ_P')

# save TAZ file 
output_file = 'commute_' + 'taz' + '_' + year + '_' + time + 'mins_' + scenario + '.csv'
jobs_commute.to_csv(os.path.join(output_path,output_file),index=False)

###########aggregated to bin##############################
# weighted by household numbers: 'hh_tot' or worker number: 'count'
jobs_commute['weighted_dist_hh'] = jobs_commute[hh_tot] * jobs_commute['mean_dist']
jobs_commute['weighted_dist_worker'] = jobs_commute['count'] * jobs_commute['mean_dist']
jobs_commute['weighted_time_hh'] = jobs_commute[hh_tot] * jobs_commute['mean_time']
jobs_commute['weighted_time_worker'] = jobs_commute['count'] * jobs_commute['mean_time']

# grouped by bin: total worker and household number 
g1 = jobs_commute.groupby('bin')['count', hh_tot, 'weighted_dist_worker', 'weighted_dist_hh', 'weighted_time_worker', 'weighted_time_hh'].sum().reset_index()
g1['avg_dist_worker'] = g1['weighted_dist_worker']/g1['count']
g1['avg_dist_hh'] = g1['weighted_dist_hh']/g1[hh_tot]
g1['avg_time_worker'] = g1['weighted_time_worker']/g1['count']
g1['avg_time_hh'] = g1['weighted_time_hh']/g1[hh_tot]

# proportion of worker/household
wk_sum = g1['count'].sum()
hh_sum = g1[hh_tot].sum()
g1['prop_worker'] = g1['count'] / wk_sum
g1['prop_hh'] = g1[hh_tot] / hh_sum

# assign bin id
bin_dict = {'Very Housing Rich': 1, 'Housing Rich': 2, 'Balanced': 3, 'Jobs Rich':4, 'Very Jobs Rich':5}
g1['bin_id'] = g1['bin'].map(bin_dict)
g1 = g1.sort_values('bin_id')
# assign taz size
taz_size = jobs_commute.groupby('bin').size().reset_index(False)
taz_size.columns = ['bin', 'taz_count']
g1_size = g1.merge(taz_size, on = 'bin', how='left')

# validation 
a = len(np.unique(workers1['hhtaz']))
b = len(np.unique(workers2['hhtaz']))
c = np.int(g1_size['taz_count'].sum())
d = g1_size['count'].sum()
e = g1_size[hh_tot].sum()

print 'number of TAZ has valid commute distance:', len(np.unique(workers1['hhtaz']))
print 'number of TAZ has valid commute time:', len(np.unique(workers2['hhtaz']))
print 'number of total TAZ in final table:', g1_size['taz_count'].sum()
print year, 'year: total number of workers: ', g1_size['count'].sum()
print year, 'year: total number of households: ', g1_size[hh_tot].sum()

# output full list of TAZ 1-3700 
jobs = hh_size.merge(job, left_on = 'hhtaz',  right_on='TAZ_P', how = 'outer')
full_list_taz = jobs.merge(workers_groupby, how='outer', left_on = 'TAZ_P', right_on='hhtaz')
validation_path = r'U:\angela\job_housing\soundcast_2050\job_housing_commute\validation'
validation_file = 'TAZ_full_list_' + year + '_' + time + 'mins_' + scenario + '.csv'
full_list_taz.to_csv(os.path.join(validation_path, validation_file), index=False)


############# output final result 
if time == '30':
    if (a == b) and (a == c+4):
        g1_file = 'commute_bin_' + year + '_' + time + 'mins_' + scenario + '.csv'
        g1_size.to_csv(os.path.join(output_path, g1_file), index=False)
        print 'done'

if time == '45':
    if (a == b) and (a == c):
        g1_file = 'commute_' + geo + '_' + year + '_' + time + 'mins_' + scenario + '.csv'
        g1_size.to_csv(os.path.join(output_path, g1_file), index=False)
        print 'done'





########## aggregate to county and region level #########################
# weighted by household numbers: 'hh_tot' or worker number: 'count'
job_commute_file_name = 'commute_' + 'taz' + '_' + year + '_' + time + 'mins_' + scenario + '.csv'
jobs_commute = pd.read_csv(os.path.join(output_path,job_commute_file_name))
# weighted by household numbers: 'hh_tot' or worker number: 'count'
jobs_commute['weighted_dist_hh'] = jobs_commute[hh_tot] * jobs_commute['mean_dist']
jobs_commute['weighted_dist_worker'] = jobs_commute['count'] * jobs_commute['mean_dist']
jobs_commute['weighted_time_hh'] = jobs_commute[hh_tot] * jobs_commute['mean_time']
jobs_commute['weighted_time_worker'] = jobs_commute['count'] * jobs_commute['mean_time']
g2 = jobs_commute.groupby(geo_boundry[geo])['count', hh_tot, 'weighted_dist_worker', 'weighted_dist_hh', 'weighted_time_worker', 'weighted_time_hh'].sum().reset_index()

g2['avg_dist_worker'] = g2['weighted_dist_worker']/g2['count']
g2['avg_dist_hh'] = g2['weighted_dist_hh']/g2[hh_tot]
g2['avg_time_worker'] = g2['weighted_time_worker']/g2['count']
g2['avg_time_hh'] = g2['weighted_time_hh']/g2[hh_tot]
wk_sum = g2['count'].sum()
hh_sum = g2[hh_tot].sum()
g2['prop_worker'] = g2['count'] / wk_sum
g2['prop_hh'] = g2[hh_tot] / hh_sum
taz_size = jobs_commute.groupby(geo_boundry[geo]).size().reset_index(False)
taz_size.columns = [geo_boundry[geo], 'taz_count']
g2_size = g2.merge(taz_size, on = geo_boundry[geo], how='left')
g2_file_name = 'commute_' + geo + '_' + year + '_' + time + 'mins_' + scenario + '.csv'
g2_size.to_csv(os.path.join(output_path, g2_file_name), index=False)



######## split by mode

trip = pd.read_csv(os.path.join(relative_path,r'daysim/') + r'_trip.tsv', sep='\t')
# homebased work mode share

trip['Trip Type'] = 'Not Home-Based Work'
trip.ix[(((trip['opurp']==0) & (trip['dpurp']==1)) | ((trip['opurp']==1) & (trip['dpurp']==0))),'Trip Type']= 'Home-Based Work'
hbw_trips = trip.loc[trip['Trip Type']=='Home-Based Work']
model_df_hbw = hbw_trips[['mode','trexpfac']].groupby(['mode']).sum()[['trexpfac']]/hbw_trips[['trexpfac']].sum()
model_df_hbw.reset_index(inplace=True)
mode_dict = {0:'Other',1:'Walk',2:'Bike',3:'SOV',4:'HOV2',5:'HOV3+',6:'Transit',8:'School Bus'}
model_df_hbw.replace({'mode':mode_dict}, inplace=True)
model_df_hbw.columns = ['mode', 'share']
model_df_hbw
