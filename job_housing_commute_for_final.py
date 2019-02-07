'''
Task:  Calculate commute information for specific j-h range (## scenario, on ## geographic level)
inputs: urbansim inputs txt file, job-housing ratio file, geographic boundry look up file
outputs: geographic classes, j-h classes, commute information, hh information
Author: AngelaY. 
Developed Time: 10/02/2018
update log: 1/2019, add some comments, update latest model runs locations
update log: 2/1, update input file - taz travel time, this file was updated from am traffic time to be (am+pm)/2
'''

import pandas as pd 
import numpy as np 
import os 
import h5py
#from statsmodels.graphics.mosaicplot import mosaic

year = '2050'
time = '30'
time_period = 'ampm'
scenario = 'Transit Forcused Growth' # stc, dug, h202, non_integrated
geo = 'county' #taz, bin, county, region
hh_tot = 'hh_' + year
bin1 = 1.0 # the threshold for job-housing ratio 
bin2 = 1.8 # the threshold for job-housing ratio 

print '**************', scenario , time_period, "*************"

### OUTPUT
output_path = r'U:\\angela\job_housing\soundcast_2050\data_request_01_2019_JHratio' 

#### INPUT 
# input for job/housing accessibility
# assume it is auto and it has to be on TAZ level, because auto/transit information only has on TAZ level
job_path = r'U:\\angela\job_housing\soundcast_2050\data_request_01_2019_JHratio'
job_file_name = 'taz' + '_auto_' + year + '_' + time + 'min_' + time_period + '_' +  scenario + '.csv' 

# input for household and commute
if year == '2014':
    #relative_path = r'\\license\Model Archive\T2040\soundcast_2014\outputs'
    #daysim_path = r'\\license\Model Archive\T2040\soundcast_2014\outputs\daysim'
    if scenario == 'Current':
        relative_path = r'L:\vision2050\soundcast\integrated\final_runs\base_year\2014\outputs' 
        daysim_path =  r'L:\vision2050\soundcast\integrated\final_runs\base_year\2014\outputs\daysim'
if year == '2050':
    if scenario == 'dug':
        relative_path = r'L:\\vision2050\soundcast\integrated\dug\2050\outputs'
        daysim_path = r'U:\angela\job_housing\soundcast_2050\inputs\2050\DUG'
    if scenario == 'stc':
        relative_path = r'L:\\vision2050\soundcast\integrated\stc\2050\outputs'
        daysim_path = r'U:\angela\job_housing\soundcast_2050\inputs\2050\STC'
    if scenario == 'non_integrated':
        relative_path = r'L:\\vision2050\soundcast\non_integrated\2050\updated_tod_top_down_2050\outputs' 
        daysim_path =  r'U:\angela\job_housing\soundcast_2050\inputs\2050\TOD'
    if scenario == 'h2o2':
        relative_path = r'L:\\vision2050\soundcast\integrated\h202\2050\outputs' 
        daysim_path =  r'U:\angela\job_housing\soundcast_2050\inputs\2050\H2O2'
    if scenario == 'rug':
        relative_path = r'L:\vision2050\soundcast\integrated\final_runs\rug\rug_run_5.run_2018_10_25_09_07\2050\outputs' 
        daysim_path =  r'L:\vision2050\soundcast\integrated\final_runs\rug\rug_run_5.run_2018_10_25_09_07\2050\outputs\daysim'
    if scenario == 'Transit Forcused Growth':
        relative_path = r'L:\\vision2050\soundcast\integrated\final_runs\tod\tod_run_8.run_2018_10_29_15_01\2050\outputs' 
        daysim_path =  r'L:\\vision2050\soundcast\integrated\final_runs\tod\tod_run_8.run_2018_10_29_15_01\2050\outputs\daysim'
    if scenario == 'Stay the Course':
        relative_path = r'L:\\vision2050\soundcast\integrated\final_runs\stc\stc_run_6.run_2018_10_23_11_15\2050\outputs' 
        daysim_path =  r'L:\\vision2050\soundcast\integrated\final_runs\stc\stc_run_6.run_2018_10_23_11_15\2050\outputs\daysim'


geo_boundry = {'county': 'county_id',
               'city': 'city_id', 
               'taz': 'TAZ_P',
               'region': 'region_id',
               'bin': 'bin'}



# input for job/housing accessibility
'''
The job / housing ratios are various by diff scenarios, so it has to be calculated BASED ON diff senarios!!!
Please make sure you run another scipt to product the newest ratio.
the output table are named by year, time buffer, scenario 
'''
job = pd.read_csv(os.path.join(job_path, job_file_name))


# read daysim files
daysim = h5py.File(daysim_path + r'\daysim_outputs.h5', "r+")
hh = pd.DataFrame(data={ 'hhtaz' : daysim['Household']['hhtaz'][:], 
                         'hhno': daysim['Household']['hhno'][:]})
ps = pd.DataFrame(data={ 'pwaudist' : daysim['Person']['pwaudist'][:], 
                         'pwautime' : daysim['Person']['pwautime'][:], 
                         #'hhtaz' : daysim['Person']['hhtaz'][:],
                         'hhno': daysim['Person']['hhno'][:]})


#person = pd.read_csv(os.path.join(relative_path, r'daysim/') + r'_person.tsv', sep='\\t')
#hh = pd.read_csv(os.path.join(relative_path, r'daysim/') + r'_household.tsv', sep='\\t')
person_hh = pd.merge(ps, hh, how = 'left', left_on='hhno', right_on='hhno')


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
jobs.ix[(jobs['job_housing_ratio'] <= bin1) & (jobs['job_housing_ratio'] >= 0) , 'bin'] = 'Housing Rich'
jobs.ix[(jobs['job_housing_ratio'] > bin1) & (jobs['job_housing_ratio'] <= bin2), 'bin'] = 'Balanced'
jobs.ix[jobs['job_housing_ratio'] > bin2, 'bin'] = 'Jobs Rich'

bin_dict = {'Housing Rich': 1, 'Balanced': 2, 'Jobs Rich': 3}
jobs['bin_id'] = jobs['bin'].map(bin_dict)



########### look into: commute distance & time from scenario files 
workers1 = person_hh.loc[ps['pwaudist']>0]
workers2 = person_hh.loc[ps['pwautime']>0]
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
output_file = 'commute_' + 'taz' + '_' + year + '_' + time + 'mins_' + time_period + '_' + scenario + '.csv'
jobs_commute.to_csv(os.path.join(output_path,output_file),index=False)


########### aggregated to bin ##############################
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
bin_dict = {'Housing Rich': 1, 'Balanced': 2, 'Jobs Rich': 3}
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
'''
# output full list of TAZ 1-3700 
jobs = hh_size.merge(job, left_on = 'hhtaz',  right_on='TAZ_P', how = 'outer')
full_list_taz = jobs.merge(workers_groupby, how='outer', left_on = 'TAZ_P', right_on='hhtaz')
validation_path = r'U:\angela\job_housing\soundcast_2050\job_housing_commute\validation'
validation_file = 'TAZ_full_list_' + year + '_' + time + 'mins_' + scenario + '.csv'
full_list_taz.to_csv(os.path.join(validation_path, validation_file), index=False)
'''

############# output final result 
if time == '30':
    #if (a == b) and (a == c+4):
        g1_file = 'commute_bin_' + str(bin1) + '_' + str(bin2) + '_' + year + '_' + time + 'mins_' + time_period + '_' + scenario + '.csv'
        g1_size.to_csv(os.path.join(output_path, g1_file), index=False)
        print 'done'

if time == '45':
    if (a == b) and (a == c):
        g1_file = 'commute_' + geo + '_' + year + '_' + time + 'mins_' + time_period + '_' +  scenario + '.csv'
        g1_size.to_csv(os.path.join(output_path, g1_file), index=False)
        print 'done'


'''
########## data request: aggregate to county and region level #########################
if geo is in ['county', 'region']:
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

'''

######### data request: job housing classes break down to every county 
if geo == 'county':
    g3 = jobs_commute.groupby(['bin', 'county_id'])['count', hh_tot, 'weighted_dist_worker', 'weighted_dist_hh', 'weighted_time_worker', 'weighted_time_hh'].sum().reset_index()
    g3_file_name = 'commute_county_bin' + '_' + year + '_' + time + 'mins_' + time_period + '_' + scenario + '.csv'
    g3.to_csv(os.path.join(output_path, g3_file_name), index=False)




