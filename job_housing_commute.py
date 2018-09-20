import pandas as pd 
import numpy as np 
import os 
year = '2014'
time = '45'
if year == '2014':
    relative_path = r'\\license\Model Archive\T2040\soundcast_2014\outputs'
if year == '2050':
    relative_path = r'L:\vision2050\soundcast\integrated\stc\2050\outputs'
    
person = pd.read_csv(os.path.join(relative_path, r'daysim/') + r'_person.tsv', sep='\\t')
hh = pd.read_csv(os.path.join(relative_path, r'daysim/') + r'_household.tsv', sep='\\t')
job = pd.read_csv(r'U:\angela\job_housing\soundcast_2050\job_housing.csv')

# jobs accessbility
emp_name = 'EMPTOT_' + year + '_' + time + 'mins'
hh_name = 'HH_' + year + '_' + time + 'mins'
hh_tot = 'HH_' + year
job['bin'] = 0
job[hh_tot] = job[hh_tot].replace(0, np.nan)
job['job_housing_ratio'] = job[emp_name]/job[hh_name]
job.ix[job['job_housing_ratio'] <= 0.8 , 'bin'] = 'low'
job.ix[(job['job_housing_ratio'] > 0.8) & (job['job_housing_ratio'] <= 1.2), 'bin'] = 'medium low'
job.ix[(job['job_housing_ratio'] > 1.2) & (job['job_housing_ratio'] <= 1.8), 'bin'] = 'medium'
job.ix[(job['job_housing_ratio'] > 1.8) & (job['job_housing_ratio'] <= 2.0), 'bin'] = 'medium high'
job.ix[job['job_housing_ratio'] > 2.0, 'bin'] = 'high'

# commute time 
person_hh = pd.merge(person, hh, how = 'left', left_on='hhno', right_on='hhno')
workers = person_hh.loc[person['pwautime']>0]
# average & total commute time, number of workers
workers_groupby = workers.groupby('hhtaz')['pwautime'].agg(['count','sum', 'mean']).reset_index()
jobs_commute = job.merge(workers_groupby, how='outer', left_on = 'TAZ_P', right_on='hhtaz')
# commute time averaged by household 
jobs_commute['avg_commute_household'] = jobs_commute['sum']/jobs_commute[hh_tot] 
# commute time averaged by workers
jobs_commute['avg_commute_worker'] = jobs_commute['sum']/jobs_commute['count']
output_file = 'taz_' + time + '_' + year + '.csv'
jobs_commute.to_csv(os.path.join(output_path,output_file),index=False)

# grouped by bin: total worker and household number 
g1 = jobs_commute.groupby('bin')['sum', 'count', hh_tot].sum().reset_index()
# grouped by bin: average commute time 
g2 = jobs_commute.groupby('bin')['sum', 'count', hh_tot, 'avg_commute_worker', 'avg_commute_household'].mean().reset_index()
# grouped by bin: how many TAZs
print jobs_commute.groupby('bin').size()

output_path = r'T:\2018September\Angela'
file1 = 'g1_' + time + '_' + year + '.csv'
file2 = 'g2_' + time + '_' + year + '.csv'
g1.to_csv(os.path.join(output_path, file1), index=False)
g2.to_csv(os.path.join(output_path, file2), index=False)