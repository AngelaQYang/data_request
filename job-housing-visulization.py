# -*- coding: utf-8 -*-
"""
Created on Wed Feb 06 15:30:57 2019
Task: To visulize the JOB-HOUSING bin distribution 
@author: AYang
"""

import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt 
import seaborn as sns 
import os

data_path = r'U:\angela\job_housing\soundcast_2050\data_request_01_2019_JHratio'
file_name1 = 'commute_taz_2014_30mins_ampm_Current.csv'
file_name2 = 'commute_taz_2050_30mins_ampm_Transit Forcused Growth.csv'
file_name3 = 'commute_taz_2050_30mins_ampm_Stay the Course.csv'
file_name4 = 'commute_taz_2050_30mins_ampm_rug.csv'
jh_bin_CURRENT = pd.read_csv(os.path.join(data_path, 'current', file_name1))
jh_bin_TOD = pd.read_csv(os.path.join(data_path, 'TOD', file_name2))
jh_bin_STD = pd.read_csv(os.path.join(data_path, 'STD', file_name3))
jh_bin_RUG = pd.read_csv(os.path.join(data_path, 'rug', file_name4))


king = jh_bin_CURRENT[jh_bin_CURRENT['county_id'] == 33]
kitsap = jh_bin_CURRENT[jh_bin_CURRENT['county_id'] == 35]
pierce = jh_bin_CURRENT[jh_bin_CURRENT['county_id'] == 53]
snohomish = jh_bin_CURRENT[jh_bin_CURRENT['county_id'] == 61]

# hexbin map 
ax = plt.figure(figsize=(6, 6)).gca() # define axis
king.plot.hexbin(x = 'job_housing_ratio', y = 'hh_2050', gridsize = 15, ax = ax)
ax.set_title('King - job housing ratio vs household') # Give the plot a main title
ax.set_ylabel('household numbers')# Set text for y axis
ax.set_xlabel('j-h ratio')
ax.set_xlim(0.5, 2.5) # Set the limits of the y axis
ax.set_ylim(0, 3000)


#kitsap.plot.hexbin(x = 'job_housing_ratio', y = 'hh_2050', gridsize = 15, ax = ax)
#pierce.plot.hexbin(x = 'job_housing_ratio', y = 'hh_2050', gridsize = 15, ax = ax)
#snohomish.plot.hexbin(x = 'job_housing_ratio', y = 'hh_2050', gridsize = 15, ax = ax)

ax.set_title('King - job housing ratio vs household') # Give the plot a main title
ax.set_ylabel('household numbers')# Set text for y axis
ax.set_xlabel('j-h ratio')
ax.set_xlim(0.5, 2.5) # Set the limits of the y axis
ax.set_ylim(0, 3000)



plt.figure(figsize=(6, 4))
plt.subplot(2, 1, 1)
king.plot.hexbin(x = 'job_housing_ratio', y = 'hh_2050', gridsize = 15, ax = ax)
plt.xlim(0.5, 2.5)
plt.ylim(0, 3000)
plt.subplot(2, 1, 2)
kitsap.plot.hexbin(x = 'job_housing_ratio', y = 'hh_2050', gridsize = 15, ax = ax)
plt.xlim(0.5, 2.5)
plt.ylim(0, 3000)
plt.show()

