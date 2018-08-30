import pandas as pd
import numpy as np 
import os

INPUT_PATH = 'T:\\2018September\Angela\DataRequest_militery'
PSRC_FILE = '2014\parcels_urbansim.txt'
BLV_FILE = 'parcels_urbansim.txt'
JBLM_LIST = [3352, 3353, 3355]
PARCEL_LIST = [1500020, 1500021, 1500022, 1500023, 1500024, 1500025]
psrc_parcel_df = pd.read_csv(os.path.join(INPUT_PATH, PSRC_FILE), sep = ' ')
blv_parcel_df = pd.read_csv(os.path.join(INPUT_PATH, BLV_FILE), sep = ' ')

taz_dict = psrc_parcel_df.set_index(['parcelid']).to_dict()['taz_p']

blv_parcel_df['psrc_taz_p'] = blv_parcel_df['PARCELID'].map(taz_dict)
blv_JBLM = blv_parcel_df[blv_parcel_df['psrc_taz_p'].isin(JBLM_LIST)]
print blv_JBLM['EMPTOT_P'].sum()
psrc_JBLM = psrc_parcel_df[psrc_parcel_df['taz_p'].isin(JBLM_LIST)]
print psrc_JBLM['emptot_p'].sum()

blv_JBLM.to_csv('BKR_JBLM.csv')
psrc_JBLM.to_csv('PSRC_JBLM.csv')
