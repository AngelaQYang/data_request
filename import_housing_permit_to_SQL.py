# -*- coding: utf-8 -*-
"""
Created on Tue Dec 25 15:38:37 2018
@author: AYang
Project: 2017 housing permit inventory
Task: convert jurisdictions' 2017 permit from csv file to SQL database 
"""

import pandas as pd
import pyodbc
import numpy as np
import os 


## 1. SQL Connection to our internal SQL database Elmer
sql_conn = pyodbc.connect('DRIVER={SQL Server}; SERVER=sql2016\DSADEV;DATABASE=Sandbox;trusted_connection=true')
cursor = sql_conn.cursor()


## 2. get the data and prepare the data for SQL read
KITSAP_JURIS = ['BREMERTON', 'PORTORCHARD', 'POULSBO', 'UNINCORPORATED']
JURI = 'BREMERTON'
COUNTY = 'Kitsap'
COUNTY_CODE = '35'
DATA_PATH = r'J:\Projects\Permits\17Permit\database\working'


juri_name = 't' + COUNTY_CODE + JURI + '_final.txt'
my_juri = pd.DataFrame.from_csv(os.path.join(DATA_PATH, COUNTY, juri_name), sep=',', index_col=None)


final_data = my_juri    
    
## convert empty string as numpy nan value
'''
replace panda or numpy value none to NULL, or the SQL server won't recogonize the NULL value.  
referrence: https://stackoverflow.com/questions/14162723/replacing-pandas-or-numpy-nan-with-a-none-to-use-with-mysqldb
'''

final_data.replace('', np.nan, inplace=True)
print (np.where(final_data.applymap(lambda x: x == '')))
final_data = final_data.where((pd.notnull(final_data)), None)


# add one more column to process duplicated data from current year to previous years
final_data['CHECK_DUPLICATED'] = 0

          
## 3. get the existing table list from the SQL database
print ('Getting a lsit of the tables in Elmer (the Central Database)')
table_names = []
for rows in cursor.tables():
    if rows.table_type == "TABLE":
        table_names.append(rows.table_name)
        
print (table_names)



    


## 4. if the table name already duplicated in the SQL database (Sandbox here), then delete the previous table!  Please be very careful of this step!!!! 
table_exists = my_tablename in table_names
if table_exists == True:
    print 'There is currently a table named ' + my_tablename + ', removing the older table'
    sql_statement = 'drop table ' + my_tablename
    cursor.execute(sql_statement)
    sql_conn.commit()



## 5. Creating a new table named HHSurvey_trips in Sandbox to hold the maintain the HH survey trips
sql_statement1 = 'create table ' + my_tablename 
sql_statement2 = '( ' + \
                 'ID INT NULL, ' + \
                 'PSRCIDN varchar(255) NULL, ' + \
                 'PERMITNO varchar(255) NULL, ' + \
                 'SORT varchar(255) NULL, ' + \
                 'MULTIREC varchar(255) NULL, ' + \
                 'PIN varchar(255) NULL, ' + \
                 'ADDRESS varchar(255) NULL, ' + \
                 'HOUSENO varchar(255) NULL, ' + \
                 'PREFIX varchar(255) NULL, ' + \
                 'STRNAME varchar(255) NULL, ' + \
                 'STRTYPE varchar(255) NULL, ' + \
                 'SUFFIX varchar(255) NULL, ' + \
                 'UNIT_BLD varchar(255) NULL, ' + \
                 'ZIP INT NULL, ' + \
                 'ISSUED datetime NULL, ' + \
                 'FINALED datetime NULL, ' + \
                 'STATUS varchar(255) NULL, ' + \
                 'TYPE INT NULL, ' + \
                 'PS INT NULL, ' + \
                 'UNITS varchar(255) NULL, ' + \
                 'BLDGS varchar(255) NULL, ' + \
                 'LANDUSE varchar(255) NULL, ' + \
                 'CONDO varchar(255) NULL, ' + \
                 'VALUE varchar(255) NULL, ' + \
                 'ZONING varchar(255) NULL, ' + \
                 'NOTES varchar(255) NULL, ' + \
                 'NOTES2 varchar(255) NULL, ' + \
                 'NOTES3 varchar(255) NULL, ' + \
                 'NOTES4 varchar(255) NULL, ' + \
                 'NOTES5 varchar(255) NULL, ' + \
                 'NOTES6 varchar(255) NULL, ' + \
                 'NOTES7 varchar(255) NULL, ' + \
                 'LOTSIZE varchar(255) NULL, ' + \
                 'PIN_PARENT varchar(255) NULL, ' + \
                 'COUNTY varchar(255) NULL, ' + \
                 'JURIS varchar(255) NULL, ' + \
                 'JURIS15 INT NULL, ' + \
                 'PLC INT NULL, ' + \
                 'PLC15 varchar(255) NULL, ' + \
                 'PROJYEAR INT NULL, ' + \
                 'CNTY INT NULL, ' + \
                 'MULTCNTY varchar(255) NULL, ' + \
                 'PSRCID varchar(255) NULL, ' + \
                 'PSRCIDXY varchar(255) NULL, ' + \
                 'X_COORD varchar(255) NULL, ' + \
                 'Y_COORD varchar(255) NULL, ' + \
                 'RUNTYPE INT NULL, '+\
                 'CHECK_DUPLICATED INT NULL)'

                    


sql_statement = sql_statement1  + sql_statement2                                                                                                                   
cursor.execute(sql_statement)
sql_conn.commit()


## 6. Insert the data into datatable 
print 'Add data to ' + my_tablename + ' in Sandbox'

## put the column name into a list
str_c = ''
for c in final_data.columns.tolist()[0:]:
    print c
    str_c = str_c + '[' + c + '],'   
str_c = str_c[:-1] 
print (str_c)


for index,row in final_data.iterrows():
    #print (index)
    #print (row)
    sql_state = 'INSERT INTO ' + my_tablename + '(' + str_c + ')' + 'values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    cursor.execute(sql_state, row['ID'],row['PSRCIDN'],row['PERMITNO'],row['SORT'],row['MULTIREC'],
                   row['PIN'],row['ADDRESS'],row['HOUSENO'],row['PREFIX'],row['STRNAME'],
                   row['STRTYPE'],row['SUFFIX'],row['UNIT_BLD'],row['ZIP'],row['ISSUED'],
                   row['FINALED'],row['STATUS'],row['TYPE'],row['PS'],row['UNITS'],
                   row['BLDGS'],row['LANDUSE'],row['CONDO'],row['VALUE'],row['ZONING'],
                   row['NOTES'],row['NOTES2'],row['NOTES3'],row['NOTES4'],row['NOTES5'],
                   row['NOTES6'],row['NOTES7'],row['LOTSIZE'],row['PIN_PARENT'],row['COUNTY'],
                   row['JURIS'],row['JURIS15'],row['PLC'],row['PLC15'],row['PROJYEAR'],
                   row['CNTY'],row['MULTCNTY'],row['PSRCID'],row['PSRCIDXY'],row['X_COORD'],
                   row['Y_COORD'],row['RUNTYPE'],row['CHECK_DUPLICATED']) 
    sql_conn.commit()
    
    
    
## 7. finished load data
print 'Closing the central database'
sql_conn.close()
















