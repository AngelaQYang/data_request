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

   

def read_data(DATA_PATH, COUNTY, COUNTY_CODE, JURI):
    juri_name = 't' + COUNTY_CODE + JURI + '_final.txt'
    my_juri = pd.DataFrame.from_csv(os.path.join(DATA_PATH, COUNTY, juri_name), sep=',', index_col=None)
    return my_juri

       
def data_process(my_data, col1):
    my_data[col1] = 0 
    if 'PIN_PARENT' not in my_data.columns:
        my_data['PIN_PARENT'] = np.nan
    if 'COUNTY' not in my_data.columns:
        my_data['COUNTY'] = COUNTY   
    my_data = my_data[[u'ID', u'PSRCIDN', u'PERMITNO', u'SORT', u'MULTIREC', u'PIN',
       u'ADDRESS', u'HOUSENO', u'PREFIX', u'STRNAME', u'STRTYPE', u'SUFFIX',
       u'UNIT_BLD', u'ZIP', u'ISSUED', u'FINALED', u'STATUS', u'TYPE', u'PS',
       u'UNITS', u'BLDGS', u'LANDUSE', u'CONDO', u'VALUE', u'ZONING', u'NOTES',
       u'NOTES2', u'NOTES3', u'NOTES4', u'NOTES5', u'NOTES6', u'NOTES7',
       u'LOTSIZE', u'JURIS', u'JURIS15', u'PLC', u'PLC15', u'PROJYEAR',
       u'CNTY', u'MULTCNTY', u'PSRCID', u'PSRCIDXY', u'X_COORD', u'Y_COORD',
       u'RUNTYPE', u'CHECK_DUPLICATED', u'PIN_PARENT', u'COUNTY']]
    return my_data


def process_null_data(my_data):
    my_data.replace('', np.nan, inplace=True)
    print (np.where(my_data.applymap(lambda x: x == '')))
    my_data = my_data.where((pd.notnull(my_data)), None)
    return my_data



def get_table_from_elmer():
    table_names = []
    for rows in cursor.tables():
        if rows.table_type == "TABLE":
            table_names.append(rows.table_name)
    return table_names


def check_duplicated_table(table_names, my_tablename):
    table_exists = my_tablename in table_names
    if table_exists == True:
        print 'There is currently a table named ' + my_tablename + ', removing the older table'
        sql_statement = 'drop table ' + my_tablename
        cursor.execute(sql_statement)
        sql_conn.commit()


def get_col_list(my_data):
    str_c = ''
    for c in final_data.columns.tolist()[0:]:
        print c
        str_c = str_c + '[' + c + '],'   
    str_c = str_c[:-1] 
    print ('data table column names are')
    print (str_c)
    return str_c
    
    
def create_data_table_in_SQL(sql_statement):                                                                                                           
    cursor.execute(sql_statement)
    sql_conn.commit()


def insert_data_into_SQL(my_data, str_c, my_tablename):
    for index,row in my_data.iterrows():
        #print (index)
        #print (row)
        sql_state = 'INSERT INTO ' + my_tablename + '(' + str_c + ')' + 'values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)' 
        cursor.execute(sql_state, row['ID'],row['PSRCIDN'],row['PERMITNO'],row['SORT'],row['MULTIREC'],
                                  row['PIN'],row['ADDRESS'],row['HOUSENO'],row['PREFIX'],row['STRNAME'],
                                  row['STRTYPE'],row['SUFFIX'],row['UNIT_BLD'],row['ZIP'],row['ISSUED'],
                                  row['FINALED'],row['STATUS'],row['TYPE'],row['PS'],row['UNITS'],
                                  row['BLDGS'],row['LANDUSE'],row['CONDO'],row['VALUE'],row['ZONING'],
                                  row['NOTES'],row['NOTES2'],row['NOTES3'],row['NOTES4'],row['NOTES5'],
                                  row['NOTES6'],row['NOTES7'],row['LOTSIZE'],
                                  row['JURIS'],row['JURIS15'],row['PLC'],row['PLC15'],row['PROJYEAR'],
                                  row['CNTY'],row['MULTCNTY'],row['PSRCID'],row['PSRCIDXY'],row['X_COORD'],
                                  row['Y_COORD'],row['RUNTYPE'],row['CHECK_DUPLICATED'], row['PIN_PARENT'],row['COUNTY']) 
        sql_conn.commit()
        
        
## 1. SQL Connection to our internal SQL database Elmer
sql_conn = pyodbc.connect('DRIVER={SQL Server}; SERVER=sql2016\DSADEV;DATABASE=Sandbox;trusted_connection=true')
cursor = sql_conn.cursor()


## 2. prepare data
## get the data and prepare the data for SQL read
KITSAP_JURIS = ['BREMERTON', 'PORTORCHARD', 'POULSBO', 'UNINCORPORATED']
#JURI = 'POULSBO'
for JURI in KITSAP_JURIS:
    COUNTY = 'Kitsap'
    COUNTY_CODE = '35'
    DATA_PATH = r'J:\Projects\Permits\17Permit\database\working'
    my_tablename = JURI + '17'  
    
    ## set up the SQL data table structure (the data types would change from table to table)
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
                 'CHECK_DUPLICATED INT NULL, ' +\
                 'PIN_PARENT varchar(255) NULL, ' + \
                 'COUNTY varchar(255) NULL)'
    
    
    final_data = read_data(DATA_PATH, COUNTY, COUNTY_CODE, JURI)
    final_data = data_process(final_data, 'CHECK_DUPLICATED')
    final_data = process_null_data(final_data)
    table_names = get_table_from_elmer()
    print table_names
    check_duplicated_table(table_names, my_tablename)  
    str_c = get_col_list(final_data)

    ## 3. exactive the SQL process                 
    sql_statement = sql_statement1  + sql_statement2  
    create_data_table_in_SQL(sql_statement)
    insert_data_into_SQL(final_data, str_c, my_tablename)
    print (JURI)
    print ('-----------------finished------------------')
    
print ('Closing the central database')
sql_conn.close()


