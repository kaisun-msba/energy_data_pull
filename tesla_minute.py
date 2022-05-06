#!/usr/bin/env python
# coding: utf-8

# In[ ]:


'''
Running this script will 
1) Update 'signal_minute_tb' table of 'energy_sites_db' database every minute using data pulled from API.
2) Print out a list of sites with negative solar power. 
'''


# In[200]:


# import libraries
import requests
import pandas as pd
import time
import sys
from datetime import datetime
import psycopg2 as ps
import schedule


# In[7]:


# API Key
API_KEY='RUFTVEVSIEVHRyAjIDMuLi4KCnlvdXIgc2ljaw'
#database credentials
host_name = 'database-1.c8e37wn95xfp.us-west-1.rds.amazonaws.com'
dbname = 'energy_sites_db'
port = '5432'
username = 'postgres' 
password = '4DBS*eK)QhWwH86'
conn = None


# In[104]:


# get list of sites
def get_sites():
    while True:
        try: 
            url='https://te-data-test.herokuapp.com/api/sites?token='
            sites_response=requests.get(url+API_KEY).json()
            sites=sites_response['sites']
            break
        except:
            print('Cannot access data. Will try again in 5 mins.')
            time.sleep(300)
    return sites


# In[126]:


# store signals in dataframe
def get_signals(sites):
    signals_url='https://te-data-test.herokuapp.com/api/signals'
    df=pd.DataFrame(columns=['site','signal_time','SITE_SM_batteryInstPower','SITE_SM_siteInstPower','SITE_SM_solarInstPower'])
    for s in sites:
        while True:
            try:
                signals_response=requests.get(signals_url, params = {"token": API_KEY,
                                                                    "site":s}).json()
                try:
                    SITE_SM_batteryInstPower=signals_response['signals']['SITE_SM_batteryInstPower']
                except: 
                    SITE_SM_batteryInstPower=0
                try:
                    SITE_SM_siteInstPower=signals_response['signals']['SITE_SM_siteInstPower']
                except: 
                    SITE_SM_siteInstPower=0
                try:
                    SITE_SM_solarInstPower=signals_response['signals']['SITE_SM_solarInstPower']
                except: 
                    SITE_SM_solarInstPower=0
                try:
                    signal_time=signals_response['timestamp']
                except: 
                    signal_time=0    

                df=df.append({'site':s,'signal_time':signal_time,
                              'SITE_SM_batteryInstPower': SITE_SM_batteryInstPower,
                              'SITE_SM_siteInstPower':SITE_SM_siteInstPower,
                              'SITE_SM_solarInstPower':SITE_SM_solarInstPower},ignore_index=True)
                break
            except:
                print('Cannot access data. Will try again in 5 mins.')
    return df


# In[129]:


def clean_df(df):
    df['signal_time'] = pd.to_datetime(df['signal_time'])
    df=df.fillna(0)
    return df


# In[136]:


def connect_to_db(host_name, dbname, port, username, password):
    try:
        conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)

    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
        return conn


# In[ ]:


# print sites with negative solarinstpower 
def detect_neg_in_solarInstPower(df):
    list_of_neg_sites = list(df['site'][df['SITE_SM_solarInstPower']<0])
    current_time=pd.to_datetime(df['signal_time'][0])
    print(f'SITE_SM_solarInstPower of sites {list_of_neg_sites} are negative at {current_time}.')


# In[180]:


# create minute table 
def create_table(curr):
    create_table_command = ("""CREATE TABLE IF NOT EXISTS signal_minute_tb (
                    site VARCHAR(255) PRIMARY KEY,
                    signal_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    SITE_SM_batteryInstPower FLOAT NOT NULL,
                    SITE_SM_siteInstPower  FLOAT NOT NULL,
                    SITE_SM_solarInstPower FLOAT NOT NULL
            )""")

    curr.execute(create_table_command)
    


# In[183]:


def check_if_site_exists(curr, site): 
    query = ("""SELECT site FROM signal_minute_tb WHERE site = %s""")

    curr.execute(query, (site,))
    return curr.fetchone() is not None


# In[184]:


def update_row(curr, site, signal_time, SITE_SM_batteryInstPower, SITE_SM_siteInstPower, SITE_SM_solarInstPower):
    query = ("""UPDATE signal_minute_tb
            SET signal_time = %s,
                SITE_SM_batteryInstPower = %s,
                SITE_SM_siteInstPower = %s,
                SITE_SM_solarInstPower = %s
            WHERE site = %s;""")
    vars_to_update = (signal_time, SITE_SM_batteryInstPower, SITE_SM_siteInstPower, SITE_SM_solarInstPower, site)
    curr.execute(query, vars_to_update)


# In[185]:


def update_db(curr,df):
    tmp_df = pd.DataFrame(columns=['site','signal_time','SITE_SM_batteryInstPower','SITE_SM_siteInstPower','SITE_SM_solarInstPower'])
    for i, row in df.iterrows():
        if check_if_site_exists(curr, row['site']): # If site already exists then we will update
            update_row(curr,row['site'],row['signal_time'],row['SITE_SM_batteryInstPower'],row['SITE_SM_siteInstPower'],row['SITE_SM_solarInstPower'])
        else: # The site doesn't exists we will add it to a temp df and append it using append_from_df_to_db
            tmp_df = tmp_df.append(row)

    return tmp_df # data on sites that don't already exist


# In[188]:


# write insert command
def insert_into_table(curr, site, signal_time, SITE_SM_batteryInstPower, SITE_SM_siteInstPower, SITE_SM_solarInstPower):
    insert_query = ("""INSERT INTO signal_minute_tb (site, signal_time, SITE_SM_batteryInstPower, SITE_SM_siteInstPower, SITE_SM_solarInstPower)
    VALUES(%s,%s,%s,%s,%s);""")
    row_to_insert = (site, signal_time, SITE_SM_batteryInstPower, SITE_SM_siteInstPower, SITE_SM_solarInstPower)
    curr.execute(insert_query, row_to_insert)


# In[189]:


# insert 1 at a time for performance
def append_from_df_to_db(curr,df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['site'],row['signal_time'],row['SITE_SM_batteryInstPower'],row['SITE_SM_siteInstPower'],row['SITE_SM_solarInstPower'])


# In[ ]:


def main():
    start_time=time.time()
    sites=get_sites()
    df=get_signals(sites)
    df=clean_df(df)
    detect_neg_in_solarInstPower(df)
    conn = connect_to_db(host_name, dbname, port, username, password)
    curr = conn.cursor()
    create_table(curr)
    conn.commit()
    new_sites_df=update_db(curr,df)
    conn.commit()
    append_from_df_to_db(curr,new_sites_df)
    conn.commit()
    end_time=time.time()
    duration=end_time-start_time
    print(f'signal_minute_tb is updated at {datetime.now()}. The program took {duration}s.')
    


# In[208]:


if __name__=='__main__':
    schedule.every(1).minutes.do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)


# In[ ]:




