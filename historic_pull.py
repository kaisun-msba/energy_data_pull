#!/usr/bin/env python
# coding: utf-8

# In[ ]:


'''
Running this script will 
1) pull signal data from API and append it to 'signal_historic_tb' of 'energy_sites_db' every 10 minute.
'''


# In[1]:


# import libraries
import requests
import pandas as pd
import time
import sys
import gc
from datetime import datetime
import psycopg2 as ps
import schedule


# In[2]:


# API Key
API_KEY='************************************8'
#database credentials
host_name = '***************'
dbname = '***************'
port = '***************'
username = '***************' 
password = '***************'
conn = None


# In[3]:


# get list of sites
def get_sites():
    while True:
        try: 
            url='***************'
            sites_response=requests.get(url+API_KEY).json()
            sites=sites_response['sites']
            break
        except:
            print('Cannot access data. Will try again in 5 mins.')
            # wait 300 seconds
            time.sleep(300)
    return sites


# In[4]:


# store signals in dataframe
def get_signals(sites):
    signals_url='***************'
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
                # wait 300 seconds
                time.sleep(300)
    return df


# In[5]:


# change timestamp to datetime
# fill nan with 0
def clean_df(df):
    df['signal_time'] = pd.to_datetime(df['signal_time'])
    df=df.fillna(0)
    return df


# In[20]:


# connect to database
def connect_to_db(host_name, dbname, port, username, password):
    while True:
        try:
            conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
            break
        except ps.OperationalError as e:
            print(e)
            print('Cannot access data. Will try again in 5 mins.')
            time.sleep(300)      
        else:
            print('Connected!')
    return conn


# In[8]:


# create signal_historic_tb table 
def create_table(curr):
    create_table_command = ("""CREATE TABLE IF NOT EXISTS signal_historic_tb (
                    site VARCHAR(255),
                    signal_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    SITE_SM_batteryInstPower FLOAT NOT NULL,
                    SITE_SM_siteInstPower  FLOAT NOT NULL,
                    SITE_SM_solarInstPower FLOAT NOT NULL,
                    PRIMARY KEY (site,signal_time)
            )""")

    curr.execute(create_table_command)
    


# In[9]:


# write insert command
def insert_into_table(curr, site, signal_time, SITE_SM_batteryInstPower, SITE_SM_siteInstPower, SITE_SM_solarInstPower):
    insert_query = ("""INSERT INTO signal_historic_tb (site, signal_time, SITE_SM_batteryInstPower, SITE_SM_siteInstPower, SITE_SM_solarInstPower)
    VALUES(%s,%s,%s,%s,%s);""")
    row_to_insert = (site, signal_time, SITE_SM_batteryInstPower, SITE_SM_siteInstPower, SITE_SM_solarInstPower)
    curr.execute(insert_query, row_to_insert)


# In[10]:


# insert 1 at a time for performance
def append_from_df_to_db(curr,df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['site'],row['signal_time'],row['SITE_SM_batteryInstPower'],row['SITE_SM_siteInstPower'],row['SITE_SM_solarInstPower'])


# In[11]:


def main():
    start_time=time.time()
    sites=get_sites()
    df=get_signals(sites)
    df=clean_df(df)
    conn = connect_to_db(host_name, dbname, port, username, password)
    curr = conn.cursor()
    create_table(curr)
    conn.commit()
    append_from_df_to_db(curr,df)
    conn.commit()
    end_time=time.time()
    duration=end_time-start_time
    print(f'signal_historic_tb is updated at {datetime.now()}. The program took {duration}s.')
    


# In[208]:


# executes main every 10 minute while running the script
if __name__=='__main__':
    schedule.every(10).minutes.do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)


# In[ ]:





# In[ ]:




