
# coding: utf-8

# In[6]:


import csv
from datetime import datetime
from functions import *
import json
import multiprocessing
import pandas as pd
import sys
import time
import tweepy
from tweepy.auth import OAuthHandler

configuration = json.load(open("configuration.json"))


# # Pull a user's data from the Twitter API

# In[5]:


def pull_user_data(api, user_ids):
    if len(user_ids) > 100:
        raise ValueError("Maximum 100 user_ids per request.")
    
    try:
        collected_at = datetime.now()
        users_data = api.lookup_users(user_ids=user_ids)
        users_data = [(user_data._json, collected_at) for user_data in users_data]
        
    except tweepy.RateLimitError:
        time.sleep(15 * 60)
        users_data = pull_user_data(api, user_ids)
    
    return users_data


# # Write the user data
# < In progress, depending on the database format >

# In[7]:


def write_users_to_database(configuration, users_data, start_interval, end_interval):
#     config_file = configuration["db_config_file"]
#     for line in open(db_config_file).readlines():
#         key, value = line.strip().split("=")
#         config[key] = value
    
#     ## Connect to the database and get a cursor object
#     database = psycopg2.connect(**config)
#     cursor = database.cursor()

#     cursor.execute(CREATE_TABLE_IF_EXISTS_STMT)
    
#     rows = []
    
#     database.commit()
    print(users_data)

