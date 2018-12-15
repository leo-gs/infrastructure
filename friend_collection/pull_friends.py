
# coding: utf-8

# In[1]:


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


# # Pull a user's friends from the Twitter API

# In[19]:


def pull_friends(api, user_ids):
    friendships = []
    error_accounts = []
    
    for user_id in user_ids:
        friend_ids = []
        collected_at = datetime.now()

        try:
            for page in tweepy.Cursor(api.friends_ids, id=user_id).pages():
                friend_ids.extend(page)
            
            friendships.extend([(user_id, user_id, friend_id, collected_at) for friend_id in friend_ids])

        except tweepy.RateLimitError:
            time.sleep(15 * 60)
            user_friendships, error_ids = pull_friends(api, [user_id])
            friendships.extend(user_friendships)
            error_accounts.extend(error_ids)
        
        except tweepy.TweepError as ex:
            error_accounts.append((user_id, ex.response.status_code, ex.response.text))
    
    return (friendships, error_accounts)


# # Write out the friendships
# < In progress, depending on the database format >

# In[ ]:


def write_friendships_to_database(configuration, friendships, start_interval, end_interval):
    config_file = configuration["db_config_file"]
    event_id = configuration["event_id"]
    
    for line in open(db_config_file).readlines():
        key, value = line.strip().split("=")
        config[key] = value
    
    ## Connect to the database and get a cursor object
    database = psycopg2.connect(**config)
    cursor = database.cursor()

    cursor.execute(CREATE_TABLE_IF_EXISTS_STMT)
    
    rows = [friendship + (start_interval, end_interval, event_id)]
    cursor.execute_many(INSERT_STMT, rows)
    
    database.commit()
    cursor.close()
    database.close()

    print(friendships)

