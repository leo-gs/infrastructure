
# coding: utf-8

# In[1]:


import json

import tweepy

import collect_users
import functions
import pull_friends
import pull_user_data

configuration = json.load(open("configuration.json"))


# # Process an individual user

# In[2]:


def process_users(user_ids, twitter_api, start_interval, end_interval):
    user_data = pull_user_data.pull_user_data(twitter_api, user_ids)
    friendships, error_ids = pull_friends.pull_friends(twitter_api, user_ids)
    return (user_data, friendships, error_ids)


# # Get all the new user_ids from the JSON files

# In[11]:


print("collecting user_ids")
user_ids, start_interval, end_interval = collect_users.run(configuration) ## Returns a set of user_ids, as long as the time window
user_ids = list(user_ids)


# # Get an authenticated API object to start pulling data

# In[12]:


print("getting a Twitter API object")
twitter_api = functions.get_twitter_api_obj()


# # Pull data for each user_id

# In[14]:


users_data = []
users_friendships = []
errors = []

chunk = 0
while chunk < len(user_ids):
    ## Process in groups of 100
    user_ids_to_process = user_ids[chunk:chunk + 100]
    
    user_data, friendships, error_ids = process_users(user_ids_to_process, twitter_api, start_interval, end_interval)
    users_data.extend(user_data)
    users_friendships.extend(friendships)
    errors.extend(error_ids)
    
    chunk = chunk + 100
    print(chunk)
    print(str(len(users_data)) + " users collected")
    print(str(len(users_friendships)) + " friendships collected")
    print("")


# # Write to the database

# In[56]:


print("writing friendships to database")
pull_friends.write_friendships_to_database(configuration, users_friendships, start_interval, end_interval)
print("writing errors to database")
pull_friends.write_errors_to_database(configuration, errors, start_interval, end_interval)
print("writing user data to database")
pull_user_data.write_users_to_database(configuration, users_data, start_interval, end_interval)
print("Done!")

