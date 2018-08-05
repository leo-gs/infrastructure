
# coding: utf-8

# In[ ]:


import csv
from datetime import datetime
import json
import os
import psycopg2
from psycopg2 import extras as ext
import re
import requests
import sys
import time
import urllib2

import sql_statements

CREATE_TABLE_STMT = sql_statements.CREATE_TABLE_STMT
INSERT_TWEET_STMT = sql_statements.INSERT_TWEET_STMT 


# # 1. Configure parameters

# ### Text search
# This will search the full text of the tweet, any retweeted_status text, and any quoted_status text.
# 
# `search_text`: set to True if you want to use text search
# 
# `keywords`: add the keywords you want to match here
# 
# `all_keywords`: whether to check for all keywords. If true, it will match only tweets that have all keywords. If false it will check whether any of the keywords exist

# In[10]:


search_text = True
keywords = ["white", "helmet"]
all_keywords = True


# ### Date bounds
# This will only match tweets within the given date bounds
# 
# `match_dates`: whether to use date bounds
# 
# `bounds`: the date bounds

# In[ ]:


match_dates = True
bounds = (datetime(2017, 5, 27, 0, 0, 0), datetime(2018, 3, 2, 0, 0, 0))


# ### Regex match
# This will regex match the full text of the tweet, any retweeted_status text, and any quoted_status text
# 
# `use_regex_match`: whether to use regex matching
# 
# `reg_expr`: the regex expression

# In[ ]:


use_regex_match = False
reg_expr = "Leo doesn't understand regex"


# ### Folders
# `folders`: Folders where the json files are (it will process all the json files in each folder)

# In[ ]:


folders = ["/data/captures/Syria composite - Restart 2018/"]


# ### Database configuration
# The file should look like:
# ```
# host = INSERT_HOSTNAME
# username = INSERT_USERNAME
# password = INSERT_PASSWORD
# ```
# Make sure that the database exists (you might have to run ```CREATE DATABASE database_name;```)
# 
# `database_name` is the name of the database
# 
# `db_config_file` is the path to the file with the configuration

# In[11]:


database_name = "syria_composite_white_helmets"
db_config_file = "/home/lgs17/config/bowker_config.txt"


# # 2. Functions

# In[22]:


def clean(s):
    ## Replace weird characters that make Postgres unhappy
    return s.replace("\x00", "") if s else None

## Get a value from the given dictionary by following the path
## If the path isn't valid, nothing will be returned
def get_nested_value(outer_dict, path_str, default=None):
    path = path_str.split(".") # get a list of nested dictionary keys (the path)
    cur_dict = outer_dict

    ## step through the path and try to process it
    try:
        for step in path:
            ## If it's actually a list index, convert it to an integer
            if step.isdigit():
                step = int(step)

            ## Get the nested value associated with that key
            cur_dict = cur_dict[step]

        ## Once it's at the end of the path, return the nested value
        return cur_dict

    ## The value didn't exist
    except (KeyError, TypeError, IndexError):
        pass

    return default


## Get a json string rather than an individual value
def get_nested_value_json(_dict, path, default=None):
    ## Pull the nested value
    value = get_nested_value(_dict, path, default)

    ## Return a string of the json dictionary
    if value:
        return json.dumps(value)


## Given a string and a list of keywords, returns all keywords that exist in the string (case-insensitive)
## To check for any matches, just see if there are things in the list
def get_matching_keywords(search_string, keywords):
    return [keyword for keyword in keywords if keyword in search_string.lower()]


## Expanding a URL
def expand_url(tweet, index=0):
    
    def expand_url_1(url):
        res = None
        try:
            return urllib2.urlopen(url).url
        except urllib2.HTTPError as e:
            return None
        except urllib2.URLError as e:
            return None
    
    def expand_url_2(url):
        try:
            return requests.get(url).url
        except requests.exceptions.SSLError as e:
            return None
        except requests.exceptions.ConnectionError as e:
            return None
    
    url_json = tweet["entities"].get("urls")
    if not url_json or index >= len(url_json):
        return None
    
    url = url_json[index]["url"]
    if url:
        url_exp = expand_url_1(url)
        if not url_exp:
            url_exp = expand_url_2(url)
        if not url_exp:
            print("Couldn't resolve url {}".format(url))
        return url_exp


# ### Reconstructing full text
# Some tweets have been truncated and have and additional `full_text` field. Additionally, we want to reconstruct quoted tweets and retweets so they appear like they would in a user's feed.

# In[14]:


def get_complete_text(tweet):

    ## Encode unicode so it plays nice with the string formatting
    def c(u):
        return u.encode('utf8')

    tweet_complete_text = tweet["text"]
    if tweet["truncated"]:
        ## Applicable to original tweets and commentary on quoted tweets
        tweet_complete_text = tweet["extended_tweet"]["full_text"]

    # this handles retweets of original tweets and retweets of quoted tweets
    if "retweeted_status" in tweet:
        return "RT @{username}: {orig_complete_text}".format(
            username=c(tweet["retweeted_status"]["user"]["screen_name"]),
            orig_complete_text=get_complete_text(tweet["retweeted_status"]))

    # I am fairly certain that the only way you can quote a tweet is by quoting the original tweet; i.e. I don't think you can quote a retweet
    elif "quoted_status" in tweet:
        return "{qt_complete_text} QT @{username}: {orig_complete_text}".format(
            qt_complete_text=c(tweet_complete_text),
            username=c(tweet["quoted_status"]["user"]["screen_name"]),
            orig_complete_text=get_complete_text(tweet["quoted_status"]))
    else:
        return c(tweet_complete_text)


# ### Filtering individual tweets
# This is where all the matching is implemented.

# In[21]:


def tweet_matches_parameters(tweet):
    
    #######################
    ## Keyword filtering ##
    #######################
    
    if search_text:
        def matches_keywords(text):
            matches = get_matching_keywords(text)

            if all_keywords:
                return matches == keywords ## only return True if all keywords matched

            else:
                return bool(matches) ## return True if there's at least one match


        ## Make a list of fields to check for keyword matches
        keyword_texts = [get_complete_text(tweet)]

        keywords_matches = [matches_keywords(text, keywords) for text in keyword_texts]
        if not any(keyword_matches):
            return False

    #############################
    ## Time interval filtering ##
    #############################
    
    if match_dates:
        created_at = get_nested_value(tweet, "created_at")
        created_ts = datetime.strptime(created_at[0:19]+created_at[25:], "%a %b %d %H:%M:%S %Y")
        
        if not created_ts or created_ts < bounds[0] or created_ts > bounds[1]:
            return False
    
    return True

    ####################
    ## Regex matching ##
    ####################
    
    if use_regex_match:
        ## Make a list of fields to check for keyword matches
        regex_texts = [get_complete_text(tweet)]
        
        regex_matches = [bool(re.search(reg_expr, text)) for text in text]
        if not any(regex_matches):
            return False
    
    return True


# ### Extracting individual tweets
# This parses the JSON into a row that can be inserted into the database.

# In[20]:


def extract_tweet(tweet):
    ## Adding everything to a huge tuple and inserting the tuple to the database
    created_at = get_nested_value(tweet, "created_at")
    created_ts = datetime.strptime(created_at[0:19]+created_at[25:], "%a %b %d %H:%M:%S %Y")
    
    ucts = get_nested_value(tweet, "user.created_at")
    user_created_ts = datetime.strptime(ucts[0:19]+ucts[25:], "%a %b %d %H:%M:%S %Y")

    item = (
        tweet["id"],
        created_at,
        created_ts,
        get_nested_value(tweet, "lang"),
        clean(get_nested_value(tweet, "text")),
        clean(get_complete_text(tweet)),
        get_nested_value(tweet, "coordinates.coordinates{0}"),
        get_nested_value(tweet, "coordinates.coordinates{1}"),
        get_nested_value_json(tweet, "contributors"),
        get_nested_value_json(tweet, "counts"),
        get_nested_value_json(tweet, "entities"),
        expand_url(tweet, index=0),
        expand_url(tweet, index=1),
        get_nested_value_json(tweet, "entities.urls"),
        get_nested_value(tweet, "filter_level"),
        get_nested_value_json(tweet, "coordinates"),
        get_nested_value_json(tweet, "place"),
        get_nested_value(tweet, "possibly_sensitive"),
        get_nested_value_json(tweet, "user"),
        get_nested_value(tweet, "user.id"),
        get_nested_value(tweet, "user.screen_name"),
        get_nested_value(tweet, "user.followers_count"),
        get_nested_value(tweet, "user.friends_count"),
        get_nested_value(tweet, "user.statuses_count"),
        get_nested_value(tweet, "user.favourites_count"),
        get_nested_value(tweet, "user.geo_enabled"),
        get_nested_value(tweet, "user.time_zone"),
        clean(get_nested_value(tweet, "user.description")),
        get_nested_value(tweet, "user.location"),
        get_nested_value(tweet, "user.created_at"),
        user_created_ts,
        get_nested_value(tweet, "user.lang"),
        get_nested_value(tweet, "user.listed_count"),
        get_nested_value(tweet, "user.name"),
        get_nested_value(tweet, "user.url"),
        get_nested_value(tweet, "user.utc_offset"),
        get_nested_value(tweet, "user.verified"),
        get_nested_value(tweet, "user.contributors_enabled"),
        get_nested_value(tweet, "user.default_profile"),
        get_nested_value(tweet, "user.is_translator"),
        get_nested_value(tweet, "retweet_count"),
        get_nested_value(tweet, "favorite_count"),
        get_nested_value_json(tweet, "retweeted_status"),
        get_nested_value(tweet, "retweeted_status.id"),
        get_nested_value(tweet, "retweeted_status.user.screen_name"),
        get_nested_value(tweet, "retweeted_status.retweet_count"),
        get_nested_value(tweet, "retweeted_status.user.id"),
        get_nested_value(tweet, "retweeted_status.user.time_zone"),
        get_nested_value(tweet, "retweeted_status.user.friends_count"),
        get_nested_value(tweet, "retweeted_status.user.statuses_count"),
        get_nested_value(tweet, "retweeted_status.user.followers_count"),
        get_nested_value(tweet, "source"),
        get_nested_value(tweet, "in_reply_to_screen_name"),
        get_nested_value(tweet, "in_reply_to_status_id"),
        get_nested_value(tweet, "in_reply_to_user_id"),
        get_nested_value(tweet, "quoted_status_id"),
        get_nested_value(tweet, "quoted_status_id_str"),
        get_nested_value_json(tweet, "quoted_status"),
        get_nested_value(tweet, "truncated"),
        get_nested_value(tweet, "quoted_status.user.screen_name"),
        clean(get_nested_value(tweet, "retweeted_status.user.description")),
        clean(get_nested_value(tweet, "quoted_status.user.description")),
        json.dumps(tweet))
  
    return item


# ### Process all the tweets in a JSON file

# In[19]:


def extract_json_file(json_file_path, cursor, database, keywords):
    queue = []

    with open(json_file_path, 'r') as infile:
        for line in infile:
            ## Skip empty lines
            if (not line or len(line) < 2):
                continue  

            tweet = None

            ## Load the tweet string into a dictionary.
            ## There's like one tweet in one json file that is bad json, so I've just been skipping
            ## it. If there end up being a lot, we should probably figure out why that's happening.
            try:
                tweet = json.loads(line)
            except ValueError:
                print("bad json")
                print(line)
                continue

            ## Make sure that the tweet matches all filtering parameters
            if not tweet_matches_parameters:
                continue

            tweet_row = extract_tweet(tweet)
            if tweet_row:
                queue.append(tweet_row)

    ## Insert the extracted tweets into the database
    ext.execute_batch(cursor, INSERT_TWEET_STMT, queue)

    ## Just to keep track of how many have been inserted
    return len(queue)


# # 3. Run everything

# In[ ]:


## Parse the database credentials out of the file
config = {"database": database_name}
for line in open(db_config_file).readlines():
    key, value = line.strip().split("=")
    config[key] = value

## Connect to the database and get a cursor object
database = psycopg2.connect(**config)
cursor = database.cursor()

cursor.execute(CREATE_TABLE_STMT)
database.commit()

## Uncomment to clear the table each time the script starts
cursor.execute("DELETE FROM tweets")

## Keep track of how many tweets have been inserted (just make sure it's running)
total = 0

## Process each folder
for folder_path in folders:
    ## Make sure only valid .json files are processed
    json_files_to_process = [json_file for json_file in os.listdir(folder_path) if json_file[-5:] == ".json"]

    for j in range(len(json_files_to_process)):
        json_file = json_files_to_process[j]
        ## For each file, extract the tweets and add the number extracted to the total
        total += extract_json_file(os.path.join(folder_path, json_file), cursor, database, keywords)
        print("{fnum}/{ftotal}: {tnum} total tweets inserted".format(fnum=j, ftotal=(len(json_files_to_process)+1), tnum=total))
        sys.stdout.flush()

## Close everything
cursor.close()
database.commit()
database.close()

