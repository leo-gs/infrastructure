
# coding: utf-8

# In[ ]:


import json
import tweepy
from tweepy.auth import OAuthHandler

configuration = json.load(open("configuration.json"))


# # Get an authenticated Twitter API object

# In[ ]:


def get_twitter_api_obj():

    config_file = configuration["twitter_config_file"]

    ## Pulling twitter login credentials from "config" file
    ## The file should have the consumer key, consumer secret, access token, and access token secret in that order,
    ## separated by newlines.

    config = open(config_file).read().split()
    consumer_key = config[0]
    consumer_secret = config[1]
    access_token = config[2]
    access_token_secret = config[3]

    ## Authenticating
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    ## Returning the authenticated API object
    api = tweepy.API(auth)

    return api
