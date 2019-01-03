import csv
import datetime
import json
import os
import sys
import time

import tweepy
from tweepy.auth import OAuthHandler

'''
Collects the k most recent tweets given a list of users and stores the tweets as JSON files.
Before using, set the value of CAP to be k and make sure all_uids contains a list of user ids.
Usage: python get_historic_tweets.py
'''

twitter_config_file = "config/twitter_config_1.txt"
input_uids_file = "harvey_ids_2.csv"
output_dir = "json_data_2"


CAP = 3200 ## How many tweets to get per user (set to None for no cap, although I think Twitter will cap it anyways eventually)

def authenticate():
	## Pulling twitter login credentials from "config" file
	## The file should have the consumer key, consumer secret, access token, and access token secret in that order, separated by newlines.
	config = open(twitter_config_file).read().split()
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

def get_historic_tweets_from_id(uid,api):
	## Printing out the user id (for debugging)
	print(uid)

	## List of tweets we've collected so far
	tweets = []

	## The timeline is returned as pages of tweets (each page has 20 tweets, starting with the 20 most recent)
	## If a cap has been set and our list of tweets gets to be longer than the cap, we'll stop collecting
	try:
		for page in tweepy.Cursor(api.user_timeline, tweet_mode='extended', id=uid, count=200).pages():
			tweets.extend(page)
			if CAP and len(tweets) >= CAP:
				return tweets
			## We get 900 requests per 15-minute window, or 1 request/second, so wait 1 second between each request just to be safe
			time.sleep(1)
	
	except tweepy.RateLimitError:
		## We received a rate limiting error, so wait 15 minutes
		time.sleep(15*60)

		## Try again
		tweets = get_historic_tweets_from_id(uid,api)

	return tweets



""""""
""""""

## Get an authenticated API object
api = authenticate()

## Load list of uids to collect
all_uids = []
with open(input_uids_file) as f:
	reader = csv.reader(f)
	for row in reader:
		all_uids.append(row[0])

## Get a list of uids we've already collected by seeing which JSON files we have (so we don't collect on the same users twice)

completed_uids = set([fname.split('.')[0] for fname in os.listdir(output_dir)])


## Figure out which uids are left
uids_remaining = set(all_uids) - completed_uids
print(len(uids_remaining))

## Loop through the list of remaining uids					
for uid in uids_remaining:
	utc_now = str(datetime.datetime.utcnow()) ## Get the timestamp of when we collected the tweets and convert it to a string so it can be stored in JSON

	try:
		## Pull the tweets using Tweepy
		historic_tweets = get_historic_tweets_from_id(uid,api)

		## Convert each Status object from Tweepy to JSON
		historic_tweets = [tweet._json for tweet in historic_tweets]

		## Add the uid and the timestamp to the JSON
		data = {"user_id":uid, "utc_timestamp":utc_now, "historic_tweets":historic_tweets}

		## Dump the JSON into a file with the name <uid>.json
		with open("json_data_2/" + str(uid) + ".json", "w+") as data_file:
			json.dump(data, data_file)

		## Print out how many tweets we've collected per user id (for debugging)
		print(str(uid) + ': ' + str(len(historic_tweets)) + ' tweets collected')

	## If we get a Tweepy error, print the uid and error and keep running
	except tweepy.error.TweepError as ex:
		print(uid)
		print(ex)