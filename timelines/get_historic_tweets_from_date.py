import csv
import datetime
import json
import os
import sys
import time

import tweepy
from tweepy.auth import OAuthHandler

'''
Collects the tweets tweeted since the given date given a list of users and stores the tweets as JSON files.
Before using, set the value of FROM_DATE_STR and make sure all_uids contains a list of user ids.
Usage: python get_historic_tweets_from_date.py input_list_of_ids.csv output_json_dir/ twitter_config.txt
'''


FROM_DATE_STR = "Fri Aug 18 00:00:00 +0000 2017"

input_list = sys.argv[1]
output_dir = sys.argv[2]
config_file = sys.argv[3]

def convert_str_to_datetime(datetime_str):
	date_format = "%a %b %d %H:%M:%S +0000 %Y"
	return datetime.datetime.strptime(datetime_str, date_format)

def get_now():
	return datetime.datetime.utcnow()

from_date = convert_str_to_datetime(FROM_DATE_STR)

def authenticate():
	## Pulling twitter login credentials from "config" file
	## The file should have the consumer key, consumer secret, access token, and access token secret in that order, separated by newlines.
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

## Gets 3200 of the most recent tweets associated with the given uid before before_id
## (or the 3200 most recent tweets if before_id is None)
## Returns the minimum id of the list of tweets (i.e. the id corresponding to the earliest tweet)
def get_historic_tweets_before_id(api, uid, max_id=None):
	## Printing out the user id (for debugging)
	#print(uid)

	## List of tweets we've collected so far
	tweets = []
	finished = False

	## The timeline is returned as pages of tweets (each page has 20 tweets, starting with the 20 most recent)
	## If a cap has been set and our list of tweets gets to be longer than the cap, we'll stop collecting
	cursor_args = {"id": uid, "count": 200}
	if max_id:
		cursor_args["max_id"] = max_id

	try:
		for page in tweepy.Cursor(api.user_timeline, **cursor_args).pages(16):
			## Adding the tweets to the list

			json_tweets = [tweet._json for tweet in page]

			if any(convert_str_to_datetime(tweet['created_at']) < from_date for tweet in json_tweets) or len(page)==1:
				## We've already gone as far back as we need to, so quit looping through pages of tweets
				finished = True
				## Filter out any older tweets
				json_tweets = [tweet for tweet in json_tweets if convert_str_to_datetime(tweet['created_at']) >= from_date]
			else:
				## We get 900 requests per 15-minute window, or 1 request/second, so wait 1 second between each request just to be safe
				time.sleep(1)

			tweets.extend(json_tweets)

			if finished:
				break
	
	except tweepy.error.TweepError as ex:
		## We received a rate limiting error, so wait 15 minutes
		if "429" in str(ex): # a hacky way to see if it's a rate limiting error
			time.sleep(15*60)
			print("rate limited :/")

			## Try again
			return get_historic_tweets_before_id(api, uid, max_id)
		elif any(code in str(ex) for code in ["401", "404"]):
			return (None, True, [])
			
		else:
			print(uid)
			print(ex)
			return (None, True, [])

	if tweets:
		max_id = tweets[0]['id']
		for tweet in tweets[1:]:
			if tweet['id'] < max_id:
				max_id = tweet['id']

		return (max_id, finished, tweets)
	
	else:
		return (None, True, [])

## Get a uid's tweets since FROM_DATE
def get_historic_tweets(api, uid):
	max_id, finished, tweets = None, False, []
	while not finished:
		max_id, finished, returned_tweets = get_historic_tweets_before_id(api, uid, max_id)

		if returned_tweets:
			tweets.extend(returned_tweets)

	return tweets




""""""
""""""

## Get an authenticated API object
api = authenticate()

## Load list of uids to collect
all_uids = []
with open(input_list) as f:
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
	utc_now = str(get_now()) ## Get the timestamp of when we collected the tweets and convert it to a string so it can be stored in JSON

	## Pull the tweets using Tweepy
	historic_tweets = get_historic_tweets(api, uid)

	if historic_tweets:
		## Add the uid and the timestamp to the JSON
		data = {"user_id":uid, "utc_timestamp":utc_now, "historic_tweets":historic_tweets}

		## Dump the JSON into a file with the name <uid>.json
		with open(output_dir + "/" + str(uid) + ".json", "w+") as data_file:
			json.dump(data, data_file)
