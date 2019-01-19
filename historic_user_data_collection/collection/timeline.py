from datetime import datetime
import json
import time

import tweepy
from tweepy.auth import OAuthHandler

from collection import collection
from db_connect import *
from utilities import notifications, utils

class Timeline(collection.Collection):

    def __init__(self, user_set_id, user_set_prefix, db, first_timebound_type, timebound_arg, subsequent_timebound_type, notifiers=[]):

        if (not first_timebound_type in ["number", "date", "last_tweet"]) or (not subsequent_timebound_type in ["number", "date", "last_tweet"]):
            raise ValueError("Please specify `timebound_type` to be 'number', 'date', or 'last_tweet'")

        self.user_set_id = user_set_id
        self.name = "timeline"
        self.user_set_prefix = user_set_prefix
        self.db = db
        self.notifiers = notifiers

        self.table_name = "{}_user_timeline".format(user_set_prefix)

        table_create = """
            CREATE TABLE IF NOT EXISTS {} (
              user_set_id TEXT,
              user_id BIGINT,
              tweet_id BIGINT,
              collection_bucket_ts TIMESTAMP,
              collected_ts TIMESTAMP,
              tweet_json JSON,
              PRIMARY KEY (user_set_id, user_id, tweet_id)
            )
            """.format(self.table_name)
        db.execute_sql(table_create, commit=True)

        select_collection_exists = """
            SELECT COUNT(*)
            FROM {}
            WHERE user_set_id = '{}'
        """.format(self.table_name, user_set_id)
        collection_exists = bool(db.execute_sql(select_collection_exists, fetch=True)[0][0] > 0)

        if not collection_exists:
            self.timebound_type = first_timebound_type
            self.timebound_arg = timebound_arg
        else:
            select_most_recent_tweet_id = """
                SELECT MAX(tweet_id)
                FROM {}
                WHERE user_set_id = '{}'
            """.format(self.table_name, self.user_set_id)
            self.most_recent_tweet_id = db.execute_sql(select_most_recent_tweet_id, fetch=True)[0][0]
            self.timebound_type = subsequent_timebound_type
            self.timebound_arg = timebound_arg

        if self.timebound_type == "date":
            self.timebound_arg = utils.twitter_str_to_dt(timebound_arg)

        self.table_insert = """
            INSERT INTO {} (user_set_id, user_id, tweet_id, collection_bucket_ts, collected_ts, tweet_json)
            VALUES (%s, %s, %s, %s, %s, %s)
        """.format(self.table_name)

        notifications.notify_all(notifiers,
                   "Historic timeline collection uploading to `{}`".
                   format(self.table_name), notify_type="info")

    def check_if_collection_is_finished(self, tweets):
        finished, filtered_tweets = False, []
        if self.timebound_type == "number":
            if len(tweets) >= self.timebound_arg:
                tweets.sort(reverse=True, key=lambda t: t['created_at'])
                finished, filtered_tweets = True, tweets[:self.timebound_arg]

        elif self.timebound_type == "date":
            min_tweet = min(tweets, key=lambda t: utils.twitter_str_to_dt(t["created_at"]))
            min_date = utils.twitter_str_to_dt(min_tweet["created_at"])
            if min_date < self.timebound_arg:
                finished, filtered_tweets = True, [t for t in tweets if utils.twitter_str_to_dt(t["created_at"]) >= self.timebound_arg]

        elif self.timebound_type == "last_tweet":
            min_tweet_id = int(min(tweets, key=lambda t: int(t["id_str"]))["id_str"])
            if min_tweet_id <= self.most_recent_tweet_id:
                finished, filtered_tweets = True, [t for t in tweets if int(t["id_str"]) > self.most_recent_tweet_id]
        else:
            raise ValueError("{} isn't a supported parameter.".format(self.timebound_type))

        return finished, filtered_tweets

    def collect(self, api, user_ids, collection_bucket_ts):
        all_rows = []

        notifications.notify_all(self.notifiers, "Starting timeline collection for {} user_ids".format(len(user_ids)), notify_type="start")

        for user_id in user_ids:
            timeline = self.get_historic_tweets(api, user_id)
            collected_at = datetime.now()

            for tweet in timeline:
                dbrow = (
                    self.user_set_id,
                    user_id,
                    tweet["id_str"],
                    collection_bucket_ts,
                    collected_at,
                    json.dumps(tweet)
                )
                all_rows.append(dbrow)

        notifications.notify_all(self.notifiers, "Finished collecting timelines for {} user_ids".format(len(user_ids)), notify_type="complete")

        return all_rows


    ## Gets 3200 of the most recent tweets associated with the given uid before before_id (or the 3200 most recent tweets if before_id is None)
    ## Returns the minimum id of the list of tweets (i.e. the id corresponding to the earliest tweet)
    def get_historic_tweets_before_id(self, api, uid, max_id=None):
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
            for page in tweepy.Cursor(api.user_timeline, tweet_mode='extended', **cursor_args).pages(16):
                ## Adding the tweets to the list

                json_tweets = [tweet._json for tweet in page]

                finished, filtered_tweets = self.check_if_collection_is_finished(json_tweets)

                if finished:
                    ## Filter out any older tweets
                    json_tweets = filtered_tweets
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
                return self.get_historic_tweets_before_id(api, uid, max_id)
            elif any(code in str(ex) for code in ["401", "404"]):
                return (None, True, [])

            else:
                print(uid)
                print(ex)
                return (None, True, [])

        if tweets:
            max_id = max(tweets, key=lambda t: int(t["id_str"]))
            return (max_id, finished, tweets)

        else:
            return (None, True, [])

    ## Get a uid's tweets
    def get_historic_tweets(self, api, uid):
        max_id, finished, tweets = None, False, []
        while not finished:
            max_id, finished, returned_tweets = self.get_historic_tweets_before_id(api, uid, max_id)

            if returned_tweets:
                tweets.extend(returned_tweets)

        return tweets
