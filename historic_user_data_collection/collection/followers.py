from datetime import datetime
import time

import tweepy
from tweepy.auth import OAuthHandler

from collection import collection
from db_connect import database, SQL
from utilities import notifications

class Followers(collection.Collection):

    def __init__(self, user_set_id, user_set_prefix, db,
                 notifiers=[]):
        self.user_set_id = user_set_id
        self.user_set_prefix = user_set_prefix
        self.db = db
        self.notifiers = notifiers

        self.table_name = "{}_user_follower".format(user_set_prefix)

        self.table_create = """
            CREATE TABLE IF NOT EXISTS {} (
              user_set_id TEXT,
              user_id BIGINT,
              collection_bucket_ts TIMESTAMP,
              collected_ts TIMESTAMP,
              follower_id BIGINT,
              PRIMARY KEY (user_set_id, user_id, collected_ts, follower_id)
            )
        """.format(self.table_name)

        self.table_insert = """
            INSERT INTO {} (user_set_id, user_id, collection_bucket_ts,
            collected_ts, follower_id)
            VALUES (%s, %s, %s, %s, %s)
        """.format(self.table_name)

        db.execute_sql(self.table_create, commit=True)

        notifications.notify_all(notifiers,
                   "Followers collection uploading to `{}`".
                   format(self.table_name), notify_type="info")

    def collect(self, api, user_ids, collection_bucket_ts):
        followerships = []
        # Keeping track of which accounts throw errors (not doing anything with this right now though)
        error_accounts = []

        notifications.notify_all(self.notifiers, "Starting follower collection for {} user_ids".format(len(user_ids)), notify_type="start")

        for user_id in user_ids:
            collected_at = datetime.now()

            def pull_followers(user_id):
                try:
                    for page in tweepy.Cursor(api.followers_ids, id=user_id).pages():
                        followerships.extend([(
                            self.user_set_id,
                            user_id,
                            collection_bucket_ts,
                            collected_at,
                            follower_id
                        ) for follower_id in page])

                    # We get 15 requests per 15-window or 1 request per 60 seconds
                    time.sleep(60)

                except tweepy.RateLimitError:
                    time.sleep(15 * 60)
                    user_followerships = pull_followers(user_id)
                    followerships.extend(user_followerships)

                except tweepy.TweepError as ex:
                    error_accounts.append(
                        (user_id, ex.response.status_code, ex.response.text))

            pull_followers(user_id)
        notifications.notify_all(self.notifiers, "Finished collecting followers for {} user_ids".format(len(user_ids)), notify_type="complete")

        return followerships

    def upload_data(self, data):
        # process data into rows here
        self.db.execute_sql(self.table_insert, data, commit=True,
                                  batch_insert=True)
        notifications.notify_all(self.notifiers, "Uploaded {} rows to {}".format(
            len(data), self.table_name))

    def get_name(self):
        return "followers"
