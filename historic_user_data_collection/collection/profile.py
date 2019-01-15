from datetime import datetime
import json
import time

import tweepy
from tweepy.auth import OAuthHandler

from collection import collection
from db_connect import *
from utilities import notifications


class Profile(collection.Collection):

    def __init__(self, user_set_id, user_set_prefix, db,
                 notifiers=[]):
        self.user_set_id = user_set_id
        self.user_set_prefix = user_set_prefix
        self.db = db
        self.notifiers = notifiers

        self.table_name = "{}_user_profile".format(user_set_prefix)

        self.table_create = """
            CREATE TABLE IF NOT EXISTS {} (
              user_set_id TEXT,
              user_id BIGINT,
              collection_bucket_ts TIMESTAMP,
              collected_ts TIMESTAMP,
              user_profile_json JSON,
              PRIMARY KEY (user_set_id, user_id, collected_ts)
            )
        """.format(self.table_name)

        self.table_insert = """
            INSERT INTO {} (user_set_id, user_id, collection_bucket_ts,
            collected_ts, user_profile_json)
            VALUES (%s, %s, %s, %s, %s)
        """.format(self.table_name)

        db.execute_sql(self.table_create, commit=True)

        notifications.notify_all(notifiers,
                   "Profile data collection uploading to `{}`".
                   format(self.table_name), notify_type="info")

    def collect(self, api, user_ids, collection_bucket_ts):
        all_rows = []

        notifications.notify_all(self.notifiers, "Starting profile data collection for {} user_ids".format(len(user_ids)), notify_type="start")

        for i in range(0, len(user_ids), 100):
            user_id_chunk = user_ids[i : (i+100)]
            all_rows.extend(pull_user_data(api, user_id_chunk, self.user_set_id, collection_bucket_ts))

        notifications.notify_all(self.notifiers, "Finished collecting profile data for {} user_ids".format(len(user_ids)), notify_type="complete")

        return all_rows

    def upload_data(self, data):
        notifications.notify_all(self.notifiers, "Starting profile data upload for {} rows".format(len(data)), notify_type="start")

        self.db.execute_sql(sql=self.table_insert, args=data, commit=True,
                                  batch_insert=True)
        notifications.notify_all(self.notifiers, "Uploaded {} rows to {}".format(
            len(data), self.table_name), notify_type="complete")

    def get_name(self):
        return "user_profile"

def pull_user_data(api, user_id_chunk, user_set_id, collection_bucket_ts):
    if len(user_id_chunk) > 100:
        raise ValueError("Maximum 100 user_ids per request.")

    user_data_rows = []
    # Keeping track of which accounts throw errors (not doing anything with this right now though)
    error_accounts = []

    try:
        collected_at = datetime.now()

        users_data = api.lookup_users(user_ids=user_id_chunk)

        for user_data in users_data:
            user_data_js = json.dumps(user_data._json)
            user_id = user_data.id_str
            user_data_rows.append((
                user_set_id,
                user_id,
                collection_bucket_ts,
                collected_at,
                user_data_js
            ))

        # We get 900 requests per 15-minute window, or 1 request per second
        # Waiting a bit between each request just to be safe
        time.sleep(1)

        return user_data_rows

    except tweepy.RateLimitError:
        time.sleep(15 * 60)
        users_data = pull_user_data(api, user_id_chunk, user_set_id, collection_bucket_ts)

    except tweepy.TweepError as ex:
        error_accounts.append(
            (user_id, ex.response.status_code, ex.response.text))
