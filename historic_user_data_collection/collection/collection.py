import json
import os

import tweepy
from tweepy.auth import OAuthHandler

from db_connect import *
from utilities import notifications


class Collection:

    def __init__(self, user_set_id, user_set_prefix, db,
                 notifiers=[]):
        raise NotImplemented("`Collection` is an abstract base class.")

    def create_db_table(self):
        raise NotImplemented("`Collection` is an abstract base class.")

    def collect(self):
        raise NotImplemented("`Collection` is an abstract base class.")

    def upload_data(self, data):
        notifications.notify_all(self.notifiers, "Starting {} data upload for {} rows".format(self.name, len(data)), notify_type="start")

        self.db.execute_sql(sql=self.table_insert, args=data, commit=True, batch_insert=True)

        notifications.notify_all(self.notifiers, "Uploaded {} rows to {}".format(len(data), self.table_name), notify_type="complete")


    def dump_data(self, data, user_set_name, ts):
        if not os.path.isdir("/data/historic_twitter_data"):
            raise RuntimeError("`/data/historic_twitter_data` must exist and be writable for JSON dumps")

        collection_name = self.name
        table_name = self.table_name
        ts_name = ts.strftime("%Y%m%d_%H%M")

        data_dir = "/data/historic_twitter_data/{}/{}".format(user_set_name, collection_name)
        data_file = "{}_{}.json".format(table_name, ts_name)

        os.makedirs(data_dir, exist_ok=True)

        def clean(elt):
            if elt.__class__.__name__ == "datetime":
                return elt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                return elt

        data_cleaned = [[clean(elt) for elt in row] for row in data]

        with open(os.path.join(data_dir, data_file), "w+") as f:
            json.dump(data_cleaned, f)
