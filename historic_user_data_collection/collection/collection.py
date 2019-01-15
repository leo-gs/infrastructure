import tweepy
from tweepy.auth import OAuthHandler

from db_connect import *
from utilities.notifications import *


class Collection:

    def __init__(self, user_set_id, user_set_prefix, db,
                 notifiers=[]):
        raise NotImplemented("`Collection` is an abstract base class.")

    def create_db_table(self):
        raise NotImplemented("`Collection` is an abstract base class.")

    def collect(self):
        raise NotImplemented("`Collection` is an abstract base class.")

    def upload_data(self, data):
        raise NotImplemented("`Collection` is an abstract base class.")

    def get_name(self):
        raise NotImplemented("`Collection` is an abstract base class.")
