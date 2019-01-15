import tweepy
from tweepy.auth import OAuthHandler


def read_credential_file(fpath):
    credentials = {}
    with open(fpath) as f:
        for line in f.readlines():
            key, value = line.strip().split("=")
            credentials[key] = value
        return credentials


def get_twitter_api_obj(consumer_key, consumer_secret, access_token, access_token_secret):

    # Authenticating
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Returning the authenticated API object
    api = tweepy.API(auth)

    return api
