import os
import sys
from tweepy import API
from tweepy import OAuthHandler
from credentials_twitter import *

def get_twitter_auth():
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    return auth

def get_twitter_client():
    auth = get_twitter_auth()
    client = API(auth, wait_on_rate_limit=True)
    return client

