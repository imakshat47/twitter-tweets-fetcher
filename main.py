# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import pymongo
import os
from os import environ

# Variables that contain the user credentials to access Twitter API
consumer_key = environ['C_KEY']
consumer_secret = environ['C_SEC']
access_token = environ['A_TOKEN']
access_token_secret = environ['A_SEC']

# This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):
    def __init__(self):
        super().__init__()
        self.max_tweets = 50000
        self.tweet_count = 0
        self.tweets = []
        self.client = pymongo.MongoClient(environ['MONGO_URI'])
        db = self.client['transio']
        self.col = db['tweets']

    def on_timeout(self):
        print("TimeOut !!")

    def on_status(self, status):
        if(self.tweet_count == self.max_tweets):
            self.client.close()
            return(False)
        else:
            try:
                tweet = status.retweeted_status.text
            except AttributeError as e:
                tweet = status.text
            self.tweet_count += 1
            _lang = status.lang
            self.col.insert_one({"text": tweet, "lang": _lang})
        return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == '__main__':

    print("Process Starts /-/-/")
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, StdOutListener())
    stream.filter(track=['up', 'bjp', 'farmlaw', "law", 'COVID', 'BB'])
    print("Process Ends /-/-/")

# Command to run process
# python twitter_streaming.py > twitter__dataset.json