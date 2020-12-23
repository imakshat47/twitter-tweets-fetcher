# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import pymongo
import os
from os import environ

# Variables that contains the user credentials to access Twitter API
consumer_key = environ['C_KEY']
consumer_secret = environ['C_SEC']
access_token = environ['A_TOKEN']
access_token_secret = environ['A_SEC']

# This is a basic listener that just prints received tweets to stdout.


class StdOutListener(StreamListener):
    def __init__(self):
        super().__init__()
        self.max_tweets = 5000
        self.tweet_count = 0        
        self.tweets = []

    def on_status(self, status):
        if(self.tweet_count == self.max_tweets):
            # self.file.write("\n]")
            print("tweets => ", self.tweets)
            client = pymongo.MongoClient(environ['MONGO_URI'])
            db = client['transio']
            col = db['tweets']            
            col.insert_one({"text": self.tweets })
            client.close()            
            return(False)
        else:            
            try:
                tweet = status.retweeted_status.text
            except AttributeError as e:
                tweet = status.text            
            self.tweet_count += 1
            self.tweets.append(tweet)

        return True

    def on_timeout(self):
        print("TimeOut :", self.tweets)

    def on_error(self, status):
        print(status)
        return True


if __name__ == '__main__':
    
    print("Process Starts /-/-/")    
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, StdOutListener())    
    stream.filter(track=['up', 'bjp', 'farmlaw', 'COVID', 'BB'])    
    print("Process Ends /-/-/")

# Command to run process
# python twitter_streaming.py > twitter__dataset.json
