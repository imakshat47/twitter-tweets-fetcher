# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
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
        self.max_tweets = 10000
        self.tweet_count = 0
        # open file
        self.file = open("output.json", 'a', encoding='UTF-8')
        self.file.write("[\n")

    def on_data(self, data):
        try:
            data
        except BaseException as e:
            print(str(e))
        self.tweet_count += 1
        if(self.tweet_count == self.max_tweets):
            self.file.write("\n]")
            self.file.close()
            return(False)
        else:
            self.file.write(data + ",")
            # close file
            return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    try:
        # This handles Twitter authetification and the connection to Twitter Streaming API
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = Stream(auth, StdOutListener())
        # This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
        stream.filter(track=['up', 'bjp', 'farm', 'law', 'COVID', 'BB'])
    except Exception as e:
        print("Error => ", e)


# Command to run process
# python twitter_streaming.py > twitter__dataset.json
