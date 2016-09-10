#!/usr/bin/env python
import time
import json

from tweepy.streaming import StreamListener

class Listener(StreamListener):
    def __init__(self, start_time, time_limit=60):
        self.time = start_time
        self.limit = time_limit
        self.tweet_data = []

    def on_data(self, data):
        with open('tweets.json', 'w') as save_file:
            while (time.time() - self.time) < self.limit:
                try:
                    self.tweet_data.append(data)
                    return True
                except BaseException, exc:
                    print 'failed ondata,', str(exc)
                    time.sleep(3)

            json.dump(data, save_file)
            return False
