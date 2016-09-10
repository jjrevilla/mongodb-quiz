#!/usr/bin/env python

import argparse
import json
import time
import os.path
from bson.code import Code

from tweepy import Stream
from tweepy import OAuthHandler
from pymongo import errors

import conf as locale
from utils import mongodb_connect
from listener import Listener

class MongoDBPipeline(object):

    def __init__(self, file_name):
        self.host = locale.MONGODB_SERVER
        self.port = locale.MONGODB_PORT
        self.database = locale.MONGODB_DB
        self.collection = locale.MONGODB_COLLECTION
        self.file_name = self.get_file(file_name)

        self.load_data()

    def get_file(self, file_name):
        if os.path.exists(file_name):
            self.file_name = file_name
        else:
            print 'Invalid path file or file does not exist'
            self.file_name = None

    def load_data(self):
        if self.file_name is not None:
            with open(self.file_name, 'r') as json_data:
                with mongodb_connect(
                    self.host, self.port,
                    self.database, self.collection) as collection:
                    collection.delete_many({}) # Cleaning MONGODB collection
                    for line in json_data:
                        try:
                            data = json.loads(line)
                            collection.insert(data)
                        except errors.DuplicateKeyError as dke:
                            print "Duplicate Key Error: ", dke
                        except ValueError as err:
                            print "Value Error: ", err


def load_data_example(file_name):
    host = locale.MONGODB_SERVER
    port = locale.MONGODB_PORT
    database = locale.MONGODB_DB
    collection = locale.MONGODB_COLLECTION
    with open(file_name, 'r') as json_data:
        with mongodb_connect(host, port, database, collection) as colection:
            colection.delete_many({}) # Cleaning MONGODB collection
            for line in json_data:
                data = json.loads(line)
                colection.insert_one(data)

    # Examples, Map Reduce Operations
    _map = Code("function () {"
                "for(var i=0; i< this.courses.length; i++){"
                "emit(this.name, this.courses[i].grade)"
                "}"
                "}")
    _reduce = Code("function (key, values) {"
                   "var average = Array.sum(values)/values.length;"
                   "if(average > 12)"
                   "return average;"
                   "}")

    _map_two = Code("function () {"
                    "for(var i=0; i< this.courses.length; i++)"
                    "emit(this.courses[i].name, this.courses[i].grade)"
                    "}")
    _reduce_two = Code("function (key, values) {"
                       "return Array.sum(values)/values.length"
                       "}")

    with mongodb_connect(host, port, database, collection) as colection:
        result = colection.map_reduce(_map, _reduce, "myresults")
        print '\nStudents with grade average greater than 12'
        for doc in result.find():
            print doc
        result = colection.map_reduce(_map_two, _reduce_two, "myresults")
        print '\nAverage Grade for Course'
        for doc in result.find():
            print doc

def load_data_tweeter(file_name):
    host = locale.MONGODB_SERVER
    port = locale.MONGODB_PORT
    database = locale.MONGODB_DB
    collection = locale.MONGODB_COLLECTION

    # OAuth process, using the keys and tokens
    auth = OAuthHandler(locale.CONSUMER_KEY, locale.CONSUMER_SECRET)
    auth.set_access_token(locale.ACCESS_TOKEN, locale.ACCESS_TOKEN_SECRET)

    start_time = time.time()
    keyword_list = ['tweets'] # Trank list to search

    # Initialize Stream object with a time out limit
    twitter_stream = Stream(auth, Listener(start_time, time_limit=3))

    # Call the filter method to run the Stream Object
    twitter_stream.filter(track=keyword_list, languages=['en'])

    with open(file_name, 'r') as json_data:
        data = json.load(json_data)
        with mongodb_connect(host, port, database, collection) as colection:
            colection.delete_many({}) # Cleaning MONGODB collection
            data = json.loads(data)
            colection.insert(data)

            tweets_iterator = colection.find()
            for tweet in tweets_iterator:
                msg = u'{0} said via twitter: {1}'.format(
                    tweet['user']['name'], tweet['text'])
                print msg

            colection.find({"retweeted_status" :{"$exists": "true"}})

def main():
    parser = argparse.ArgumentParser(description="Import records into MongoDB")
    parser.add_argument("path",
                        help="The json file path for importing.")
    args = parser.parse_args()

    print 'First example:'
    load_data_example('example.json')
    print '\nSecond example:\n'
    load_data_tweeter('tweets.json')

    print '\nImporting Data to MONGODB from FILE'
    MongoDBPipeline(args.path)

if __name__ == '__main__':
    main()
