#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager

from pymongo import MongoClient
from pymongo import errors


@contextmanager
def mongodb_connect(host, port, database, collection):
    connection = MongoClient(host, port)
    mydb = connection[database]
    collection = mydb[collection]

    try:
        yield collection
    except errors.ConnectionFailure, err:
        print "Could not connect to MongoDB: %s" % err
    finally:
        connection.close()
