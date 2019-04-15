import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pymongo
from pymongo import MongoClient
import geoson-mongo-import

response = urllib.request.urlopen('https://data.cityofchicago.org/api/geospatial/bbvz-uum9?method=export&format=GeoJSON')
print(response)
client = MongoClient()
db = client.test_database
test = db.test

