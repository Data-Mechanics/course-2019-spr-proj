import json
import dml
import pandas
from bson.json_util import dumps


client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('zhangyb', 'zhangyb')

station = repo['zhangyb.meter_crime']
station_json = dumps(station.find())
data = json.loads(station_json)[0]['Data']

with open('project 3/visualization/data/meter_crime.json', 'w') as json_file:  
    json.dump(data, json_file)
