import json
import dml
import pandas
from bson.json_util import dumps


client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('zhangyb', 'zhangyb')

station = repo['zhangyb.mbta_hub']
station_json = dumps(station.find())
data = json.loads(station_json)

with open('project 3/visualization/data/mbta_hub.json', 'w') as json_file:  
    json.dump(data[0]['Data'], json_file)


