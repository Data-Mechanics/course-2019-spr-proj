#import jsonschema
from flask import Flask, jsonify, abort, make_response, request,render_template
from bson.json_util import dumps
import dml
import datetime
import geopy.distance
import json
import prov.model
import pprint
import random
import uuid
#from flask.ext.httpauth import HTTPBasicAuth

app = Flask(__name__)

reads = ['henryhcy_jshen97_leochans_wangyp.neighborhoods']
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('henryhcy_jshen97_leochans_wangyp', 'henryhcy_jshen97_leochans_wangyp')

boston_neighbors=repo.henryhcy_jshen97_leochans_wangyp.neighborhoods.find_one()

#print(type(boston_neighbors))
#print(boston_neighbors)




@app.route('/')
def index():
    map_data =dumps(boston_neighbors)
    return render_template('index.html',map_data = map_data)


if __name__ == '__main__':
    app.run(debug=True)