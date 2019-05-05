import flask
from flask import Flask, render_template
import gmplot
import json
import uuid
import urllib.request
'''
import urllib.request
import json
import prov.model
import datetime
import uuid
import math
from bson.objectid import ObjectId
import mapdata as md
from random import shuffle
from math import sqrt
import gmplot
'''
app = Flask(__name__)
contributor = 'ido_jconstan_jeansolo_suitcase'

#default page  
@app.route("/", methods=['GET'])
def hello():
	# DATA SET 9 [StopsLatLng]
	# Bus Stops Latitude and Longitude
	# r9 = {'lat', 'long, 'og'}
	url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/StopsLatLng.json'
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r9 = json.loads(response)
	return render_template('busstops.html', name="hello")

@app.route("/updateMapBefore", methods=['GET', 'POST'])
def updateMapBefore():

	return render_template('busstops.html', name="test")

if __name__ == "__main__":
	app.run()