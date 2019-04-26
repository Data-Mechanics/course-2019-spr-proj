import urllib.request
import json
from bson.json_util import dumps
import dml
from rtree import index
from shapely.geometry import Polygon
from flask import Flask, jsonify, abort, make_response, request, render_template
from flask_pymongo import PyMongo

app = Flask(__name__)

contributor = "gasparde_ljmcgann_tlux"
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(contributor, contributor)

################################################################################
kmeans = dumps(repo[contributor + ".KMeans"].find())
neighborhoods = dumps(repo[contributor + ".Neighborhoods"].find())
openspaces = dumps(repo[contributor + ".OpenSpaces"].find())
@app.route("/")
@app.route('/index')
def hello():
    return render_template("index.html",
                           #   censushealth=censushealth,
                           #kmeans=kmeans,
                           # stats=stats,
                           neighborhoods=neighborhoods,
                           openspaces=openspaces,
                           # parcelgeo=parcelgeo,
                           # assessments=assessments,
                           # censusshape=censusshape
                           )

@app.route("/")
@app.route('/index', methods = ['GET'])
def kmeans():
    if request.method == 'GET':
        return render_template("index.html",
                               #   censushealth=censushealth,
                               #kmeans=kmeans,
                               # stats=stats,
                               neighborhoods=neighborhoods,
                               openspaces=openspaces,
                               # parcelgeo=parcelgeo,
                               # assessments=assessments,
                               # censusshape=censusshape
                               )
if __name__ == "__main__":
    app.run(port=5000, debug=True)
