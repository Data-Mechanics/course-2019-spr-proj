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

# censushealth = dumps(repo[contributor + ".CensusTractHealth"].find())
# kmeans = dumps(repo[contributor + ".KMeans"].find())
# stats = dumps(repo[contributor + ".Statistics"].find())
neighborhoods = dumps(repo[contributor + ".Neighborhoods"].find())


openspaces = dumps(repo[contributor + ".OpenSpaces"].find())
# parcelgeo = dumps(repo[contributor + ".ParcelGeo"].find())
# assessments = dumps(repo[contributor + ".ParcelAssessments"].find())
# censusshape = dumps(repo[contributor + ".CensusTractShape"].find())
################################################################################

# neighborhoods_index = index.Index()
# neighborhoods_flattened = []
# for neighborhood in neighborhoods_data:
#     geom = neighborhood["geometry"]
#     if geom['type'] == 'Polygon':
#         shape = []
#         coords = geom['coordinates']
#         for i in coords[0]:
#             shape.append((i[0], i[1]))
#         neighborhoods_flattened.append([shape, neighborhood["properties"]["OBJECTID"]])
#     if geom['type'] == 'MultiPolygon':
#         coords = geom['coordinates']
#         for i in coords:
#             shape = []
#             for j in i:
#                 for k in j:
#                     # need to change list type to tuple so that shapely can read it
#                     shape.append((k[0], k[1]))
#             neighborhoods_flattened.append([shape, neighborhood["properties"]["OBJECTID"]])

# neighborhoods = dumps(neighborhoods_flattened)

# print(neighborhoods_flattened)


# print(kmeans)


@app.route("/")
@app.route('/index')
def hello():
    return render_template("index.html",
                           #   censushealth=censushealth,
                           #  kmeans=kmeans,
                           # stats=stats,
                           neighborhoods=neighborhoods,
                           openspaces=openspaces,
                           # parcelgeo=parcelgeo,
                           # assessments=assessments,
                           # censusshape=censusshape
                           )


if __name__ == "__main__":
    app.run(port=5000, debug=True)
