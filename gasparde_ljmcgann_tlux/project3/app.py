import urllib.request
import json
from bson.json_util import dumps
from shapely.geometry import Polygon
from flask import Flask, jsonify, abort, make_response, request, render_template
from flask_pymongo import PyMongo
# from flask_cors import CORS, cross_origin
################################################################################
##################  kmeans  ####################################################
################################################################################
from scipy.cluster.vq import kmeans
import dml
from shapely.geometry import Polygon

app = Flask(__name__)
# CORS(app)

contributor = "gasparde_ljmcgann_tlux"
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(contributor, contributor)


def geojson_to_polygon(geom):
    """

    :return: list of shapely polygons corresponding to the geojson object
    """
    polys = []
    if geom['type'] == 'Polygon':
        shape = []
        coords = geom['coordinates']
        for i in coords[0]:
            shape.append((i[0], i[1]))
        polys.append(Polygon(shape))
    if geom['type'] == 'MultiPolygon':
        coords = geom['coordinates']
        for i in coords:
            shape = []
            for j in i:
                for k in j:
                    # need to change list type to tuple so that shapely can read it
                    shape.append((k[0], k[1]))
            poly = Polygon(shape)
            polys.append(poly)
    return polys


def health_score(row):
    average = (float(row["obesity"]) + float(row["low_phys"]) + float(row["asthma"])) // 3
    # we implement this scale to exagerate weights
    # in the future should implement method to change how
    # we weight
    if average > 20:
        return 100
    elif average > 15:
        return 10
    else:
        return 1


def distance_score(distance_score, stdev, mean):
    z_score = (distance_score - mean) / (stdev)
    if z_score > 1.5:
        return 100
    elif z_score > .75:
        return 10
    else:
        return 1


def compute_weight(dist_score, dist_mean, dist_stdev, health_score, health_mean, health_stdev):
    dist_z_score = (dist_score - dist_mean) / dist_stdev
    health_z_score = (health_score - health_mean) / health_stdev
    average_z_score = (dist_z_score + health_z_score) / 2
    if average_z_score > 1.5:
        return 100
    elif average_z_score > .75:
        return 10
    else:
        return 1


def compute_kmeans(neighborhood, num_means=3):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('gasparde_ljmcgann_tlux', 'gasparde_ljmcgann_tlux')
    parcels = repo['gasparde_ljmcgann_tlux' + ".ParcelsCombined"]
    neighborhood_parcels = list(parcels.find({"Neighborhood": neighborhood}))
    stats = repo['gasparde_ljmcgann_tlux' + ".Statistics"]
    print(neighborhood)
    print(list(stats.find({"Neighborhood": neighborhood, "variable": "health_score"})))
    dist_mean = float(
        stats.find_one({"Neighborhood": neighborhood, "variable": "distance_score", "statistic": "mean"})["value"])
    dist_stdev = float(
        stats.find_one({"Neighborhood": neighborhood, "variable": "distance_score", "statistic": "std_dev"})["value"])
    health_mean = float(
        stats.find_one({"Neighborhood": neighborhood, "variable": "health_score", "statistic": "mean"})["value"])
    health_stdev = float(
        stats.find_one({"Neighborhood": neighborhood, "variable": "health_score", "statistic": "std_dev"})["value"])

    kmean = []

    for i in range(len(neighborhood_parcels)):
        shape = geojson_to_polygon(neighborhood_parcels[i]["geometry"])[0]
        # out of order, want [latitude, longitude]
        coords = [shape.centroid.coords[0][1], shape.centroid.coords[0][0]]
        weight = compute_weight(neighborhood_parcels[i]["distance_score"], dist_mean, dist_stdev,
                                neighborhood_parcels[i]["health_score"], health_mean, health_stdev)
        for _ in range(weight):
            kmean.append([coords[0], coords[1]])

    output = kmeans(kmean, num_means)[0].tolist()
    print(output)
    return output


# print(compute_kmeans("Allston", 10))


################################################################################

# censushealth = dumps(repo[contributor + ".CensusTractHealth"].find())
kmeans = dumps(repo[contributor + ".KMeans"].find())
# stats = dumps(repo[contributor + ".Statistics"].find())
neighborhoods = dumps(repo[contributor + ".Neighborhoods"].find())
# openspaces = dumps(repo[contributor + ".OpenSpaces"].find())
# parcelgeo = dumps(repo[contributor + ".ParcelGeo"].find())
# assessments = dumps(repo[contributor + ".ParcelAssessments"].find())
censusshape = dumps(repo[contributor + ".CensusTractShape"].find())

################################################################################

print(censusshape)


def run_kmeans(num_kmeans):
    print(num_kmeans)


@app.route("/", methods=["GET", "POST"])
@app.route('/index', methods=["GET", "POST"])
def hello():
    if request.method == "GET":
        return render_template("index.html",
                               # censushealth=censushealth,
                               kmeans=kmeans,
                               # stats=stats,
                               neighborhoods=neighborhoods,
                               # openspaces=openspaces,
                               # parcelgeo=parcelgeo,
                               # assessments=assessments,
                               censusshape=censusshape
                               )

    if request.method == "POST":
        data = request.get_json()
        print(data)
        neighborhood = data.get('neighborhood')

        num_means = request.data
        print(num_means)
        means = dumps(compute_kmeans(neighborhood))
        return render_template("index.html",
                               # censushealth=censushealth,
                               kmeans=means,
                               # stats=stats,
                               neighborhoods=neighborhoods,
                               # openspaces=openspaces,
                               # parcelgeo=parcelgeo,
                               # assessments=assessments,
                               censusshape=censusshape
                               )


if __name__ == "__main__":
    app.run(port=5000, debug=True)
