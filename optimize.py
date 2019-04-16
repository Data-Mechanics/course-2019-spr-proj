import urllib.request
import json
from shapely.geometry import Polygon, Point
import json
import dml
import prov.model
import datetime
import csv
import codecs
import uuid

import random
from scipy.cluster.vq import kmeans
import matplotlib.pyplot as pyplt

class optimize(dml.Algorithm):
    contributor = 'gasparde_ljmcgann_tlux'
    reads = []
    writes = []

    @staticmethod
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


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(optimize.contributor, optimize.contributor)
        neighborhood = list(repo[optimize.contributor + ".JamaicaPlainParcels"].find())
        print(neighborhood[0])
        M = []
        x = []
        y = []
        for i in range(len(neighborhood)):
            shape = optimize.geojson_to_polygon(neighborhood[i]["geometry"])[0]
            row = [tuple(shape.centroid.coords)[0][1], tuple(shape.centroid.coords)[0][0]]
            weight = max(int(neighborhood[i]["score_improvement"] // 2),1)
            print(weight)
            for i in range(weight):
                rand = random.random() / 10000
                x.append(row[0]+ rand)
                y.append(row[1] + rand)
                M.append([row[0]+ rand, row[1] + rand])
        # pyplt.scatter(x,y, s = .5)
        output = kmeans(M, 5)
        # means = list(output)[0]
        # mean_x = []
        # mean_y = []

        # for i in means:
        #     mean_x.append(i[0])
        #     mean_y.append(i[1])
        # pyplt.scatter(mean_x, mean_y)
        pyplt.show()


    @staticmethod
    def provenance():
        return 0


optimize.execute()