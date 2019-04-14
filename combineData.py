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
from math import *
from tqdm import tqdm
from rtree import index

class combineData(dml.Algorithm):
    contributor = 'gasparde_ljmcgann_tlux'
    reads = [ contributor + ".CensusTractShape", contributor + ".CensusTractHealth",
              contributor + ".Neighborhoods", contributor + ".ParcelAssessments",
              contributor + ".ParcelGeo"]
    writes = []

    @staticmethod
    def haversine(point1, point2):
        """
        computes correct distance between two points given
        :param point1:
        :param point2:
        :return:
        """
        lon1 = point1[1]
        lon2 = point2[1]
        lat1 = point1[0]
        lat2 = point2[0]

        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1

        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(min(1, sqrt(a)))
        r = 6371  # Radius of earth in kilometers. Use 3956 for miles

        return c * r

    @staticmethod
    def geo_distance(point, poly2):
        min_distance = 1000
        for other_point in list(poly2.exterior.coords):
            min_distance = min(min_distance, combineData.haversine((point.x, point.y), other_point))
        return min_distance

    @staticmethod
    def score(neighborhood):
        score = 0
        for i in neighborhood:
            score += i["min_distance"]
        return score

    @staticmethod
    def create_neighborhood_dict(neighborhoods):
        """
        returns dictionary with keys of neighborhoods whose value is an
        empty list
        :return:
        """
        val = {}
        for neighborhood in neighborhoods:
            val[neighborhood['properties']['Name']] = []
        return val

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
        name = "gasparde_ljmcgann_tlux"
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(combineData.contributor, combineData.contributor)

        ####################################################################

        # putting opens spaces into their respective neighborhoods
        # if an open spaces is in or overlaps a neighborhoods we put this
        # into the neighborhood, thus allows for an open space to be in
        # multiple neighborhoods
        open_spaces = list(repo[combineData.contributor + ".OpenSpaces"].find())
        neighborhoods = list(repo[combineData.contributor + ".Neighborhoods"].find())
        open_spaces_by_neighborhood = combineData.create_neighborhood_dict(neighborhoods)
        for open_space in open_spaces:
            for neighborhood in neighborhoods:
                found = False
                neighborhood_shapely = combineData.geojson_to_polygon(neighborhood["geometry"])
                op_s_shapely = combineData.geojson_to_polygon(open_space["geometry"])
                for op_s_shape in op_s_shapely:
                    for neighborhood_shape in neighborhood_shapely:
                        if op_s_shape.intersects(neighborhood_shape):
                            open_spaces_by_neighborhood[neighborhood['properties']['Name']].append(open_space)
                            found = True
                            break
                        if found:
                            break
        print(open_spaces_by_neighborhood["Allston"][0])
        parcels_by_neighborhood = combineData.create_neighborhood_dict(neighborhoods)



    @staticmethod
    def provenance():
        return 0
combineData.execute()
