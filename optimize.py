from shapely.geometry import Polygon, Point
import dml
import prov.model
import datetime
import codecs
import uuid
import random
from scipy.cluster.vq import kmeans
# import matplotlib.pyplot as pyplt

class optimize(dml.Algorithm):
    contributor = 'gasparde_ljmcgann_tlux'
    reads = [contributor + ".Neighborhoods", contributor + ".ParcelsCombined"]
    writes = [contributor + ".KMeans"]

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
        # read in neighborhoods to get the list of their names
        neighborhoods = list(repo[optimize.contributor + ".Neighborhoods"].find())
        # load in parcels as we will iterate kmeans from this data
        parcels = repo[optimize.contributor + ".ParcelsCombined"]

        repo.dropCollection(optimize.contributor + ".KMeans")
        repo.createCollection(optimize.contributor + ".KMeans")
        for i in range(len(neighborhoods)):
            name = neighborhoods[i]["properties"]["Name"]
            neighborhood = list(parcels.find({"Neighborhood":name}))
            M = []
            # x = []
            # y = []
            for j in range(len(neighborhood)):

                shape = optimize.geojson_to_polygon(neighborhood[j]["geometry"])[0]

                row = [shape.centroid.coords[0][1], shape.centroid.coords[0][0]]
                # do weighted kmeans by adding additional points
                weight = max(int(neighborhood[j]["score_improvement"]),1)
                for _ in range(weight):
                    M.append([row[0], row[1]])

                    # this was for purpose of making our scatterplots
                    # look nicer, not needed for kmeans to function properly
                    # rand = random.random() / 10000
                    # M.append([row[0] + rand, row[1] + rand])
                    # x.append(row[0]+ rand)
                    # y.append(row[1] + rand)


                # pyplt.scatter(x,y, s = .5)
                # means = list(output)[0]
                # mean_x = []
                # mean_y = []
                #
                # for i in means:
                #     mean_x.append(i[0])
                #     mean_y.append(i[1])
                # pyplt.scatter(mean_x, mean_y)
                # pyplt.show()
            output = kmeans(M, 5)[0].tolist()
            repo[optimize.contributor + ".KMeans"].insert_one({"Neighborhoods": name,"means": output})
        repo[optimize.contributor + ".KMeans"].metadata({'complete': True})



    @staticmethod
    def provenance():
        return 0


optimize.execute()