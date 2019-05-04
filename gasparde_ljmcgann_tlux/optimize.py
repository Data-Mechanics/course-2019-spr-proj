import datetime
import uuid

import dml
import prov.model
from scipy.cluster.vq import kmeans
from shapely.geometry import Polygon
from rtree import index


class optimize(dml.Algorithm):
    contributor = 'gasparde_ljmcgann_tlux'
    reads = [contributor + ".Neighborhoods", contributor + ".ParcelsCombined", contributor + ".Statistics"]
    writes = [contributor + ".KMeans"]

    @staticmethod
    def compute_weight(dist_score, dist_mean, dist_stdev, health_score, health_mean, health_stdev, weight):
        dist_z_score = ((dist_score - dist_mean) / dist_stdev) * (1 - (weight / 100))
        # print("dist", dist_z_score)
        health_z_score = ((health_score - health_mean) / health_stdev) * ((weight / 100))
        # print("health", health_z_score)
        average_z_score = (dist_z_score + health_z_score)

        if average_z_score > 1.5:
            return 100
        elif average_z_score > 1:
            return 10
        else:
            return 1
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
    def compute_kmeans(neighborhood, num_means, passed_weight):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gasparde_ljmcgann_tlux', 'gasparde_ljmcgann_tlux')
        parcels = repo['gasparde_ljmcgann_tlux' + ".ParcelsCombined"]
        neighborhood_parcels = list(parcels.find({"Neighborhood": neighborhood}))
        stats = repo['gasparde_ljmcgann_tlux' + ".Statistics"]
        dist_mean = float(
            stats.find_one({"Neighborhood": neighborhood, "variable": "distance_score", "statistic": "mean"})["value"])
        dist_stdev = float(
            stats.find_one({"Neighborhood": neighborhood, "variable": "distance_score", "statistic": "std_dev"})[
                "value"])
        health_mean = float(
            stats.find_one({"Neighborhood": neighborhood, "variable": "health_score", "statistic": "mean"})["value"])
        health_stdev = float(
            stats.find_one({"Neighborhood": neighborhood, "variable": "health_score", "statistic": "std_dev"})["value"])

        kmean = []
        parcel_index = index.Index()
        for i in range(len(neighborhood_parcels)):
            shape = optimize.geojson_to_polygon(neighborhood_parcels[i]["geometry"])[0]
            parcel_index.insert(i, shape.bounds)
            # out of order, want [latitude, longitude]
            coords = [shape.centroid.coords[0][1], shape.centroid.coords[0][0]]
            weight = optimize.compute_weight(neighborhood_parcels[i]["distance_score"], dist_mean, dist_stdev,
                                    neighborhood_parcels[i]["health_score"], health_mean, health_stdev, passed_weight)

            for _ in range(weight):
                kmean.append([coords[0], coords[1]])
        output = kmeans(kmean, num_means)[0].tolist()
        dict = {"kmeans": str(output)}
        dict["Avg_Land_Val"] = []
        dict["Dist_To_Park"] = []
        dict["Avg_Health"] = []
        for mean in output:
            point = (mean[1], mean[0], mean[1], mean[0])
            # get 5 nearest parcels to k-means point to compute a location's statistics
            bounds = [i for i in parcel_index.nearest(point, 5)]
            avg_val = 0
            dist_to_park = 0
            health_score = 0
            # take only five observations in case there are more due to ties
            for ij in bounds[:5]:
                avg_val += round(
                    float(neighborhood_parcels[ij]["AV_TOTAL"]) / float(neighborhood_parcels[ij]["LAND_SF"]), 2)
                dist_to_park += float(neighborhood_parcels[ij]["min_distance_km"])
                health_score += float(neighborhood_parcels[ij]["health_score"])
            dict["Avg_Land_Val"].append(round(avg_val / 5, 2))
            dict["Dist_To_Park"].append(round(dist_to_park / 5, 2))
            dict["Avg_Health"].append(round(health_score / 5, 2))

        return dict

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(optimize.contributor, optimize.contributor)
        # read in neighborhoods to get the list of their names
        neighborhoods = list(repo[optimize.contributor + ".Neighborhoods"].find())
        repo.dropCollection(optimize.contributor + ".KMeans")
        repo.createCollection(optimize.contributor + ".KMeans")
        for i in range(len(neighborhoods)):
            name = neighborhoods[i]["properties"]["Name"]
            distance_kmeans = optimize.compute_kmeans(name, 5, 0)
            health_kmeans = optimize.compute_kmeans(name, 5, 100)
            distance_kmeans["metric"] = "distance_score"
            health_kmeans["metric"] = "health_score"
            if len(distance_kmeans) > 0:
                repo[optimize.contributor + ".KMeans"].insert_one(distance_kmeans)
                repo[optimize.contributor + ".KMeans"].insert_one(health_kmeans)

        repo[optimize.contributor + ".KMeans"].metadata({'complete': True})

        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        this_script = doc.agent('alg:gasparde_ljmcgann_tlux#optimize',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'],
                                 'ont:Extension': 'py'})

        Neighborhoods = doc.entity('dat:gasparde_ljmcgann_tlux#Neighborhoods',
                                   {prov.model.PROV_LABEL: 'Shape of Boston Neighborhoods',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})
        ParcelsCombined = doc.entity('dat:gasparde_ljmcgann_tlux#ParcelCombined',
                                     {prov.model.PROV_LABEL: 'Final Dataset Produced for Optimization and Analysis',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})
        Stats = doc.entity('dat:gasparde_ljmcgann_tlux#Statistics',
                           {prov.model.PROV_LABEL: 'Various Statistics on Health and Open Space Data',
                            prov.model.PROV_TYPE: 'ont:DataSet'})

        getOptimization = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(getOptimization, this_script)
        doc.usage(getOptimization, Neighborhoods, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(getOptimization, ParcelsCombined, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(getOptimization, Stats, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        Optimization = doc.entity('dat:gasparde_ljmcgann_tlux#KMeans',
                                  {prov.model.PROV_LABEL: 'Performs K-Means on Distance and Health Metrics',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(Optimization, this_script)

        doc.wasGeneratedBy(Optimization, getOptimization, endTime)

        doc.wasDerivedFrom(Optimization, Neighborhoods, getOptimization, getOptimization,
                           getOptimization)
        doc.wasDerivedFrom(Optimization, ParcelsCombined, getOptimization, getOptimization,
                           getOptimization)
        doc.wasDerivedFrom(Optimization, Stats, getOptimization, getOptimization,
                           getOptimization)
        return doc

