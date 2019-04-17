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
from scipy.stats import pearsonr
from statistics import mean, stdev

class getStatistics(dml.Algorithm):
    contributor = 'gasparde_ljmcgann_tlux'
    reads = [contributor + ".Neighborhoods", contributor + ".ParcelsCombined"]
    writes = [contributor + ".Statistics"]

    @staticmethod
    def execute(trial = False):


        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(getStatistics.contributor, getStatistics.contributor)
        parcels = repo[getStatistics.contributor + ".ParcelsCombined"]
        neighborhoods = list(repo[getStatistics.contributor + ".Neighborhoods"].find())

        repo.dropCollection(getStatistics.contributor + ".Statistics")
        repo.createCollection(getStatistics.contributor + ".Statistics")
        for i in range(len(neighborhoods)):
            name = neighborhoods[i]["properties"]["Name"]
            data = list(parcels.find({"Neighborhood":name}))
            if len(data) > 0:
                for category in ["obesity", "asthma", "low_phys"]:
                    x = []
                    y = []
                    for i in range(len(data)):
                        y.append(float(data[i][category]))
                        x.append(float(data[i]["distance_score"]))

                    corr = pearsonr(x,y)
                    repo[getStatistics.contributor + ".Statistics"].insert_one({"Neighborhood": name , "variable": category,
                                                                         "statistic": "pearsonr", "value":corr})
                    m = mean(y)
                    repo[getStatistics.contributor + ".Statistics"].insert_one(
                        {"Neighborhood": name, "variable": category,
                         "statistic":"mean","value": m})
                    repo[getStatistics.contributor + ".Statistics"].insert_one(
                        {"Neighborhood": name, "variable": category,
                          "statistic":"std_dev","val": stdev(y, m)})

        for i in range(len(neighborhoods)):
            name = neighborhoods[i]["properties"]["Name"]
            data = list(parcels.find({"Neighborhood":name}))
            if len(data) > 0:
                x = []
                for i in range(len(data)):
                    x.append(float(data[i]["distance_score"]))
                m = mean(x)
                repo[getStatistics.contributor + ".Statistics"].insert_one(
                    {"Neighborhood": name, "variable": "distance_score",
                     "statistic":"mean","value": m})
                repo[getStatistics.contributor + ".Statistics"].insert_one(
                    {"Neighborhood": name, "variable": "distance_score",
                     "statistic":"std_dev","value": stdev(x,m)})


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        return 0
