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
from tqdm import tqdm

class statistics(dml.Algorithm):
    contributor = 'gasparde_ljmcgann_tlux'
    reads = [contributor + ".CensusTractShape", contributor + ".CensusTractHealth",
             contributor + ".Neighborhoods", contributor + ".ParcelAssessments",
             contributor + ".ParcelGeo"]
    writes = []

    @staticmethod
    def execute(trial = False):


        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(statistics.contributor, statistics.contributor)
        data = list(repo[statistics.contributor + ".ParcelsCombined"].find())
        x = []
        y = []
        for i in tqdm(range(len(data))):
            if "obesity" in data[i].keys():
                y.append(float(data[i]["asthma"]))
                x.append(float(data[i]["min_distance_km"]))
        print(pearsonr(y,x))
        return 0

    @staticmethod
    def provenance():
        return 0

statistics.execute()