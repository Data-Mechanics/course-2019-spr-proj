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


class statistics(dml.Algorithm):
    contributor = 'gasparde_ljmcgann_tlux'
    reads = [contributor + ".Neighborhoods", contributor + ".ParcelsCombined"]
    writes = [contributor + ".Statistics"]

    @staticmethod
    def execute(trial=False):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(statistics.contributor, statistics.contributor)
        parcels = repo[statistics.contributor + ".ParcelsCombined"]
        neighborhoods = list(repo[statistics.contributor + ".Neighborhoods"].find())

        repo.dropCollection(statistics.contributor + ".Statistics")
        repo.createCollection(statistics.contributor + ".Statistics")
        for i in range(len(neighborhoods)):
            name = neighborhoods[i]["properties"]["Name"]
            data = list(parcels.find({"Neighborhood": name}))
            if len(data) > 0:
                for category in ["obesity", "asthma", "low_phys"]:
                    x = []
                    y = []
                    for i in range(len(data)):
                        y.append(float(data[i][category]))
                        x.append(float(data[i]["distance_score"]))

                    corr = pearsonr(x, y)
                    repo[statistics.contributor + ".Statistics"].insert_one({"Neighborhood": name, "variable": category,
                                                                             "statistic": "pearsonr", "value": corr})
                    m = mean(y)
                    repo[statistics.contributor + ".Statistics"].insert_one(
                        {"Neighborhood": name, "variable": category,
                         "statistic": "mean", "value": m})
                    repo[statistics.contributor + ".Statistics"].insert_one(
                        {"Neighborhood": name, "variable": category,
                         "statistic": "std_dev", "val": stdev(y, m)})

        for i in range(len(neighborhoods)):
            name = neighborhoods[i]["properties"]["Name"]
            data = list(parcels.find({"Neighborhood": name}))
            if len(data) > 0:
                x = []
                for i in range(len(data)):
                    x.append(float(data[i]["distance_score"]))
                m = mean(x)
                repo[statistics.contributor + ".Statistics"].insert_one(
                    {"Neighborhood": name, "variable": "distance_score",
                     "statistic": "mean", "value": m})
                repo[statistics.contributor + ".Statistics"].insert_one(
                    {"Neighborhood": name, "variable": "distance_score",
                     "statistic": "std_dev", "value": stdev(x, m)})

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gasparde_ljmcgann_tlux', 'gasparde_ljmcgann_tlux')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        this_script = doc.agent('alg:gasparde_ljmcgann_tlux#collect',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'],
                                 'ont:Extension': 'py'})

        Neighborhoods = doc.entity('dat:gasparde_ljmcgann_tlux#Neighborhoods',
                                   {prov.model.PROV_LABEL: 'Shape of Boston Neighborhoods',
                                    prov.model.PROV_TYPE: 'ont:DataSet'})
        ParcelsCombined = doc.entity('dat:gasparde_ljmcgann_tlux#ParcelCombined',
                                     {prov.model.PROV_LABEL: 'Final Dataset Produced for Optimization and Analysis',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})

        getStatistics = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(getStatistics, this_script)
        doc.usage(getStatistics, Neighborhoods, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(getStatistics, ParcelsCombined, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        Stats = doc.entity('dat:gasparde_ljmcgann_tlux#Statistics',
                           {prov.model.PROV_LABEL: 'Various Statistics on Health and Open Space Data',
                            prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(Stats, this_script)

        doc.wasGeneratedBy(Stats, getStatistics, endTime)

        doc.wasDerivedFrom(Stats, Neighborhoods, getStatistics, getStatistics,
                           getStatistics)
        doc.wasDerivedFrom(Stats, ParcelsCombined, getStatistics, getStatistics,
                           getStatistics)


statistics.execute()
