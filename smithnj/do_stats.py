import json
import prov.model
import datetime
import uuid
import dml
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sklearn
from sklearn.cluster import KMeans
from mpl_toolkits.mplot3d import Axes3D
from sklearn.preprocessing import scale
import sklearn.metrics as sm
from scipy.stats.stats import pearsonr

############################################
# do_stats.py
# Script for performing statistical analysis on gathered metrics.
############################################

class do_stats(dml.Algorithm):

    contributor = 'smithnj'
    reads = ['smithnj.zones']
    writes = ['']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        # ---[ Initialize Cursors ]----------------------------------
        zones_cursor = repo.smithnj.zones.find()
        # ---[ Staging & Scaling ]-----------------------------------
        metrics_data = []
        for i in zones_cursor:
            metrics_data.append((i["StationID"], i["Popularity"], i["HardshipIndex"], i["TaxiDemand"]))
        popularity = [b for (a, b, c, d) in metrics_data if b is not None]
        hardship = [c for (a, b, c, d) in metrics_data if c is not None]
        taxidemand = [d for (a, b, c, d) in metrics_data if d is not None]
        # ---[ Stats Operations ]-----------------------------------
        if (trial):
            Corr_popularity_hardship = pearsonr(popularity[0:3], hardship[0:3])
            Corr_popularity_taxi = pearsonr(popularity[0:3], taxidemand[0:3])
            Corr_hardship_taxi = pearsonr(hardship[0:3], taxidemand[0:3])
        else:
            Corr_popularity_hardship = pearsonr(popularity[0:123], hardship[0:123])
            Corr_popularity_taxi = pearsonr(popularity[0:123], taxidemand[0:123])
            Corr_hardship_taxi = pearsonr(hardship[0:123], taxidemand[0:123])
        print("Popularity and Hardship Correlation Coeff:" + str(Corr_popularity_hardship[0]))
        print("Popularity and Taxi Demand Correlation Coeff:" + str(Corr_popularity_taxi[0]))
        print("Hardship and Taxi Demand Correlation Coeff: " + str(Corr_hardship_taxi[0]))
        # ---[ Finishing Up ]---------------------------------------
        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/stats')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/stats')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/smithnj/')

        this_script = doc.agent('alg:smithnj#stats',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:metrics',
                              {'prov:label': 'station id, popularity, hardship index, taxi demand',
                               prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})

        transformation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transformation, this_script)
        doc.usage(transformation, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Transformation',
                   'ont:Query': ''
                   }
                  )
        do_stats = doc.entity('dat:smithnj#stats',
                                        {prov.model.PROV_LABEL: 'do_stats',
                                         prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(do_stats, this_script)
        doc.wasGeneratedBy(do_stats, transformation, endTime)
        doc.wasDerivedFrom(do_stats, resource, transformation, transformation, transformation)

        repo.logout()

        return doc