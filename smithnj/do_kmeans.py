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
############################################
# do_kmeans.py
# Script for performing k_means cluster analysis on the metrics database.
############################################
class do_kmeans(dml.Algorithm):

    contributor = 'smithnj'
    reads = ['smithnj.metrics']
    writes = ['smithnj.zones']

    @staticmethod
    def execute(trial = False):
        # ---[ Assistant Functions ]---------------------------------
        def project(R, p):
            return [p(t) for t in R]

        startTime = datetime.datetime.now()
        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.zones'
        # ---[ Initialize Cursors ]----------------------------------
        metrics_cursor = repo.smithnj.metrics.find()
        # ---[ Transformations ]-------------------------------------
        metrics_list = []
        metrics_data = []
        for i in metrics_cursor:
            metrics_list.append([i["Station ID"], i["Popularity"], i["Hardship Index"], i["Taxi Demand"]])
            metrics_data.append([i["Popularity"], i["Hardship Index"], i["Taxi Demand"]])
        # ---[ Send to Dict ]----------------------------------------
        labels = lambda t: {'Station ID': t[0], 'Popularity': t[1], 'Hardship Index': t[2], 'Taxi Demand': t[3]}
        result = project(metrics_list, labels)
        # ---[ Staging & Scaling ]-----------------------------------
        metrics_data = [[a, b, c] for [a, b, c] in metrics_data if (b is not None) and (
                c is not None)]  # Sadly ~20 stations must be ignored since they do not reside in chicago proper.
        df = pd.DataFrame.from_dict(result)
        df.dropna()  # Sadly ~20 stations must be ignored since they do not reside in chicago proper.
        X = scale(metrics_data)  # Scale the data.
        Y = df
        variable_names = ['Popularity', 'Hardship Index', 'Taxi Demand']
        # ---[ KMeans Operation ]-----------------------------------
        fignum = 1
        if (trial):
            titles = ["8 clusters/zones", "3 clusters/zones"]
            estimators = [('kmeans_8', KMeans(n_clusters=8)), ('kmeans_3', KMeans(n_clusters=3))]
        else:
            titles = ["8 clusters/zones", "6 clusters/zones", "4 clusters/zones", "3 clusters/zones"]
            estimators = [('kmeans_8', KMeans(n_clusters=8)), ('kmeans_6', KMeans(n_clusters=6)),
                      ('kmeans_4', KMeans(n_clusters=4)), ('kmeans_3', KMeans(n_clusters=3))]
        clusters = []
        for name, est in estimators:
            est.fit(X)
            y_kmeans = est.predict(X)
            three = plt.figure().gca(projection='3d')
            three.scatter(X[:, 0], X[:, 1], X[:, 2], c=y_kmeans, cmap='viridis')
            three.set_xlabel('Hardship Index')
            three.set_ylabel('Popularity')
            three.set_zlabel('Taxi Demand')
            centers = est.cluster_centers_
            three.scatter(centers[:, 0], centers[:, 1], centers[:, 2], c='black', s=200, alpha=0.5)
            title = "Figure #" + str(fignum) + ": " + titles[fignum - 1]
            plt.title(title)
            # plt.show()  # - uncomment to show graphs
            fignum = fignum + 1
            clusters.append(y_kmeans)
        # ---[ MongoDB Insertion ]-----------------------------------
        df_clusters = pd.DataFrame.from_records(clusters)
        df_clusters = df_clusters.transpose()
        df_final = df.join(df_clusters)
        if (trial):
            columns = ["HardshipIndex", "TaxiDemand", "StationID", "Hardship", "8Zones", "3Zones"]
        else:
            columns = ["HardshipIndex", "TaxiDemand", "StationID", "Hardship", "8Zones", "6Zones", "4Zones", "3Zones"]
        df_final.columns = columns
        # ---[ MongoDB Insertion ]-----------------------------------
        df = df_final.to_json(orient="records")
        loaded = json.loads(df)
        repo.dropCollection(repo_name)
        repo.createCollection(repo_name)
        print('done')
        repo[repo_name].insert_many(loaded)
        repo[repo_name].metadata({'complete': True})
        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/zones')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/zones')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/data/smithnj/')

        this_script = doc.agent('alg:smithnj#zones',
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
        do_kmeans = doc.entity('dat:smithnj#zones',
                                        {prov.model.PROV_LABEL: 'do_kmeans',
                                         prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(do_kmeans, this_script)
        doc.wasGeneratedBy(do_kmeans, transformation, endTime)
        doc.wasDerivedFrom(do_kmeans, resource, transformation, transformation, transformation)

        repo.logout()

        return doc