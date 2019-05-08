import pandas as pd
import requests
import json
import dml
import prov.model
import datetime
import uuid
import csv
from io import StringIO
import json
import pymongo
import numpy as np

import matplotlib.pyplot as plt
from sklearn.cluster import KMeans


class street_clustered_location(dml.Algorithm):
    contributor = 'mmao95_dongyihe_weijiang_zhukk'
    reads = [contributor + '.streetbook_filtered']
    writes = [contributor + '.street_clustered_location']

    @staticmethod
    def execute(trial=False):
        contributor = 'mmao95_dongyihe_weijiang_zhukk'
        reads = [contributor + '.streetbook_filtered']
        writes = [contributor + '.street_clustered_location']

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        streetbook_filtered_list = list(repo[reads[0]].find())
        streetbook_filtered_df = pd.DataFrame(streetbook_filtered_list)
        streetbook_filtered_list = np.array(streetbook_filtered_df).tolist()

        street_lat_long = pd.read_csv(
            "http://datamechanics.io/data/roads_2013_jzi.csv").values.tolist()

        # process street latitude and longitude dataset
        street_lat_long_data = [(fullName, location, length) for (fId, location, s, c, tl, tf, tf2, m, fullName,
                                                                  sm, lf, lt, rf, rt, zi, zipr, length, classGroup, r,
                                                                  cluster, m, zone, bg, ct) in street_lat_long]

        # print(street_lat_long[0])
        lat_long_df = pd.DataFrame(street_lat_long_data)
        lat_long_df.columns = ['fullName', 'location', 'length']
        lat_long_df = lat_long_df.dropna()

        lat_long_list = np.array(lat_long_df).tolist()

        # normalize latitude and longitude list
        for i in range(0, len(lat_long_list)):
            lat_long_list[i][1] = lat_long_list[i][1][18: -2]

        for i in range(0, len(lat_long_list)):
            pairs = lat_long_list[i][1].split(',')
            for j in range(0, len(pairs)):
                pairs[j] = pairs[j].lstrip()

            pair_tuple = []
            for j in range(0, len(pairs)):
                str = pairs[j].split(' ')
                pair_tuple.append([float(str[0]), float(str[1])])
            lat_long_list[i][1] = pair_tuple

        # lat_long_list = [(fullName, (location, length)) for (fullName, location, length) in lat_long_list]
        name_unique = np.array(lat_long_df['fullName'].unique()).tolist()
        street_agg_list = [[fullName, [], 0] for fullName in name_unique]
        # print(type(street_agg_list[0][1]))
        # print(type(lat_long_list[0][1]))
        for i in range(0, len(street_agg_list)):
            for j in range(0, len(lat_long_list)):
                if(street_agg_list[i][0] == lat_long_list[j][0]):
                    street_agg_list[i][1].extend(lat_long_list[j][1])
                    street_agg_list[i][2] += lat_long_list[j][2]

        # process datasets for K means
        output_street_location = []
        for i in range(0, len(street_agg_list)):
            count1 = 0
            count2 = 0
            for j in range(0, len(street_agg_list[i][1])):
                count1 += street_agg_list[i][1][j][0]
                count2 += street_agg_list[i][1][j][1]
            output_street_location += [[street_agg_list[i][0], count1 / len(
                street_agg_list[i][1]), count2 / len(street_agg_list[i][1]), street_agg_list[i][2]]]
        # print(len(output_street_location))

        long_lat_list = [(long, lat) for (
            fullName, long, lat, length) in output_street_location]
        long_lat_df = pd.DataFrame(long_lat_list)
        long_lat_df.columns = ['long', 'lat']
        x = long_lat_df['long']
        y = long_lat_df['lat']
        colors = np.random.rand(len(long_lat_df))
        plt.figure(figsize=(20, 20))
        plt.scatter(x, y, c=colors, alpha=0.5)
        # plt.show()

        # set datasets into 23 clusters
        X = long_lat_df
        X = X[~np.isnan(X)]

        # K means Clustering
        def doKmeans(X, nclust):
            model = KMeans(nclust)
            model.fit(X)
            clust_labels = model.predict(X)
            cent = model.cluster_centers_
            return (clust_labels, cent)

        clust_labels, cent = doKmeans(X, 23)
        kmeans = pd.DataFrame(clust_labels)
        X.insert((X.shape[1]), 'kmeans', kmeans)

        # Plot the clusters obtained using k means
        fig = plt.figure(figsize=(20, 20))
        ax = fig.add_subplot(111)
        scatter = ax.scatter(X['long'], X['lat'],
                             c=kmeans[0], s=50)
        ax.set_title('K-Means Clustering')
        ax.set_xlabel('Long')
        ax.set_ylabel('Lat')
        plt.colorbar(scatter)

        kmeans_list = np.array(kmeans).tolist()
        for i in range(0, len(output_street_location)):
            output_street_location[i] += (kmeans_list[i])

        street_clustered = [[cluster, fullName] for (
            fullName, long, lat, length, cluster) in output_street_location]
        later_use = street_clustered
        street_clustered = [[cluster, name] for (cluster, fullName1) in street_clustered
                            for (fullName2, name, zipcode, id) in streetbook_filtered_list if fullName2.startswith(fullName1)]

        result = [[i, ] for i in range(0, 23)]
        for i in range(0, 23):
            for j in range(0, len(street_clustered)):
                if(i == street_clustered[j][0]):
                    result[i] += [street_clustered[j][1]]

        # output_street_location
        output_street_location_df = pd.DataFrame(output_street_location)
        output_street_location_df.columns = [
            'Full Name', 'Longitude', 'Latitude', 'Street Length', 'Cluster']

        data = json.loads(output_street_location_df.to_json(orient="records"))
        repo.dropCollection('street_clustered_location')
        repo.createCollection('street_clustered_location')
        repo[writes[0]].insert_many(data)

        repo[writes[0]].metadata({'complete': True})
        print(repo[writes[0]].metadata())
        [record for record in repo[writes[0]].find()]

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        contributor = 'mmao95_dongyihe_weijiang_zhukk'
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        # The scripts are in <folder>#<filename> format.
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        # The event log.
        doc.add_namespace('log', 'http://datamechanics.io/log/')

        this_script = doc.agent('alg:' + contributor + '#street_clustered_location', {
                                prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        streetbook_filtered = doc.entity('dat:' + contributor + '#streetbook_filtered', {
            prov.model.PROV_LABEL: 'Streetbook filtered', prov.model.PROV_TYPE: 'ont:DataSet'})
        street_clustered_location = doc.activity(
            'log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(street_clustered_location, this_script)
        doc.usage(street_clustered_location, streetbook_filtered, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Computation': 'Filter'
                   }
                  )

        result = doc.entity('dat:' + contributor + '#street_clustered_location', {
            prov.model.PROV_LABEL: 'Street Clustered Location', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(result, this_script)
        doc.wasGeneratedBy(result, street_clustered_location, endTime)
        doc.wasDerivedFrom(result, streetbook_filtered,
                           street_clustered_location)

        repo.logout()

        return doc

# eof
