from gmplot import gmplot
from sklearn.cluster import KMeans
import json
import dml
import uuid
import prov.model
from datetime import datetime
from pyproj import Proj, transform
from numpy.random import uniform
from numpy import array
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import math

import folium
from folium import plugins
import pymongo
from geopy.distance import vincenty
import random
import matplotlib.pyplot as plt


class k_means_price(dml.Algorithm):
    contributor = 'ekmak_gzhou_kaylaipp_shen99'
    reads = ['ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data']
    writes = ['ekmak_gzhou_kaylaipp_shen99.price_clusters']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()

        #connect to database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')

        # read in zillow search result data 
        zillow_data = repo.ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data.find()
        lat_list = []
        long_list = []
        count = 0
        all_coords = []   #holds x,y lat long coords 
        coords_with_labels = []
        results = []   #in format (zpid, valuation, x, y)
        price = []

        # get coordinates from zillow data 
        for info in zillow_data: 
            try: 
                zpid = info['zpid']
                lon = float(info['full_address']['latitude'])
                lat = float(info['full_address']['longitude'])
                valuation = int(info['zestimate']['amount'])

                # ensure values are not NaN 
                if valuation and lon and lat: 
                    price.append([valuation, lat, lon])
                    results.append((zpid, valuation, lat, lon))
            except: 
                continue
        
        # convert price list to np array and reshape 
        price = array(price)


        # --------------------------------------
        # use elbow technique to find and plot optimal k 
        # see visual result in folder 
        # --------------------------------------
        # Sum_of_squared_distances = []
        # K = range(1,10)
        # for k in K:
        #     km = KMeans(n_clusters=k)
        #     km = km.fit(price)
        #     Sum_of_squared_distances.append(km.inertia_)

        # plt.plot(K, Sum_of_squared_distances, 'bx-')
        # plt.xlabel('k')
        # plt.ylabel('Sum of Squared Distances')
        # plt.title('Elbow Method For Determining Optimal k in kmeans price clustering')
        # plt.show()


        # k means 
        kmeans = KMeans(n_clusters=3, random_state=0).fit(price)
        labels = kmeans.labels_
        cluster_centers = kmeans.cluster_centers_
        print(labels)
        print(cluster_centers)

        # intitialize map to south boston
        map_ = folium.Map(location = [42.3381569,-71.0547323], zoom_start =14)

        # initialize colors for clusters 
        colors = {0: '#8CDBFC', 1: '#BCF777', 2: '#D158B9', 3: '#FF69B4', 
        4: '#4169E1', 5: '#00FFFF', 6: '#00CED1'}


        '''
        loop over clusters and calculate Euclidian distance of 
        each point within that cluster from its centroid and 
        pick the maximum which is the radius of that cluster
        '''

        # holds all distances from points cluster 
        distances_from_cluster = {0:[], 1:[], 2:[]}

        # holds index of furthest point in cluster
        cluster_furthest_point = {0:0, 1:0, 2:0}

        # cluster radius 
        cluster_radi = {0:0, 1:0, 2:0}

        # cluster prices 
        cluster_prices = {0:[], 1:[], 2:[]}
        

        for idx, info in enumerate(price): 
            assigned_cluster = labels[idx]
            dist = euclidean(info, cluster_centers[assigned_cluster])
            distances_from_cluster[assigned_cluster].append(dist)
            valuation = info[0]
            cluster_prices[assigned_cluster].append(valuation)

        print('cluster prices:')
        print(cluster_prices)

        # average cluster prices for each cluster 
        for cluster, valuations in cluster_prices.items(): 
            cluster_prices[cluster] = sum(valuations)/len(valuations)

        print('avg cluster prices: ')
        print(cluster_prices)

        # get max distance from each cluster, which would be radius for that cluster
        for cluster,distances in distances_from_cluster.items(): 
            max_dist = max(distances)
            max_dist_idx = np.argmax(distances)
            cluster_furthest_point[cluster] = max_dist_idx

        print('cluster radi')
        print(cluster_furthest_point)      

        # calculate distance from furthest point & cluster center
        for cluster_num, idx_point in cluster_furthest_point.items(): 
            print(price[idx_point][1:])
            print(cluster_centers[cluster_num][1:])
            dist = euclidean(price[idx_point][1:], cluster_centers[cluster_num][1:])
            cluster_radi[cluster_num] = dist*23000

        print('cluster size')
        print(cluster_radi)      
        
        # plot all property points based on their cluster numbers 
        for idx,label in enumerate(labels):
            info = price[idx]
            lat = float(info[1])
            lon = float(info[2])
            folium.CircleMarker(
                location=[lon,lat],
                radius=0.3,
                color=colors[label],
                fill=True,
                fill_color=colors[label]
            ).add_to(map_)

        cluster_captions = ['Cluster 1: $1,952,818.29', 'Cluster 2: 625,496.54', 'Cluster 3: 1,022,676.34']
        folium_colors = ['lightblue', 'lightgreen', 'purple']
        # plot clusters from k - means 
        for idx,clusters in enumerate(cluster_centers): 
            
            price = clusters[0]
            lat = float(clusters[1])
            lon = float(clusters[2])
            # print('lat, long: ', lon, lat)
            # folium.CircleMarker(
            #     location=[lon,lat],
            #     radius=cluster_radi[idx],
            #     color=colors[idx],
            #     fill=True,
            #     fill_color=colors[idx]
            # ).add_to(map_)
            folium.Marker(
            location=[lon,lat],
            popup=cluster_captions[idx],
            icon=folium.Icon(color=folium_colors[idx], icon='info-sign')
        ).add_to(map_)

        map_.save('./k_means_clustering.html')
        print('done!! check k_means_clustering.html to see results')


        # Store information in db
        repo.logout()
        endTime = datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99', 'ekmak_gzhou_kaylaipp_shen99')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:ekmak_gzhou_kaylaipp_shen99#k_means_price', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        zillow_data = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#zillow_getsearchresults_data', {prov.model.PROV_LABEL: 'Zillow Search Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_zillow_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(zillow_data, this_script)

        doc.usage(get_zillow_data, zillow_data, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        get_cluster_medians = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        price_clusters = doc.entity('dat:ekmak_gzhou_kaylaipp_shen99#price_clusters',
                                     {prov.model.PROV_LABEL: 'Cluster centers for price valuations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(price_clusters, this_script)
        doc.wasGeneratedBy(price_clusters, get_cluster_medians, endTime)
        doc.wasDerivedFrom(price_clusters, zillow_data, get_cluster_medians, get_cluster_medians, get_cluster_medians)

        repo.logout()

        return doc

def euclidean(v1, v2):
    return sum((p-q)**2 for p, q in zip(v1, v2)) ** .5

if __name__ == "__main__":
    k_means_price.execute()
    # doc = optimization.provenance()

