from gmplot import gmplot
from sklearn.cluster import KMeans
from sklearn.preprocessing import scale
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
import random

import folium
from folium import plugins
import pymongo
from geopy.distance import vincenty
import random
import matplotlib.pyplot as plt
from tqdm import tqdm


class k_means_price(dml.Algorithm):
    contributor = 'ekmak_gzhou_kaylaipp_shen99'
    reads = ['ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data']
    writes = ['ekmak_gzhou_kaylaipp_shen99.price_clusters']

    @staticmethod
    # def execute(trial=False):
    def execute(trial):
        
        startTime = datetime.now()

        #connect to database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')
        print('')
        print('inserting k means price data ...')

        # read in zillow search result data 
        zillow_data = repo.ekmak_gzhou_kaylaipp_shen99.zillow_getsearchresults_data.find()

        # if trial mode on, only take random subset of data
        if trial: 
            print('in trial mode!')
            print('inital zillow data count: ', zillow_data.count())
            ranges = random.sample(range(0, zillow_data.count()), 2)
            start = min(ranges)
            end = max(ranges)
            zillow_data = list(zillow_data)[start:end]
            print('after zillow data count: ', len(zillow_data))

        lat_list_orig = []
        long_list_orig = []
        count = 0
        all_coords = []   #holds x,y lat long coords 
        coords_with_labels = []
        results = []   #in format (zpid, valuation, x, y)
        price = []
        val_list_orig = []

        # get coordinates from zillow data 
        for info in zillow_data: 
            try: 
                zpid = info['zpid']
                lon = float(info['full_address']['latitude'])
                lat = float(info['full_address']['longitude'])
                valuation = int(info['zestimate']['amount'])

                # ensure values are not NaN 
                if valuation and lon and lat: 
                    lat_list_orig.append(lat)
                    long_list_orig.append(lon)
                    val_list_orig.append(valuation)
            except: 
                continue

        # convert to np arrays 
        lat_list_orig = np.array(lat_list_orig)
        long_list_orig = np.array(long_list_orig)
        val_list_orig = np.array(val_list_orig)
        val_list_orig = val_list_orig.astype(np.float)

        # scale longitdue and latitude coords 
        lat_mean = lat_list_orig.mean(axis=0)
        lat_std = lat_list_orig.std(axis=0)
        lon_mean = long_list_orig.mean(axis=0)
        lon_std = long_list_orig.std(axis=0)

        # scale longitude, latitude and price of property! 
        lat_list_scaled = scale(lat_list_orig)
        long_list_scaled = scale(long_list_orig)
        vals_scaled = scale(val_list_orig)

        # add scaled coordinates to price and results lists 
        for idx in range(len(val_list_orig)): 
            val = vals_scaled[idx]
            lat = lat_list_scaled[idx]
            lon = long_list_scaled[idx]
            price.append([val, lat, lon])

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
        # plt.title('Determining Optimal K in K-means price clustering')
        # plt.show()

        # k means 
        kmeans = KMeans(n_clusters=4, random_state=0, init='k-means++').fit(price)
        labels = kmeans.labels_
        cluster_centers = kmeans.cluster_centers_

        # intitialize map to south boston
        map_ = folium.Map(location = [42.3381569,-71.0547323], zoom_start =14)

        # initialize colors for clusters 
        colors = {0: '#8CDBFC', 1: '#BCF777', 2: '#D158B9', 3: '#FA9638'}


        '''
        loop over clusters and calculate Euclidian distance of 
        each point within that cluster from its centroid and 
        pick the maximum which is the radius of that cluster
        '''

        # holds all distances from points cluster 
        distances_from_cluster = {0:[], 1:[], 2:[], 3:[]}

        # holds index of furthest point in cluster
        cluster_furthest_point = {0:0, 1:0, 2:0, 3:0}

        # cluster radius 
        cluster_radi = {0:0, 1:0, 2:0, 3:0}

        # hold prices for all properties in each cluster
        cluster_prices = {0:[], 1:[], 2:[], 3:[]}
        

        for idx, info in enumerate(price): 
            assigned_cluster = labels[idx]
            dist = euclidean(info, cluster_centers[assigned_cluster])
            distances_from_cluster[assigned_cluster].append(dist)
            valuation = val_list_orig[idx]
            cluster_prices[assigned_cluster].append(valuation)

        # average cluster prices for each cluster 
        # for cluster, valuations in cluster_prices.items(): 
        #     cluster_prices[cluster] = sum(valuations)/len(valuations)

        # get max distance from each cluster, which would be radius for that cluster
        for cluster,distances in distances_from_cluster.items(): 
            max_dist = max(distances)
            max_dist_idx = np.argmax(distances)
            cluster_furthest_point[cluster] = max_dist_idx
  

        # calculate distance from furthest point & cluster center
        for cluster_num, idx_point in tqdm(cluster_furthest_point.items()): 
            dist = euclidean(price[idx_point][1:], cluster_centers[cluster_num][1:])
            cluster_radi[cluster_num] = dist*23000
  
        
        # plot all property points based on their cluster numbers 
        for idx,label in enumerate(labels):
            info = price[idx]
            lat = lat_list_orig[idx]
            lon = long_list_orig[idx]
            s = '''\
            Cluster: {cluster}
            Price: {price} \
            '''.format(cluster=label, price=val_list_orig[idx])

            folium.CircleMarker(
                location=[lon,lat],
                radius=0.3,
                color=colors[label],
                fill=True,
                fill_color=colors[label]
            ).add_child(folium.Popup(s)).add_to(map_)

        cluster_captions = ['Cluster 1: $1,952,818.29', 'Cluster 2: 625,496.54', 'Cluster 3: 1,022,676.34']
        folium_colors = ['lightblue', 'lightgreen', 'purple', 'orange']

        
        # calculate unscaled cluster centers from scaled long,lat point 
        # X_original = (X_scale * std_of_array) + mean_of_array
        unscaled_cluster_centers = []
        for c in cluster_centers:  
            lat_scaled = c[1]
            lon_scaled = c[2]
            lat_unscaled = (lat_scaled * lat_list_orig.std()) + lat_list_orig.mean()
            lon_unscaled = (lon_scaled * long_list_orig.std()) + long_list_orig.mean()
            unscaled_cluster_centers.append([lat_unscaled, lon_unscaled])

        # plot clusters centers from k - means 
        for idx,clusters in enumerate(cluster_centers): 
            price = clusters[0]
            lat = float(unscaled_cluster_centers[idx][0])
            lon = float(unscaled_cluster_centers[idx][1])
            folium.Marker(
            location=[lon,lat],
            icon=folium.Icon(color=folium_colors[idx], icon='info-sign')
        ).add_to(map_)

        # --------------------------------------------
        # save interative map to k_means_clustering_scaled.html 
        # --------------------------------------------
        # map_.save('./k_means_clustering_scaled.html')
        # print('Map generated: check k_means_clustering_scaled.html to see results')

        # clear database 
        repo.dropCollection("price_clusters")
        repo.createCollection("price_clusters")

        # store information in db 
        for key,val in cluster_prices.items(): 
            repo['ekmak_gzhou_kaylaipp_shen99.price_clusters'].insert_one({'cluster_num':key, 'prices':val})

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

        this_script = doc.agent('alg:ekmak_gzhou_kaylaipp_shen99#k_means_price2', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

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


k_means_price.execute(False)
k_means_price.provenance()
print('prov done! ')