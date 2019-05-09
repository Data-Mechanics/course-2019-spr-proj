from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt
import geopy.distance
from pymongo import MongoClient
from collections import defaultdict
from os import path

def get_dist(point_1, point_2):
    return geopy.distance.vincenty(point_1, point_2).km

def get_avg_dist(kmeans, pickup_points):
    mean_mapping = kmeans.predict(pickup_points)

    dist_dict = defaultdict(list)

    for i, point in enumerate(pickup_points):
        closest_mean_idx = mean_mapping[i]
        closest_mean = kmeans.cluster_centers_[closest_mean_idx]
        point_dist = get_dist(closest_mean, point)
        dist_dict[closest_mean_idx].append(point_dist)
    
    dist_avg = np.array([])
    for mean in dist_dict:
        dist_list = dist_dict[mean]
        dist_avg = np.append(dist_avg, sum(dist_list)/len(dist_list))
    
    return dist_avg

def extract_data(dataset_label):
    # Set up the database connection.
        client = MongoClient()
        repo = client.repo
        repo.authenticate('mriver_osagga', 'mriver_osagga')

        data = list(repo['mriver_osagga.ny_uber_data_clean'].find())

        # Projection to create an in-memory list with the pickup points
        pickup_p = np.array([ ( float(val['Lat']), float(val['Lon'])) for val in data])

        return pickup_p

class kMeanCompute(object):
    def __init__(self, dataset_label='mriver_osagga.ny_uber_data_clean'):
        self.dataset = extract_data(dataset_label)
        return
    
    def compute_kmean(self, k_val):
        if (k_val == None):
            k_val = 5
        
        filename = './k_mean_figures/{}_k.png'.format(k_val)
        
        if path.isfile(filename):
            return filename
        
        kmeans = KMeans(n_clusters=k_val, random_state=0).fit(self.dataset)

        # Graphing the data
        y_predict = kmeans.predict(self.dataset)

        fig = plt.figure()
        plt.scatter(self.dataset[:, 0], self.dataset[:, 1], c=y_predict, s=50, cmap='viridis')
        fig.suptitle("Express Pickup points for Uber rides (using k={})".format(k_val), fontsize=20)
        centers = kmeans.cluster_centers_
        plt.scatter(centers[:, 0], centers[:, 1], c='black', s=75, alpha=0.7)
        plt.xlabel("Latitude", fontsize=18)
        plt.ylabel("Longitude", fontsize=18)
        
        filename = './k_mean_figures/{}_k.png'.format(k_val)
        plt.savefig(fname=filename, dpi=700, format='png', bbox_inches='tight')
        return filename