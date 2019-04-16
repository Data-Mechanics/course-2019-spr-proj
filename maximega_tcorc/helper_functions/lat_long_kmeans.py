import numpy as np
import pandas as pd
import matplotlib.pyplot as matplot
import sklearn
from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import pairwise_distances_argmin_min
from sklearn.metrics.pairwise import euclidean_distances

from sklearn.cluster import KMeans

import json
from sklearn import preprocessing 

def createScatter(df):
    matplot.subplot(1,2,1)
    matplot.scatter(x=df['latitude'], y=df['longitude'], s=1)
    matplot.title('Lat vs Long')
    

def vectorizeAndCluster(data, labels):
    v = DictVectorizer(sparse=False)
    X = v.fit_transform(data)
    clusters = KMeans(n_clusters=5)
    pred = clusters.fit_predict(X)
    # printLabels(pred, labels)
    return [clusters, X]

def getLabels(cats, kmeans):
    asc_order_centroids = kmeans.cluster_centers_.argsort()
    order_centroids = asc_order_centroids[:,::-1]
    labels = []
    for i in range(len(order_centroids)):
        list_arr = order_centroids[i].tolist()
        list_arr.remove(15)
        list_arr.remove(16)
        labels.append(cats[list_arr[0]])
    return labels

def printLabels(pred_classes, labels):
    for cluster in range(5):
        print('cluster: ', cluster)
        print(labels[np.where(pred_classes == cluster)])
            
            
def formatIntoVectors(dataf):
    vector_arr = []
    labels = np.array([])
    for (idx, row) in dataf.iterrows():
        temp_obj = {}
        temp_obj['latitude'] = row['scaled_latitude']
        temp_obj['longitude'] = row['scaled_longitude']
        temp_obj['income'] = row['scaled_income']
        labels = np.append(labels, row['ntaname'])
        vector_arr.append(temp_obj)
        
    return vectorizeAndCluster(vector_arr, labels)
    
def scalePoints(df):
    min_max_scaler = preprocessing.MinMaxScaler()
    counts_minmax = min_max_scaler.fit_transform(df['x'].values.reshape(-1,1))
    counts_minmax = pd.DataFrame(counts_minmax)
    return counts_minmax

def scaleAllData(dataf):
    
    lat = pd.DataFrame(dataf['latitude'].values.tolist(), columns=['x'])
    lon = pd.DataFrame(dataf['longitude'].values.tolist(), columns=['x'])
    inc = pd.DataFrame(dataf['income'].values.tolist(), columns=['x'])

    lat_scaled = scalePoints(lat)
    long_scaled = scalePoints(lon)
    income_scaled = scalePoints(inc)
    
    dataf['scaled_latitude'] = lat_scaled[0].values.tolist()
    dataf['scaled_longitude'] = long_scaled[0].values.tolist()
    dataf['scaled_income'] = income_scaled[0].values.tolist()
    
    return dataf
    
def plotRealAndClustered(df, clusters):
    color_list = ['red', 'blue', 'green', 'yellow', 'orange', 'pink', 'lightblue']
    # for i in range(len(color_list)):
    #     print(labels[i] + " = " + color_list[i])
    colors = np.array(color_list)
    matplot.subplot(1,2,1)
    matplot.scatter(x=df['latitude'], y=df['longitude'], c=colors[clusters.labels_], s=10)
    matplot.title('Clusters')
    matplot.show()
    
def detectOutliers(df, labels, centroids, X):
    count = 0
    distances = []
    cluster_labels = []
    for (idx, row) in df.iterrows():
        dist = euclidean_distances([centroids[labels[count]]], [X[count]])[0][0]
        distances.append(dist)
        cluster_labels.append(labels[count])
        count += 1
    df['distances'] = distances
    df['cluster_labels'] = cluster_labels
    return df

def run_lat_long_kmeans(data):
    data = pd.DataFrame(data, columns=['ntaname', 'longitude', 'latitude', 'income'])

    data_scaled = scaleAllData(data) # scale lat and long points

    # print(data_scaled.tail())

    clusters = formatIntoVectors(data_scaled)[0]

    # plotRealAndClustered(data_scaled, clusters)

    # print(clusters.labels_)

    # X = formatIntoVectors(data_w_scaled, top_categories, cat_dict)[1]
    # labels = getLabels(top_categories, clusters)
    # plotRealAndClustered(data_w_scaled, clusters, labels)
    # return [data_w_scaled, clusters, X]