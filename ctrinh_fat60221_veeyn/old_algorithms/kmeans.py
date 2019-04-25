import urllib.request
import json
import datetime
import uuid
import csv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from gmplot import gmplot
from gmplot import GoogleMapPlotter

url = 'http://datamechanics.io/data/min_dist.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)

centroids = []

for x in range(len(r['Green Line B'])):
	centroids.append([float(r['Green Line B'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line B'][x]['coffee_shop']['coordinates']['latitude'])])

for x in range(len(r['Green Line C'])):
	centroids.append([float(r['Green Line C'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line C'][x]['coffee_shop']['coordinates']['latitude'])])

for x in range(len(r['Green Line D'])):
	centroids.append([float(r['Green Line D'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line D'][x]['coffee_shop']['coordinates']['latitude'])])

for x in range(len(r['Green Line E'])):
	centroids.append([float(r['Green Line E'][x]['coffee_shop']['coordinates']['longitude']),float(r['Green Line E'][x]['coffee_shop']['coordinates']['latitude'])])

centroids = np.array(centroids)


data = []
weighted_data = []

url2 = "http://datamechanics.io/data/final_mbta.csv"
r2 = pd.read_csv(url2)
mbta_stops = r2.to_dict('records')

def getWeight(arrivalCount):
	return round(arrivalCount/100)

for i in range(len(mbta_stops)):
	data.append([float(mbta_stops[i]['LATITUDE']), float(mbta_stops[i]['LONGITUDE'])])

for i in range(len(mbta_stops)):
	if float(int(mbta_stops[i]['ARRIVAL_COUNT']) > 100):
		w_value = getWeight(int(mbta_stops[i]['ARRIVAL_COUNT']))
		for x in range(w_value):
			weighted_data.append([float(mbta_stops[i]['LATITUDE']), float(mbta_stops[i]['LONGITUDE'])])
	else:
		weighted_data.append([float(mbta_stops[i]['LATITUDE']), float(mbta_stops[i]['LONGITUDE'])])


weighted_data=np.array(weighted_data)
data=np.array(data)


kmeans = KMeans(n_clusters=len(centroids), init=centroids)
kmeans.fit(weighted_data)

with open('kmeans.csv','w') as writeFile:
	writer = csv.writer(writeFile)
	writer.writerows(kmeans.cluster_centers_)
writeFile.close()


gmap = gmplot.GoogleMapPlotter(42.361145, -71.057083, 13, apikey='AIzaSyDEIbWpVHRxlCa3A-vuT8Pv2Ga0v0Np6XI')

gmap.scatter(data[:,1], data[:,0],'#FF0000')
gmap.scatter(centroids[:,1], centroids[:,0], '#00FF00')
gmap.scatter(kmeans.cluster_centers_[:,1], kmeans.cluster_centers_[:,0],'#0000FF')

gmap.draw("/Users/veeynguyen/Desktop/map.html")




