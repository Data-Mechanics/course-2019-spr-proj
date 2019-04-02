# from mpl_toolkits.basemap import Basemap
# import matplotlib.pyplot as plt
# import numpy as np

# # source activate base
# # conda install basemap
# # conda install -c conda-forge basemap-data-hires


# # input desired coordinates
# # my_coords = [38.9719980,-76.9219820]
# my_coords = [42.338156,-71.0547323]

# # How much to zoom from coordinates (in degrees)
# zoom_scale = 1

# # Setup the bounding box for the zoom and bounds of the map
# bbox = [my_coords[0]-zoom_scale,my_coords[0]+zoom_scale,\
#         my_coords[1]-zoom_scale,my_coords[1]+zoom_scale]

# plt.figure(figsize=(12,6))
# # Define the projection, scale, the corners of the map, and the resolution.
# m = Basemap(projection='merc',llcrnrlat=bbox[0],urcrnrlat=bbox[1],\
#             llcrnrlon=bbox[2],urcrnrlon=bbox[3],lat_ts=10,resolution='i')

# # Draw coastlines and fill continents and water with color
# m.drawcoastlines()
# # m.drawcounties()
# m.drawrivers()
# m.drawstates()
# m.fillcontinents(color='peru',lake_color='dodgerblue')

# # draw parallels, meridians, and color boundaries
# m.drawparallels(np.arange(bbox[0],bbox[1],(bbox[1]-bbox[0])/5),labels=[1,0,0,0])
# m.drawmeridians(np.arange(bbox[2],bbox[3],(bbox[3]-bbox[2])/5),labels=[0,0,0,1],rotation=45)
# m.drawmapboundary(fill_color='dodgerblue')

# # build and plot coordinates onto map
# x,y = m(my_coords[1],my_coords[0])
# m.plot(x,y,marker='D',color='r')
# plt.title("Geographic Point Test")
# plt.savefig('coordinate_test.png', format='png', dpi=500)
# plt.show()

# pip install gmplot
# pip install geopy
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
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm
from sklearn.cluster import KMeans

import folium
from folium import plugins
import pymongo
from geopy.distance import vincenty
import random

api = 'AIzaSyB9zXb3Rzh-IWfAacHfJgRCfoCm3E7WTgw'
gmap = gmplot.GoogleMapPlotter(42.3381569, -71.0547323, 15)
gmap.apikey = api
geolocator = Nominatim(user_agent='test_application')
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=4)
tqdm.pandas()



class optimization(dml.Algorithm):
    contributor = 'ekmak_gzhou_kaylaipp_shen99'
    reads = ['ekmak_gzhou_kaylaipp_shen99.accessing_data']
    writes = ['ekmak_gzhou_kaylaipp_shen99.optimization']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.now()

        #connect to database
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')

        accessing_data = repo.ekmak_gzhou_kaylaipp_shen99.accessing_data.find()
        lat_list = []
        long_list = []
        count = 0
        all_coords = []   #holds x,y lat long coords 
        coords_with_labels = []

        for info in accessing_data: 
            try: 
                coords = info['COORDS']
                all_coords.append(coords)
            except: 
                continue

        # k means 
        kmeans = KMeans(n_clusters=7, random_state=0).fit(all_coords)
        labels = kmeans.labels_
        cluster_centers = kmeans.cluster_centers_
        print(labels)
        print(cluster_centers)

        # visuzalize datapoints & clusters 
        # for c in all_coords: 
        #     lat = c[0]
        #     lon = c[1]
        #     lat_list.append(lat)
        #     long_list.append(lon)

        # intitialize map to south boston
        map_ = folium.Map(location = [42.3381569,-71.0547323], zoom_start =14)

        # plot all property points 
        for c in all_coords:
            folium.CircleMarker(
                location=c,
                radius=1,
                color='#3186cc',
                fill=True,
                fill_color='#3186cc'
            ).add_to(map_)

        # plot clusters from k - means 
        for clusters in cluster_centers: 
            folium.CircleMarker(
                location=clusters,
                radius=50,
                color='#FF00FF',
                fill=True,
                fill_color='#3186cc'
            ).add_to(map_)


        map_.save('./clustering.html')
        print('map drawn!')


        # gmap = gmplot.GoogleMapPlotter(42.3381569,-71.0547323, 13) 
  
        # gmap.scatter(lat_list, long_list, '# FF0000', 
        #                                 size = 5, marker = False) 

        # gmap.draw("C:\\Users\\kaylaippongi\\Desktop\\course-2019-spr-proj\\clusters.html") 
        # print('map drawn!')


        # Store information in db
        repo.logout()
        endTime = datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liwang_pyhsieh', 'liwang_pyhsieh')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:liwang_pyhsieh#kMeansForAccidents', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        crash_2015 = doc.entity('dat:liwang_pyhsieh#crash_2015', {prov.model.PROV_LABEL: '2015 Massachusetts Crash Report', prov.model.PROV_TYPE: 'ont:DataSet'})

        get_crash_2015 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crash_2015, this_script)

        doc.usage(get_crash_2015, crash_2015, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        get_crash_clusters_median = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        crash_clusters_median = doc.entity('dat:liwang_pyhsieh#crash_cluster_medians',
                                     {prov.model.PROV_LABEL: 'Clustered medians for crash events', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash_clusters_median, this_script)
        doc.wasGeneratedBy(crash_clusters_median, get_crash_clusters_median, endTime)
        doc.wasDerivedFrom(crash_clusters_median, crash_2015, get_crash_clusters_median, get_crash_clusters_median, get_crash_clusters_median)

        get_crash_clusters_distribution = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        crash_clusters_distribution = doc.entity('dat:liwang_pyhsieh#crash_clusters_distribution',
                                     {prov.model.PROV_LABEL: 'Crash accident locations clustering result', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crash_clusters_distribution, this_script)
        doc.wasGeneratedBy(crash_clusters_distribution, get_crash_clusters_distribution, endTime)
        doc.wasDerivedFrom(crash_clusters_distribution, crash_2015, get_crash_clusters_distribution, get_crash_clusters_distribution, get_crash_clusters_distribution)

        repo.logout()

        return doc

if __name__ == "__main__":
    optimization.execute()
    # doc = optimization.provenance()

