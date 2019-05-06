#!/usr/bin/env python
# coding: utf-8

# In[126]:


import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns; sns.set()  # for plot styling
import numpy as np
import json
import os
import folium


# In[74]:

class final():
    data = pd.read_csv('http://datamechanics.io/data/Jinghang_Yuan/listings.csv')


    # In[79]:


    priceLoc = data[['price','latitude','longitude']]
    for i in range(len(priceLoc)):
        priceLoc.loc[i:i,'price'] = int((priceLoc.loc[i:i,'price'][i][1:-3]).replace(',', ''))
        #print(priceLoc.loc[i:i,'price'][i])



    long = priceLoc['longitude']
    lat = priceLoc['latitude']


    # In[84]:


    from sklearn.cluster import KMeans
    X = np.array(list(zip(long,lat)))
    colors = np.random.rand(50)
    plt.figure(figsize=(20,20))
    plt.scatter(X[:, 0],X[:, 1], alpha=0.3)
    plt.xlabel('longitude', fontsize=18)
    plt.ylabel('latitude', fontsize=16)
    plt.show()


    # In[85]:


    Sum_of_squared_distances = []
    K = range(5,30)
    for k in K:
        km = KMeans(n_clusters=k)
        km = km.fit(X)
        Sum_of_squared_distances.append(km.inertia_)
        
    plt.plot(K, Sum_of_squared_distances, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Sum_of_squared_distances')
    plt.title('Elbow Method For Optimal k')
    plt.show()


    # In[86]:


    kmeans = KMeans(n_clusters=25)  
    kmeans.fit(X) 


    from matplotlib.patches import Patch

    c0 = Patch(facecolor='b', edgecolor='b',
                             label='0')
    c1 = Patch(facecolor='c', edgecolor='c',
                             label='1')
    c2 = Patch(facecolor='y', edgecolor='y',
                             label='2')
    c3 = Patch(facecolor='m', edgecolor='m',
                             label='3')
    c4 = Patch(facecolor='r', edgecolor='r',
                             label='4')
    c5 = Patch(facecolor='g', edgecolor='g',
                             label='5')
    c6 = Patch(facecolor='k', edgecolor='k',
                             label='6')

    plt.figure(figsize=(10,10))
    labels = kmeans.labels_
    colors = {0:'b', 1:'c', 2:'y', 3:'m', 4:'r', 5:'g', 6:'k'}
    label_color = [colors[l%7] for l in labels]
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0, handles=[c0,c1,c2,c3,c4,c5,c6])
    plt.scatter(X[:,0],X[:,1], c=label_color)
    plt.title("K means clustering of crime data in Boston")
    plt.xlabel("longitude")
    plt.ylabel("latitude")
    plt.show()

    countary = np.zeros(25)
    sumary = np.zeros(25)
    for i in range(len(priceLoc)):
        sumary[labels[i]] += priceLoc.loc[i:i,'price'][i]
        countary[labels[i]] += 1
    res = np.zeros(25)
    for i in range(25):
        res[i] = sumary[i]/countary[i]



    # In[97]:


    joinResult = [[res[i],kmeans.cluster_centers_[i,0],kmeans.cluster_centers_[i,1]]for i in range(25)]


    # In[108]:


    url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/acedd06b39854088b0f2fefccffebf84_0.csv'
    df = pd.read_csv(url)
    json_df = df.to_json(orient='records')
    r = json.loads(json_df)


    # In[117]:


    def group(r):
        res = -1
        min_v = 999999
        for i in range(len(kmeans.cluster_centers_)):
            dis = (r['X']-kmeans.cluster_centers_[i][0])**2 + (r['Y']-kmeans.cluster_centers_[i][1])**2
            if(dis < min_v):
                min_v = dis
                res = i
        return res


    # In[119]:


    communityCenterCount = np.zeros(25)
    for i in r:
        communityCenterCount[group(i)]+=1


    # In[120]:


    url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/5575f763dbb64effa36acd67085ef3a8_0.csv'
    df = pd.read_csv(url)
    json_df = df.to_json(orient='records')
    r = json.loads(json_df)

    communityCenterPoolCount = np.zeros(25)
    for i in r:
        communityCenterPoolCount[group(i)]+=1


    # In[121]:


    url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0.csv'
    df = pd.read_csv(url)
    json_df = df.to_json(orient='records')
    r = json.loads(json_df)

    publicSchoolCount = np.zeros(25)
    for i in r:
        publicSchoolCount[group(i)]+=1


    # In[122]:



    url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6.csv'
    df = pd.read_csv(url)
    json_df = df.to_json(orient='records')
    r = json.loads(json_df)

    policeStationCount = np.zeros(25)
    for i in r:
        policeStationCount[group(i)]+=1


    # In[128]:


    neighbor_geo = "http://datamechanics.io/data/Jinghang_Yuan/bostonNeighbor.json"
    #neighbor_avgPrice = os.path.join(path, "airbnb_neighborhood_crime_rate.csv")
    data = pd.read_csv("http://datamechanics.io/data/Jinghang_Yuan/airbnb_neighborhood_crime_rate.csv")


    bins = data.loc[:,'price'].quantile([0, 0.25, 0.5, 0.75, 1])
    m = folium.Map(location=[42.3554130763512, -71.1323798419855], zoom_start=12)

    folium.Choropleth(
        geo_data=neighbor_geo,
        name='choropleth',
        data=data,
        columns=['neighborhood', 'price'],
        key_on='properties.Name',
        fill_color='YlGn',
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name='Average Price',
        bins = bins,
        reset = True
    ).add_to(m)

    for i in range(25):
        folium.Marker(
            location = [kmeans.cluster_centers_[i][1],kmeans.cluster_centers_[i][0]],
            popup=
            'Average Price:'+ str(res[i])+\
            '\nCommunity Center:'+ str(communityCenterCount[i])+\
            '\nCommunity Center Pool:'+ str(communityCenterPoolCount[i])+\
            '\nPublic School:'+ str(publicSchoolCount[i])+\
            '\nPolice Station:'+ str(policeStationCount[i]),
            icon=folium.Icon(icon='cloud')
        ).add_to(m)


    folium.LayerControl().add_to(m)

    m.save(os.path.join('final.html'))
final




