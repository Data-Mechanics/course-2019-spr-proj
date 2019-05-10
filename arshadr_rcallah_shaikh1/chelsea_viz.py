#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import json
import requests
import folium


# In[2]:


df_asses = pd.read_excel("data/Chelsea Assessor's Database - Fiscal Year 2018.xlsx")
df1 = df_asses[['Street Number', 'Street Name', 'Land Area in Acres', 'Total Appraised Parcel Value', 'Price', 'Gross Area', 'Living Area']]
df1 = df1.dropna(subset=['Street Number', 'Street Name'])
df1['Lat'] = 0
df1['Lon'] = 0
df1['Name'] = ''
for index, row in df1.iterrows():
    num = row['Street Number']
    st = row['Street Name']
    add = str(num) + ' ' + st + ' CHELSEA MA'
    df1.at[index, 'Name'] = add
df1 = df1[df1['Price'] > 1000]
df1 = df1.reset_index(drop=True)

df1['API_AD'] = df1['Name'].apply(lambda x: str(x).replace(" ", "+"))

def getLatitude(address):
    try:
        key = "AIzaSyDHvlHUQemkaVj1VSeuEL8UrjrMCF3zthQ"
        url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + address + "&key=" + key
        response = requests.get(url)
        if address.lower() == "nan":
            return "N/A"
        else:
            d = json.loads(response.text)['results'][0]
            if "partial_match" in d.keys():
                return "Partial Match"
            else:
                coord = d['geometry']['location']
                return (coord['lat'], coord['lng'])
    except Exception:
        print(address)

df1['coord'] = df1['API_AD'].apply(lambda x: getLatitude(x))


# In[2]:


df1.to_csv('assessor_coords.csv')


# In[2]:


df3 = pd.read_csv('assessor_coords.csv')
#print(df3.head())
df3 = df3[df3['coord'] != 'Partial Match']

str_coords = df3.coord.str[1:-1].str.replace("'", '').str.split(',')
df3['Lat'] = str_coords.str[0].astype('float')
df3['Lon'] = str_coords.str[1].astype('float')
print(df3.head())


# In[6]:


df3['Differential'] = (df3['Price'] - df3['Total Appraised Parcel Value']) / df3['Total Appraised Parcel Value']

def difcolors(counter):
    if counter['Differential'] < -2: 
        return 'red'
    elif counter['Differential'] >= -2 and counter['Differential'] < -1:
        return 'orange'
    elif counter['Differential'] >= -1 and counter['Differential'] < 0:
        return 'yellow'
    elif counter['Differential'] >= 0 and counter['Differential'] < 1:
        return 'lightblue'
    elif counter['Differential'] >= 1 and counter['Differential'] < 2:
        return 'blue'
    else:
        return 'green'
    
df3['Color'] = df3.apply(difcolors, axis=1)
df3['Color'] = df3['Color'].values.tolist()
chelsea_map = folium.Map(location=[42.3899, -71.0338], tiles='cartodbpositron',
                    zoom_start = 13) 


#heat_data2 = df2[['Lat', 'Lon', 'Differential']].values.tolist()
#chelsea_map.add_child(HeatMap(heat_data2, radius = 5, gradient={.4: 'blue', .65: 'lime', 1: 'red'}))
#HeatMap(heat_data2).add_to(chelsea_map)

for i in range(0, 2900):
    folium.CircleMarker(
    location=[df3['Lat'].iloc[i], df3['Lon'].iloc[i]],
    radius = 5,
    color = df3['Color'].iloc[i],
    fill = True,
    fill_color = df3['Color'].iloc[i]
    ).add_to(chelsea_map)


#chelsea_map
# Visualization of the pricing differentials


# In[7]:


df3 = pd.read_csv('assessor_coords.csv')
df3 = df3[df3['coord'] != 'Partial Match']
str_coords = df3.coord.str[1:-1].str.replace("'", '').str.split(',')
df3['Lat'] = str_coords.str[0].astype('float')
df3['Lon'] = str_coords.str[1].astype('float')


dfsqft = df3[['Name', 'Lat', 'Lon', 'Price', 'Living Area']]
dfsqft['$/sqft'] = dfsqft['Price'] / dfsqft['Living Area']
data = dfsqft[['Lat', 'Lon', '$/sqft']]

m = folium.Map(location=[42.3899, -71.0338], tiles='cartodbpositron', zoom_start=13)
for i in range(0, 2900):
    col = 'green' if data['$/sqft'].iloc[i] > 300 else ('blue' if data['$/sqft'].iloc[i] > 100 else 'red')
    folium.CircleMarker(
    location=[data.iloc[i]['Lat'], data.iloc[i]['Lon']],
    radius=3,
    color=col,
    fill=True,
    fill_color=col,
    ).add_to(m)

#m
# Visualization of the pricing by sqft 


# In[8]:


print(data['$/sqft'])


# In[18]:


df4 = pd.read_excel('data/fy19_chelsea_property_data.xlsx')

df4['Lat'] = 0
df4['Lon'] = 0
df4['Name'] = ''
for index, row in df4.iterrows():
    num = row['ST #']
    st = row['STREET']
    add = str(num) + ' ' + st + ' CHELSEA MA'
    df4.at[index, 'Name'] = add
    
df4 = df4[['Name', 'GRADE']]
print(df4.head())

df4['API_AD'] = df4['Name'].apply(lambda x: str(x).replace(" ", "+"))


# In[19]:


def getLatitude(address):
    try:
        key = "AIzaSyDHvlHUQemkaVj1VSeuEL8UrjrMCF3zthQ"
        url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + address + "&key=" + key
        response = requests.get(url)
        if address.lower() == "nan":
            return "N/A"
        else:
            d = json.loads(response.text)['results'][0]
            if "partial_match" in d.keys():
                return "Partial Match"
            else:
                coord = d['geometry']['location']
                return (coord['lat'], coord['lng'])
    except Exception:
        print(address)

df4['coord'] = df4['API_AD'].apply(lambda x: getLatitude(x))


# In[20]:


print(df4.head())


# In[98]:


df4.to_csv('data/2019data.csv')
df4 = df4[df4['coord'] != 'Partial Match']
df4['Lat'] = str_coords.str[0].astype('float')
df4['Lon'] = str_coords.str[1].astype('float')

def gradecolors(counter):
    if counter['GRADE'] == 'Minimum':
        return 'red'
    if counter['GRADE'] == 'Below Average':
        return 'darkred'
    if counter['GRADE'] == 'Average' or counter['GRADE'] == 'Average +10' or counter['GRADE'] == 'Average +20':
        return 'orange'
    if counter['GRADE'] == 'Good' or counter['GRADE'] == 'Good +10' or counter['GRADE'] == 'Good +20':
        return 'lightgreen'
    else:
        return 'green'
    
df4['Color'] = df4.apply(gradecolors, axis=1)
df4['Color'] = df4['Color'].values.tolist()

chelsea_map2 = folium.Map(location=[42.3899, -71.0338], tiles='cartodbpositron',
                    zoom_start = 13)
df4 = df4.dropna(subset=['Lat', 'Lon'])
for i in range(0, 2700):
    folium.CircleMarker(
    location=[df4['Lat'].iloc[i], df4['Lon'].iloc[i]],
    radius = 1,
    color = df4['Color'].iloc[i],
    fill = True,
    fill_color = df4['Color'].iloc[i]
    ).add_to(chelsea_map2)


#chelsea_map2
# Visualization of the housing grade distribution


# In[96]:


dfyr = pd.read_csv('data/City_Zhvi_SingleFamilyResidence.csv', encoding='latin-1')
import matplotlib.pyplot as plt

dfyr2 = dfyr[(dfyr['RegionName'] =='Chelsea') & (dfyr['State'] == 'MA')]
dfyr2 = dfyr2.drop(dfyr2.columns[0:6], axis=1)

dfyrbos = dfyr[(dfyr['RegionName'] =='Boston') & (dfyr['State'] == 'MA')]
dfyrbos = dfyrbos.drop(dfyrbos.columns[0:6], axis=1)

years = dfyr2.columns
dfyr2 = dfyr2.transpose()
dfyrbos = dfyrbos.transpose()

plt.plot(years, dfyr2, 'r')
plt.plot(years, dfyrbos, 'b')
#plt.show()
# Visualization of the average pricing by month from 04/1994 to 01/2019 between Boston and Chelsea


# In[ ]:




