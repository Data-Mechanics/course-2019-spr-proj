import pandas as pd
import numpy as np
import json
import requests
import folium

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


df1.to_csv('assessor_coords.csv')

df3 = pd.read_csv('assessor_coords.csv')
#print(df3.head())
df3 = df3[df3['coord'] != 'Partial Match']

str_coords = df3.coord.str[1:-1].str.replace("'", '').str.split(',')
df3['Lat'] = str_coords.str[0].astype('float')
df3['Lon'] = str_coords.str[1].astype('float')
print(df3.head())

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


chelsea_map
