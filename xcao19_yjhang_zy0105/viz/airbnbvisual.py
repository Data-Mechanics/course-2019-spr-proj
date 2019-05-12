import os
import folium
import json
import pandas as pd
import numpy as np


class airbnbvisual():

    #path = "http://datamechanics.io/data/Jinghang_Yuan"
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

    folium.LayerControl().add_to(m)

    m.save(os.path.join('1.html'))
airbnbvisual
