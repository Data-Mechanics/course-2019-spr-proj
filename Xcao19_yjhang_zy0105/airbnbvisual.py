import os
import folium
import json
import pandas as pd


class airbnbvisual():

    path = "C:/Users/Teddyzhang的PC/course-2019-spr-proj"

    neighbor_geo = os.path.join(path, 'neighbor_geod.json')
    #geo = pd.read_csv(neighbor_geo)

    neighbor_avgPrice = os.path.join(path, "neighbor_price.json")
    #
    print(neighbor_avgPrice)

    data_str = open(neighbor_avgPrice).read()
    #data = [[d['price'], d['longitude'],d['latitude']] for d in data_list]
    data_list = json.loads(data_str)
    df = pd.DataFrame(data_list)

    neighbor_avgPrice = df

    bins = list(neighbor_avgPrice['price'].quantile([0, 0.25, 0.5, 0.75, 1]))

    m = folium.Map(location=[42.3554130763512, -71.1323798419855], zoom_start=3)

    folium.Choropleth(
        geo_data=neighbor_geo,
        name='choropleth',
        data=neighbor_avgPrice,
        columns=['Neighbor', 'Average Price'],
        key_on='feature.id',
        fill_color='YlGn',
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name='Average Price(%)',
        bins = bins,
        reset = True
    ).add_to(m)

    folium.LayerControl().add_to(m)

    m.save(os.path.join('1.html'))