import folium
import numpy as np
from folium.plugins import HeatMap
import os
import pandas as pd
import branca
from random import randint

county_data = os.path.join('', 'mappingQuestion.csv')
county_geo = os.path.join('', 'countyGeo.json')

df = pd.read_csv(county_data, na_values=[' '])

colorscale = branca.colormap.linear.YlOrRd_09.scale(0, 1)
interested_data = df.set_index('County')

def style_function(feature):
	key = feature['properties']['NAME']
	data_y = interested_data.loc[key, 'Yes']
	data_n = interested_data.loc[key, 'No']
	data_b = interested_data.loc[key, 'Blanks']
	data = float(data_y) / float(data_y + data_n)
	return {
		'fillOpacity' : 0.5,
		'weight' : 0,
		'fillColor' : '#black' if data is None else colorscale(data)
	}

m = folium.Map(location=[42.407211, -71.382439], tiles='cartodbpositron', zoom_start=8)

folium.TopoJson(
	open(county_geo),
	'objects.cb_2015_massachusetts_county_20m',
	style_function=style_function
).add_to(m)


m.save('./templates/question.html')


