#!/usr/bin/env python
# coding: utf-8

from bokeh.plotting import figure, show, output_notebook, output_file
from bokeh.models import BoxSelectTool, ColumnDataSource, HoverTool
from bokeh.tile_providers import CARTODBPOSITRON
from bokeh.plotting import figure
import pandas as pd
# import shapefile
# import shapely.geometry as geometry
import dml
import math
import shapefile
import shapely.geometry as geometry
import math
from ast import literal_eval

df = pd.read_csv('http://datamechanics.io/data/airbnb_neighborhood_crime_rate.csv')
# df['crime_rate'].corr(df['price'])


# convert long/lat to merc
def merc(Coords):
    #     Coordinates = literal_eval(Coords)
    lat = Coords[0]
    lon = Coords[1]

    r_major = 6378137.000
    x = r_major * math.radians(lon)
    scale = x/lon
    y = 180.0/math.pi * math.log(math.tan(math.pi/4.0 + 
    lat * (math.pi/180.0)/2.0)) * scale
    return (x, y)

for i, row in df.iterrows():
    long = df.at[i, 'longitude']
    lat = df.at[i, 'latitude']
    pair = (lat, long)
#     print(pair)
    x, y = merc(pair)
    df.at[i, 'X'] = x
    df.at[i, 'Y'] = y


#Define metric for circle size in bokeh visualization
df['price_vis'] = (df['price'] - df['price'].min()) / (df['price'].max() - df['price'].min())*30
df['crime_vis'] = (df['crime_rate'] - df['crime_rate'].min()) / (df['crime_rate'].max() - df['crime_rate'].min())*30

source = ColumnDataSource(data=dict(
        x=list(df['X']), 
        y=list(df['Y']),
        n = list(df['neighborhood']),
        avg_p=list(df['price']),
        crime_rate=list(df['crime_rate']),
        price_size = list(df['price_vis']),
        crime_size = list(df['crime_vis'])
        ))

hover = HoverTool(tooltips = [
    ("neighborhood", "@n"),
    ("average airbnb price", "@avg_p"),
    ("crimes since 2015", "@crime_rate")
])

p = figure(title="Crimes Reported vs Average Airbnb Prices",x_axis_type="mercator", y_axis_type="mercator", tools=[hover, "pan","wheel_zoom","reset"])
p.add_tile(CARTODBPOSITRON)

p.circle(x = 'x',
        y = 'y',
        source=source,
        size = 'crime_size',
        legend="crimes reported",
        line_color="#bc5016",
        fill_color="#ff7328",
        fill_alpha=0.5)

p.circle(x='x',
         y='y', 
         source=source,
         size='price_size',
         legend="average Airbnb price",
         line_color="#63cc2a", 
         fill_color="#6cff59",
         fill_alpha=0.5)

p.legend.location = "bottom_right"
output_file("viz.html")
show(p)



