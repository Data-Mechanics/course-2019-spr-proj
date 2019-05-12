#!/usr/bin/env python
# coding: utf-8

import shapefile
import shapely.geometry as geometry
import math
import dml
import prov.model
import datetime
import uuid
import pandas as pd

sf = shapefile.Reader("http://datamechanics.io/data/Boston_Neighborhoods/Boston_Neighborhoods.shp")
shapes = sf.shapes()
records = sf.records()

#coordinate in the form of longitude, latitude
def get_neighborhood(coordinates):
    for i in range(0, len(shapes)):
        boundary = shapes[i]
    #     print(boundary.points[0])
        neighborhood = records[i][1]
        if geometry.Point(pt).within(geometry.shape(boundary)):
#             print("The point is in", neighborhood)
            return neighborhood


crimes_df = pd.read_csv('https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b/download/tmptlla4m8_.csv')
crimes_df = crimes_df[crimes_df['lat'].notnull()]

# NOTE: the following lines have been commented out because the execution takes approximately 1 hour. 
# The finalized data is hosted in datamechanics.io this script is no longer necessary in the context of other scripts which use its result.


# reverse geo-mapping of long/lat to neighborhood
# crimes_df['neighborhood'] = ""
# for i, row in crimes_df.iterrows():
#     long = crimes_df.at[i, 'long']
#     lat = crimes_df.at[i, 'lat']
#     pt = long, lat
#     neighborhood = get_neighborhood(pt)
#     print(i, long, lat, neighborhood)
#     crimes_df.at[i, 'neighborhood'] = neighborhood

# crimes_df = crimes_df[crimes_df['neighborhood'].notnull()]
# export_csv = crimes_df.to_csv ('crimes_with_neighborhood.csv', index = None, header=True)