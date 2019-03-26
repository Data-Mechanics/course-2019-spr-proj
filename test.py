import prov.model
import datetime
import uuid
import json
from shapely.geometry import Point, Polygon




with open('C:/Users/tylux/Downloads/cb_2016_25_tract_500k (2).json') as json_file:
    d = json.load(json_file)

shapes = []
p = []
datas = d['features']
for data in datas:
    if data['properties']['COUNTYFP'] == '025':
        geom = data['geometry']
        p.append(geom)
g = json.dumps(p)
with open('C:/Users/tylux/Downloads/boston_census_track.json', 'w') as f:
    f.write(g)

for data in datas:
    if data['properties']['COUNTYFP'] == '025':
        geom = data['geometry']
        polys = []
        if geom['type'] == 'Polygon':
            shape = []
            coords = geom['coordinates']
            for i in coords[0]:
                shape.append((i[0], i[1]))
            polys.append(Polygon(shape))
        if geom['type'] == 'MultiPolygon':
            coords = geom['coordinates']
            for i in coords:
                shape = []
                for j in i:
                    for k in j:
                        # need to change list type to tuple so that shapely can read it
                        shape.append((k[0], k[1]))
                poly = Polygon(shape)
                polys.append(poly)

        shapes.append({"Shape":polys, "Census Tract":data['properties']['GEOID']})

print(shapes)
