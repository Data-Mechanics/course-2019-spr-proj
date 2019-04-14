import urllib.request
import json
from shapely.geometry import Polygon



#Property assessment data
url1 = "https://data.boston.gov/datastore/odata3.0/fd351943-c2c6-4630-992d-3f895360febd?$format=json"
response = urllib.request.urlopen(url1).read()
Assessment = json.loads(response)
Assessment = Assessment['value']


#Read parcel data
with open("Parcels 2018.geojson") as file:
    P = file.read()

Parcels = json.loads(P)
Parcels = Parcels['features']
Parcels = [{'PID':x['properties']['PID_LONG'],'GEOMETRY':x['geometry']} for x in Parcels]
#print(json.dumps(Parcels, sort_keys=True, indent = 2))


#Combine to get (PID, GEOMETRY, AV_TOTAL, PTYPE
for i in Parcels:
    for j in Assessment:
        if j["PID"] == i["PID"]:
            i['AV_TOTAL'] = j["AV_TOTAL"]
            i['PTYPE'] = j["PTYPE"]
            break


    #if 'AV_TOTAL' not in i:
    #    Parcels.remove(i)

#print(Parcels[0])

"""
for parcel in Parcels:
    geom = parcel["GEOMETRY"]
    polys = []
    if geom['type'] == 'Polygon':
        shape = []
        coords = geom['coordinates']
        for point in coords[0]:
            shape.append((point[0], point[1]))
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

    parcel['SHAPE'] = polys
    parcel.pop('GEOMETRY', None)

print(json.dumps(Parcels, sort_keys=True, indent = 2))

"""
"""
##################################################
# census track data
url2 = "http://datamechanics.io/data/gasparde_ljmcgann_tlux/boston_census_track.json"
response = urllib.request.urlopen(url2).read()
d = json.loads(response)
#print(d)
shapes = []
p = []


for geom in d:
    polys = []
    if geom['type'] == 'Polygon':
        shape = []
        coords = geom['coordinates']
        for point in coords[0]:
            shape.append((point[0], point[1]))
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

    shapes.append({"Shape":polys, "Census Tract":geom["Census Tract"]})

print(shapes)


url = 'https://chronicdata.cdc.gov/resource/csmm-fdhi.geojson'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
#print(s)


"""
