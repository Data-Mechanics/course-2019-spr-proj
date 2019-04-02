import urllib.request
import json
from shapely.geometry import Polygon

# parcel geojson data
url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/b7739e6673104c048f5e2f28bb9b2281_0.geojson"

response = urllib.request.urlopen(url).read()

l = json.loads(response.decode('utf-8'))
parcels = []
l_features = l["features"]
for row in l_features:
    geom = row["geometry"]
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
    parcels.append({"PID":row["properties"]["PID_LONG"], "Shape":polys})
# print(parcels)
#
#
# # property assessment data
# url1 = "https://data.boston.gov/datastore/odata3.0/fd351943-c2c6-4630-992d-3f895360febd?$format=json"
# response = urllib.request.urlopen(url1).read()
# g = json.loads(response)
# g = g['value']

# census track data
url2 = "http://datamechanics.io/data/gasparde_ljmcgann_tlux/boston_census_track.json"
response = urllib.request.urlopen(url2).read()
d = json.loads(response)

shapes = []
p = []


for geom in d:
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

    shapes.append({"Shape":polys, "Census Tract":geom["Census Tract"]})
print(len(shapes))
url = 'https://chronicdata.cdc.gov/resource/47z2-4wuh.json?placename=Boston'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
boston_cdc = []
for data in r:
    for tract in shapes:

        if data["tractfips"] == tract["Census Tract"]:
            d = {"obesity":data["obesity_crudeprev"], "low_phys":data["lpa_crudeprev"],
                 "asthma":data["casthma_crudeprev"]}
            combined = {**tract, **d}

            boston_cdc.append(combined)
            break
combined = []

print(boston_cdc)

for parcel in parcels:
    #print(parcel["Shape"])
    for tract in boston_cdc:
        for shape in tract["Shape"]:
            if shape.contains(parcel["Shape"][0]):
                combined.append({**parcel, "Census Track":tract["Census Tract"]})
                break
#print(combined)
#print(len(combined))
#print(len(parcels))
# shapes_tracts = []
# boston_cdc_tracts = []
# for i in shapes:
#     shapes_tracts.append(i["Census Tract"])
# for i in boston_cdc:
#     boston_cdc_tracts.append(i["Census Tract"])
# for i in shapes_tracts:
#     if i not in boston_cdc_tracts:
#         print(i)


# client = dml.pymongo.MongoClient()
# repo = client.repo
# repo.authenticate('tlux', 'tlux')
# repo.dropCollection("test")
# repo.createCollection("test")
# repo['tlux.test'].insert_many(l['features'])
# repo['tlux.test'].metadata({'complete': True})