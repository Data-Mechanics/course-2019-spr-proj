import urllib.request
import json
from shapely.geometry import Polygon

# # parcel geojson data
# url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/b7739e6673104c048f5e2f28bb9b2281_0.geojson"
#
# response = urllib.request.urlopen(url).read()
#
# l = json.loads(response.decode('utf-8'))
#
#
# # property assessment data
# url1 = "https://data.boston.gov/datastore/odata3.0/695a8596-5458-442b-a017-7cd72471aade?$format=json"
# response = urllib.request.urlopen(url1).read()
# g = json.loads(response)
# g = g['value']

# census track data
url2 = "http://datamechanics.io/data/gasparde_ljmcgann_tlux/boston_census_track.json"
response = urllib.request.urlopen(url2).read()
d = json.loads(response)
print(d)
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

url = 'https://chronicdata.cdc.gov/resource/csmm-fdhi.geojson'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
print(s)





# client = dml.pymongo.MongoClient()
# repo = client.repo
# repo.authenticate('tlux', 'tlux')
# repo.dropCollection("test")
# repo.createCollection("test")
# repo['tlux.test'].insert_many(l['features'])
# repo['tlux.test'].metadata({'complete': True})