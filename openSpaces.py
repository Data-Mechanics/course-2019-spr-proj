import urllib.request
import json
from shapely.geometry import Point, Polygon
import pickle
from tqdm import tqdm

# open spaces in boston
url = "http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson"
response = json.loads(urllib.request.urlopen(url).read())

wanted_types = ["Parkways, Reservations & Beaches", "Parks, Playgrounds & Athletic Fields",
                        "Urban Wilds & Natural Areas", "Community Gardens"]
response = response["features"]
open_spaces = [i for i in response if i['properties']['TypeLong'] in wanted_types]

#print(open_spaces)
# neighborhoods
url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
# print(r['features'])
neighborhoods = r['features']
list_of_neighborhoods = []
for neighborhood in neighborhoods:
    geom = neighborhood['geometry']
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

    list_of_neighborhoods.append({'neighborhood': neighborhood['properties']['Name'], "Shape": polys})
open_spaces_by_neighborhoods = {}
for neighborhood in list_of_neighborhoods:
    open_spaces_by_neighborhoods[neighborhood['neighborhood']] =[]
#print(open_spaces_by_neighborhoods)

open_spaces_shape = []
for open_space in open_spaces:
    geom = open_space['geometry']
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
    open_spaces_shape.append({"OBJECTID":open_space["properties"]["OBJECTID"],
                              "Shape": polys})
#print(open_spaces_shape)

for open_space in open_spaces_shape:
    for neighborhood in list_of_neighborhoods:
        found = False
        for open_space_shape in open_space["Shape"]:
            for shape in neighborhood["Shape"]:
                if open_space_shape.intersects(shape):
                    open_spaces_by_neighborhoods[neighborhood['neighborhood']].append(open_space)
                    found = True
                    break
            if found:
                break

with open("combined.pickle", "rb") as f:
    parcels = pickle.loads(f.read())

print(len(parcels["Allston"]))
print(len(open_spaces_by_neighborhoods["Allston"]))
parcels_with_min = {}
for neighborhood in list_of_neighborhoods:
    parcels_with_min[neighborhood['neighborhood']] =[]
for parcel in tqdm(parcels["Allston"]):
    min_distance = 100
    for open_space in open_spaces_by_neighborhoods["Allston"]:
        for shape in open_space["Shape"]:
            min_distance = min(min_distance, parcel["Shape"][0].distance(shape))
    parcels_with_min["Allston"].append({**parcel, "min_distance" : min_distance})
print(parcels_with_min["Allston"])