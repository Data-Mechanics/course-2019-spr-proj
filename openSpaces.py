import urllib.request
import json
from shapely.geometry import Point, Polygon
import pickle
from math import *
from tqdm import tqdm
from rtree import index


def haversine(point1, point2):
    lon1 = point1[1]
    lon2 = point2[1]
    lat1 = point1[0]
    lat2 = point2[0]

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1

    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(min(1,sqrt(a)))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles

    return c * r
def geo_distance(point, poly2):
    min_distance = 1000
    for other_point in list(poly2.exterior.coords):
        min_distance = min(min_distance, haversine((point.x,point.y), other_point))
    return min_distance

def score(neighborhood):
    score = 0
    for i in neighborhood:
        score += i["min_distance"]
    return score
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
largest_distance = 0
for neighborhood in list_of_neighborhoods:
    parcels_with_min[neighborhood['neighborhood']] =[]
for parcel in tqdm(parcels["Allston"]):
    min_distance = 100
    min_object = None
    space_name = None
    for open_space in open_spaces_by_neighborhoods["Allston"]:
        for shape in open_space["Shape"]:
            dis = parcel["Shape"][0].distance(shape)
            if (min_distance > dis):
                min_distance = dis
                min_object = shape
                space_name = open_space["OBJECTID"]
    min_distance_km = geo_distance(parcel["Shape"][0].centroid, min_object)
    largest_distance = max(largest_distance, min_distance)
    parcels_with_min["Allston"].append({**parcel, "min_distance" : min_distance,"nearest_open_space": space_name,"min_distance_km":min_distance_km})
print(parcels_with_min["Allston"])
score = score(parcels_with_min["Allston"])

index = index.Index()
for i in range(len(parcels["Allston"])):
    index.insert(i, parcels["Allston"][i]["Shape"][0].bounds)
def improvement_scores(input, index):
    parcels_with_improvement_scores = []
    for parcel in tqdm(input):
        score_improvement = 0
        new_park = parcel["Shape"][0].bounds
        for other_parcel in [j for j in index.nearest(new_park, 1000)]:
            shapely_distance = input[other_parcel]["Shape"][0].distance(parcel["Shape"][0])
            if shapely_distance < input[other_parcel]["min_distance"]:
                new_dist = geo_distance(input[other_parcel]["Shape"][0].centroid, parcel["Shape"][0])
                if (input[other_parcel]["min_distance_km"] - new_dist) > 0:
                    score_improvement += input[other_parcel]["min_distance_km"] - new_dist
            else:
                print("no improvement")
        parcels_with_improvement_scores.append({**parcel, "improvement":score_improvement})

    return parcels_with_improvement_scores

parcels_with_improvement_scores = improvement_scores(parcels_with_min["Allston"], index)

print(parcels_with_improvement_scores)
sum_improvement_scores = 0
for parcel in parcels_with_improvement_scores:
    sum_improvement_scores += parcel["improvement"]
print(sum_improvement_scores)