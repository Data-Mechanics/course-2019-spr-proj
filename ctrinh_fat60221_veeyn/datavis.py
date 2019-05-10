import urllib.request
import json

# make a JSON with Green Line polyline locations

resdict = {
    "type": "FeatureCollection", 
    "features": [],
    "crs": {"type": "EPSG", "properties": {"code": "4326"}},
    "bbox": [-71.2543334960938,42.2063484191895,-70.9897079467773,42.4386367797852]
    }

# print(resdict)

route = "http://datamechanics.io/data/ctrinh_fat60221_veeyn/subwaylines_p_odp.json"

response = urllib.request.urlopen(route).read().decode("utf-8")
r = json.loads(response)
reslist = []

# print(r)

for i in range(len(r["features"])):
    if r["features"][i]["properties"]["LINE"] == "GREEN":
        # print(r["features"][i]["properties"]["LINE"])
        resdict["features"].append(r["features"][i])


outfile = open("green.txt", "w")
print(json.dumps(resdict), file=outfile)

# print(resdict)

# now to make a JSON file with coffee shop locations

resdict = {
    "type": "MultiPoint", 
    "coordinates": []
    }

# print(resdict)

route = "http://datamechanics.io/data/min_dist.json"

response = urllib.request.urlopen(route).read().decode("utf-8")
r = json.loads(response)
reslist = resdict['coordinates']

for i in range(len(r['Green Line B'])):
    templist = []
    templist.append(r['Green Line B'][i]['coffee_shop']['coordinates']['longitude'])
    templist.append(r['Green Line B'][i]['coffee_shop']['coordinates']['latitude'])
    reslist.append(templist)

for i in range(len(r['Green Line C'])):
    templist = []
    templist.append(r['Green Line C'][i]['coffee_shop']['coordinates']['longitude'])
    templist.append(r['Green Line C'][i]['coffee_shop']['coordinates']['latitude'])
    reslist.append(templist)

for i in range(len(r['Green Line D'])):
    templist = []
    templist.append(r['Green Line D'][i]['coffee_shop']['coordinates']['longitude'])
    templist.append(r['Green Line D'][i]['coffee_shop']['coordinates']['latitude'])
    reslist.append(templist)

for i in range(len(r['Green Line E'])):
    templist = []
    templist.append(r['Green Line E'][i]['coffee_shop']['coordinates']['longitude'])
    templist.append(r['Green Line E'][i]['coffee_shop']['coordinates']['latitude'])
    reslist.append(templist)

# resdict['coordinates'].append(reslist)

# print(r)

# for i in range(len(r["features"])):
#     if r["features"][i]["properties"]["LINE"] == "GREEN":
#         # print(r["features"][i]["properties"]["LINE"])
#         resdict["features"].append(r["features"][i])


outfile = open("coffee.txt", "w")
print(json.dumps(resdict), file=outfile)

import dml

resdict = {
    "type": "MultiPoint", 
    "coordinates": []
    }

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ctrinh_fat60221_veeyn', 'ctrinh_fat60221_veeyn')

# kmeans = db.ctrinh_fat60221_veeyn.kMeans.find( { }, { _id: 0 } )

kmeans = list(repo['ctrinh_fat60221_veeyn.kMeans'].find())

reslist = resdict['coordinates']

# uber = list(repo['ctrinh_fat60221_veeyn.uber'].find({}))

for i in range(len(kmeans)):
    templist = []
    templist.append(kmeans[i]['latitude'])
    templist.append(kmeans[i]['longitude'])
    reslist.append(templist)
    # print(i)

outfile = open("adjusted.txt", "w")
print(json.dumps(resdict), file=outfile)