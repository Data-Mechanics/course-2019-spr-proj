import urllib.request
import json

resdict = {}

route = "https://api-v3.mbta.com/stops?filter[route]=Green-B"
response = urllib.request.urlopen(route).read().decode("utf-8")
r = json.loads(response)
reslist = []

for i in range(len(r["data"])):
    rindict = {}
    name = r["data"][i]["attributes"]["name"]
    sid = r["data"][i]["id"]
    latitude = r["data"][i]["attributes"]["latitude"]
    longitude = r["data"][i]["attributes"]["longitude"]
    rindict["name"] = name
    rindict["stop_id"] = sid
    rindict["latitude"] = latitude
    rindict["longitude"] = longitude
    reslist.append(rindict)

resdict["Green Line B"] = reslist

route = "https://api-v3.mbta.com/stops?filter[route]=Green-C"
response = urllib.request.urlopen(route).read().decode("utf-8")
r = json.loads(response)
reslist = []

for i in range(len(r["data"])):
    rindict = {}
    name = r["data"][i]["attributes"]["name"]
    sid = r["data"][i]["id"]
    latitude = r["data"][i]["attributes"]["latitude"]
    longitude = r["data"][i]["attributes"]["longitude"]
    rindict["name"] = name
    rindict["stop_id"] = sid
    rindict["latitude"] = latitude
    rindict["longitude"] = longitude
    reslist.append(rindict)

resdict["Green Line C"] = reslist

route = "https://api-v3.mbta.com/stops?filter[route]=Green-D"
response = urllib.request.urlopen(route).read().decode("utf-8")
r = json.loads(response)
reslist = []

for i in range(len(r["data"])):
    rindict = {}
    name = r["data"][i]["attributes"]["name"]
    sid = r["data"][i]["id"]
    latitude = r["data"][i]["attributes"]["latitude"]
    longitude = r["data"][i]["attributes"]["longitude"]
    rindict["name"] = name
    rindict["stop_id"] = sid
    rindict["latitude"] = latitude
    rindict["longitude"] = longitude
    reslist.append(rindict)

resdict["Green Line D"] = reslist

route = "https://api-v3.mbta.com/stops?filter[route]=Green-E"
response = urllib.request.urlopen(route).read().decode("utf-8")
r = json.loads(response)
reslist = []

for i in range(len(r["data"])):
    rindict = {}
    name = r["data"][i]["attributes"]["name"]
    sid = r["data"][i]["id"]
    latitude = r["data"][i]["attributes"]["latitude"]
    longitude = r["data"][i]["attributes"]["longitude"]
    rindict["name"] = name
    rindict["stop_id"] = sid
    rindict["latitude"] = latitude
    rindict["longitude"] = longitude
    reslist.append(rindict)

resdict["Green Line E"] = reslist

print(resdict["Green Line B"])
print()
print(resdict["Green Line C"])
print()
print(resdict["Green Line D"])
print()
print(resdict["Green Line E"])

outfile = open("mbtacoords.txt", "w")
print(json.dumps(resdict), file=outfile)