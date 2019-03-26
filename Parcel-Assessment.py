import urllib.request
import json


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

#print(Parcels[0])
#print(json.dumps(Parcels, sort_keys=True, indent = 2))
