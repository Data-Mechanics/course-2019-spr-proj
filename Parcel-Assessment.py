import urllib.request
import json
from shapely.geometry import Polygon



#Property assessment data
All_Assessments = []
for i in range(9):
    print(i)
    skip = 20000*(i)
    url1 = "https://data.boston.gov/datastore/odata3.0/fd351943-c2c6-4630-992d-3f895360febd?$top=20000&$format=json&$skip=" + str(skip)
    response = urllib.request.urlopen(url1).read()
    Assessment = json.loads(response)
    Assessment = Assessment['value']
    All_Assessments += Assessment
print(len(All_Assessments))
dict_assessment = {}
count = 0
for assess in All_Assessments:
    print(count)
    count += 1
    dict_assessment[assess["PID"]] = {"AV_TOTAL":assess["AV_TOTAL"], "PTYPE":assess["PTYPE"]}
#Read parcel data
with open("dict_assessments.json","w") as f:
    f.write(json.dumps(dict_assessment))
with open("Parcels 2018.geojson") as file:
    P = file.read()

Parcels = json.loads(P)
Parcels = Parcels['features']
Parcels = [{'PID':x['properties']['PID_LONG'],'GEOMETRY':x['geometry']} for x in Parcels]
#print(json.dumps(Parcels, sort_keys=True, indent = 2))


#Combine to get (PID, GEOMETRY, AV_TOTAL, PTYPE
parcels_combined = []

for i in Parcels:
    try:
        parcels_combined.append({**i, **dict_assessment[i["PID"]]})
    except:
        pass
print(len(parcels_combined))
print(parcels_combined)
#print(json.dumps(Parcels, sort_keys=True, indent = 2))
