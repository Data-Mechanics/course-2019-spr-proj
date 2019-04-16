import json
import dml
import prov.model
import datetime
import uuid
import urllib.request
import requests

class transformOpenSpace(dml.Algorithm):
    contributor = 'misn15'
    reads = ['misn15.open_space']
    writes = ['misn15.openSpace_centroids']

    @staticmethod
    def execute(trial = False):
        '''gets centroids and fips tracts for green spaces'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        open_space = list(repo['misn15.open_space'].find())

        # get coordinates for open space
        if trial:
            open_space = open_space[0:25]

        openSpace_coords = []
        for x in open_space:
            avg_lat = 0
            avg_long = 0
            count = 0
            for y in x['geometry']['coordinates'][0]:
                if type(y[0]) == list:
                    for j in y:
                        avg_long += j[0]
                        avg_lat += j[1]
                        count +=1
                else:
                    avg_long += float(y[0])
                    avg_lat += float(y[1])
                    count += 1
            openSpace_coords += [[x['properties']['SITE_NAME'], avg_long/count, avg_lat/count]]

            # get fips tract codes for open spaces
            for x in openSpace_coords:
                params = urllib.parse.urlencode({'latitude': x[2], 'longitude': x[1], 'format': 'json'})
                url = 'https://geo.fcc.gov/api/census/block/find?' + params
                response = requests.get(url)
                data = response.json()
                geoid = data['Block']['FIPS'][0:11]
                x += [geoid]

        repo.dropCollection("misn15.openSpace_centroids")
        repo.createCollection("misn15.openSpace_centroids")

        for x in openSpace_coords:
            entry = {'Name': x[0], 'Coordinates': (x[1], x[2]), 'FIPS': x[3]}
            repo['misn15.openSpace_centroids'].insert_one(entry)

        repo['misn15.openSpace_centroids'].metadata({'complete':True})
        print(repo['misn15.openSpace_centroids'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('waste', 'http://datamechanics.io/data/misn15/hwgenids.json') # The event log.
        doc.add_namespace('oil', 'http://datamechanics.io/data/misn15/oil_sites.geojson') # The event log.
        
        this_script = doc.agent('alg:misn15#transformWaste', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:waste', {'prov:label':'Boston Waste Sites', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('dat:oil', {'prov:label':'Boston Oil Sites', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
       
        get_merged = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_merged, this_script)
        doc.usage(get_merged, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                        }
                  )
        doc.usage(get_merged, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                        }
                  )
        oil_data = doc.entity('dat:misn15#oil', {prov.model.PROV_LABEL:'Waste Sites', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(oil_data, this_script)
        doc.wasGeneratedBy(oil_data, get_merged, endTime)
        doc.wasDerivedFrom(oil_data, resource, get_merged, get_merged, get_merged)

        waste_data = doc.entity('dat:misn15#waste', {prov.model.PROV_LABEL:'Waste Sites', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(waste_data, this_script)
        doc.wasGeneratedBy(waste_data, get_merged, endTime)
        doc.wasDerivedFrom(waste_data, resource2, get_merged, get_merged, get_merged)
                
        return doc

transformOpenSpace.execute(trial=True)
doc = transformOpenSpace.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
