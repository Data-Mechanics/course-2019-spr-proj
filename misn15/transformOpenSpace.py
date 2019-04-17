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
            open_space = open_space[0:20]
        else:
            open_space = open_space

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

        # for x in open_space_list:
        #     entry = {'coordinates': x}
        #     repo['misn15.openSpace_centroids'].insert_one(entry)

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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/misn15/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/misn15/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:transformOpenSpace', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:open_space', {'prov:label':'Boston Open Space Sites', prov.model.PROV_TYPE:'ont:DataResource'})

        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                        }
                  )
        resource2 = doc.entity('dat:openSpace_centroids', {prov.model.PROV_LABEL:'Centroids of Open Spaces', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(resource2, this_script)
        doc.wasGeneratedBy(resource2, this_run, endTime)
        doc.wasDerivedFrom(resource2, resource, this_run, this_run, this_run)

        return doc

#transformOpenSpace.execute(trial=True)
#doc = transformOpenSpace.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
