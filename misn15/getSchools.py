import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getSchools(dml.Algorithm):
    contributor = 'misn15'
    reads = []
    writes = ['misn15.schools']

    @staticmethod
    def execute(trial = False):
        '''Retrieve school locations'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        url = 'https://opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("schools")
        repo.createCollection("schools")
        repo['misn15.schools'].insert_many(r['features'])

        url = 'https://opendata.arcgis.com/datasets/0046426a3e4340a6b025ad52b41be70a_1.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo['misn15.schools'].insert_many(r['features'])

        repo['misn15.schools'].metadata({'complete': True})
        print(repo['misn15.schools'].metadata())

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
        doc.add_namespace('bdp', 'https://opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:misn15#getSchools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:1d9509a8b2fd485d9ad471ba2fdb1f90_0', {'prov:label':'Boston Public Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource2 = doc.entity('bdp:0046426a3e4340a6b025ad52b41be70a_1',{'prov:label': 'Boston Non-Public Schools', prov.model.PROV_TYPE: 'ont:DataResource','ont:Extension': 'geojson'})

        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_schools, this_script)
        doc.usage(get_schools, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                   }
                  )
        doc.usage(get_schools, resource2, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )

        schools = doc.entity('dat:misn15#schools', {prov.model.PROV_LABEL:'Boston Schools Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource, get_schools, get_schools, get_schools)
        doc.wasDerivedFrom(schools, resource2, get_schools, get_schools, get_schools)
                  
        return doc

# getSchools.execute()
# doc = getSchools.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
