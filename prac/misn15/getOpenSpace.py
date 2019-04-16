import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getOpenSpace(dml.Algorithm):
    contributor = 'misn15'
    reads = []
    writes = ['misn15.open_spaces']

    @staticmethod
    def execute(trial = False):
        '''Retrieve open space data for city of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        url = 'https://opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        
        repo.dropCollection("open_space")
        repo.createCollection("open_space")
        repo['misn15.open_space'].insert_many(r['features'])
        repo['misn15.open_space'].metadata({'complete':True})
        print(repo['misn15.open_space'].metadata())

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
        doc.add_namespace('bdp', 'https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT * from "12cb3883-56f5-47de-afa5-3b1cf61b257b" WHERE CAST(year AS Integer) > 2016')

        this_script = doc.agent('alg:misn15#getCrime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:Boston_crime', {'prov:label':'Boston_crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query': '?sql=SELECT * from "12cb3883-56f5-47de-afa5-3b1cf61b257b" WHERE CAST(year AS Integer) > 2016'
                  }
                  )
        crime_data = doc.entity('dat:misn15#RetrieveCrime', {prov.model.PROV_LABEL:'Boston Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_data, this_script)
        doc.wasGeneratedBy(crime_data, get_crime, endTime)
        doc.wasDerivedFrom(crime_data, resource, get_crime, get_crime, get_crime)
                  
        return doc

getOpenSpace.execute()
doc = getOpenSpace.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
