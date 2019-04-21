import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import json

class retrieveDatasets(dml.Algorithm):
    contributor = 'nhuang54_tkixi_wud'
    reads = []
    writes = ['nhuang54_tkixi_wud.boston_collisions', 'nhuang54_tkixi_wud.boston_bikes', 'nhuang54_tkixi_wud.boston_streetlights']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nhuang54_tkixi_wud', 'nhuang54_tkixi_wud')


        # Dataset : Boston Bike Network System
        print("Inserting Dataset : Boston Bike Network System")
        url = 'http://datamechanics.io/data/tkixi/bike_network.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("nhuang54_tkixi_wud.boston_bikes")
        repo.createCollection("nhuang54_tkixi_wud.boston_bikes")

        repo['nhuang54_tkixi_wud.boston_bikes'].insert_many(r)
        print("Done Inserting Dataset : Boston Bike Network System")
        repo['nhuang54_tkixi_wud.boston_bikes'].metadata({'complete':True})
        print(repo['nhuang54_tkixi_wud.boston_bikes'].metadata())
        print()


        #Dataset : Boston Vision Zero Crash Records
        print("Inserting Dataset: Boston Collisions")
        # url = 'https://data.boston.gov/dataset/7b29c1b2-7ec2-4023-8292-c24f5d8f0905/resource/e4bfe397-6bfc-49c5-9367-c879fac7401d/download/crash_open_data.csv'
        url = 'http://datamechanics.io/data/tkixi/crash_open_data.csv'
        data = pd.read_csv(url)
        repo.dropCollection("nhuang54_tkixi_wud.boston_collisions")
        repo.createCollection("nhuang54_tkixi_wud.boston_collisions")

        repo['nhuang54_tkixi_wud.boston_collisions'].insert_many(data.to_dict('records'))
        print("Done Inserting Dataset : Boston Collisions")
        repo['nhuang54_tkixi_wud.boston_collisions'].metadata({'complete':True})
        print(repo['nhuang54_tkixi_wud.boston_collisions'].metadata())
        print()

        # Dataset : Boston Street Light Locations
        print("Inserting Dataset : Boston Street Light Locations")
        url = 'https://data.boston.gov/dataset/52b0fdad-4037-460c-9c92-290f5774ab2b/resource/c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5/download/streetlight-locations.csv'
        data = pd.read_csv(url)
        repo.dropCollection("nhuang54_tkixi_wud.boston_streetlights")
        repo.createCollection("nhuang54_tkixi_wud.boston_streetlights")

        repo['nhuang54_tkixi_wud.boston_streetlights'].insert_many(data.to_dict('records'))
        print("Done Inserting Dataset : Boston Street Light Locations")
        repo['nhuang54_tkixi_wud.boston_streetlights'].metadata({'complete':True})
        print(repo['nhuang54_tkixi_wud.boston_streetlights'].metadata())
        print()
        

        repo.logout()

        endTime = datetime.datetime.now()
        # print("done with data retrieval")

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nhuang54_tkixi_wud', 'nhuang54_tkixi_wud')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/') #boston data portal

        # Dataset : Boston Bike Network System
        this_script = doc.agent('alg:nhuang54_tkixi_wud#retrieveDatasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:boston-existing-bike-network', {'prov:label':'Existing Bike Network', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bikeNetwork = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bikeNetwork, this_script)
        doc.usage(get_bikeNetwork, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        bikeNetwork = doc.entity('dat:nhuang54_tkixi_wud#bikeNetwork', {prov.model.PROV_LABEL:'Existing Bike Network', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikeNetwork, this_script)
        doc.wasGeneratedBy(bikeNetwork, get_bikeNetwork, endTime)
        doc.wasDerivedFrom(bikeNetwork, resource, get_bikeNetwork, get_bikeNetwork, get_bikeNetwork)


        # Dataset : Boston Street Light Locations
        this_script = doc.agent('alg:nhuang54_tkixi_wud#retrieveDatasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:boston_streetlights', {'prov:label':'Boston Streetlights', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_streetlights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_streetlights, this_script)
        doc.usage(get_streetlights, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        streetLights = doc.entity('dat:nhuang54_tkixi_wud#streetLights', {prov.model.PROV_LABEL:'Boston Street Lights', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(streetLights, this_script)
        doc.wasGeneratedBy(streetLights, get_streetlights, endTime)
        doc.wasDerivedFrom(streetLights, resource, get_streetlights, get_streetlights, get_streetlights)

        # Dataset : Boston Vision Zero Crash Records
        this_script = doc.agent('alg:tkixi#retrieveDatasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:boston-vision-zero-crash-records', {'prov:label':'Vision Zero Crash Records', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_crashRecords = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crashRecords, this_script)
        doc.usage(get_crashRecords, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        crashRecords = doc.entity('dat:tkixi#crashRecords', {prov.model.PROV_LABEL:'Boston Collisions', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crashRecords, this_script)
        doc.wasGeneratedBy(crashRecords, get_crashRecords, endTime)
        doc.wasDerivedFrom(crashRecords, resource, get_crashRecords, get_crashRecords, get_crashRecords)


       

        repo.logout()
                  
        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
# retrieveDatasets.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


#eof