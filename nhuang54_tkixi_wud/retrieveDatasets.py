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
    writes = ['nhuang54_tkixi_wud.boston_collisions', 'nhuang54_tkixi_wud.boston_bikes', 'nhuang54_tkixi_wud.traffic_lights', 'nhuang54_tkixi_wud.boston_hubway, nhuang54_tkixi_wud.boston_streetlights']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nhuang54_tkixi_wud', 'nhuang54_tkixi_wud')

        


        # Dataset #1: Boston Bike Network System
        print("Inserting Dataset #1: Boston Bike Network System")
        url = 'http://datamechanics.io/data/tkixi/bike_network.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("nhuang54_tkixi_wud.boston_bikes")
        repo.createCollection("nhuang54_tkixi_wud.boston_bikes")

        repo['nhuang54_tkixi_wud.boston_bikes'].insert_many(r)
        print("Done Inserting Dataset #1: Boston Bike Network System")
        repo['nhuang54_tkixi_wud.boston_bikes'].metadata({'complete':True})
        print(repo['nhuang54_tkixi_wud.boston_bikes'].metadata())
        print()

        # Dataset #2: Boston Hubway Stations 
        print("Inserting Dataset #2: Boston Hubway Stations")
        url = 'http://datamechanics.io/data/tkixi/hubway.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("nhuang54_tkixi_wud.boston_hubway")
        repo.createCollection("nhuang54_tkixi_wud.boston_hubway")

        repo['nhuang54_tkixi_wud.boston_collisions'].insert_many(r)
        print("Done Inserting Dataset #2: Boston Hubway Stations")
        repo['nhuang54_tkixi_wud.boston_hubway'].metadata({'complete':True})
        print(repo['nhuang54_tkixi_wud.boston_hubway'].metadata())
        print()

        #Dataset #3: Boston Traffic Signals Locations
        print("Inserting Dataset #3: Boston Traffic Signals Locations")
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/eee77dc4ab3d479f83b2100542285727_12.csv'
        data = pd.read_csv(url)
        repo.dropCollection("nhuang54_tkixi_wud.traffic_lights")
        repo.createCollection("nhuang54_tkixi_wud.traffic_lights")

        repo['nhuang54_tkixi_wud.traffic_lights'].insert_many(data.to_dict('records'))
        print("Done Inserting Dataset #3: Boston Traffic Signals Locations")
        repo['nhuang54_tkixi_wud.traffic_lights'].metadata({'complete':True})
        print(repo['nhuang54_tkixi_wud.traffic_lights'].metadata())
        print()

        #Dataset #4: Boston Vision Zero Crash Records
        print("Inserting Dataset #4: Boston Collisions")
        # url = 'https://data.boston.gov/dataset/7b29c1b2-7ec2-4023-8292-c24f5d8f0905/resource/e4bfe397-6bfc-49c5-9367-c879fac7401d/download/crash_open_data.csv'
        url = 'http://datamechanics.io/data/tkixi/crash_open_data.csv'
        data = pd.read_csv(url)
        repo.dropCollection("nhuang54_tkixi_wud.boston_collisions")
        repo.createCollection("nhuang54_tkixi_wud.boston_collisions")

        repo['nhuang54_tkixi_wud.boston_collisions'].insert_many(data.to_dict('records'))
        print("Done Inserting Dataset #4: Boston Collisions")
        repo['nhuang54_tkixi_wud.boston_collisions'].metadata({'complete':True})
        print(repo['nhuang54_tkixi_wud.boston_collisions'].metadata())
        print()

        # Dataset #5: Boston Street Light Locations
        print("Inserting Dataset #5: Boston Street Light Locations")
        url = 'https://data.boston.gov/dataset/52b0fdad-4037-460c-9c92-290f5774ab2b/resource/c2fcc1e3-c38f-44ad-a0cf-e5ea2a6585b5/download/streetlight-locations.csv'
        data = pd.read_csv(url)
        repo.dropCollection("nhuang54_tkixi_wud.boston_streetlights")
        repo.createCollection("nhuang54_tkixi_wud.boston_streetlights")

        repo['nhuang54_tkixi_wud.boston_streetlights'].insert_many(data.to_dict('records'))
        print("Done Inserting Dataset #5: Boston Street Light Locations")
        repo['nhuang54_tkixi_wud.boston_streetlights'].metadata({'complete':True})
        print(repo['nhuang54_tkixi_wud.boston_streetlights'].metadata())
        print()
        

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

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nhuang54_tkixi_wud', 'nhuang54_tkixi_wud')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/') #boston data portal

        # Dataset #1: Boston Bike Network System
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


        # Dataset #2: Boston Hubway Stations 
        this_script = doc.agent('alg:tkixi#retrieveDatasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:boston-hubway-station', {'prov:label':'Hubway Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_hubwayStation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hubwayStation, this_script)
        doc.usage(get_hubwayStation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        hubwayStation = doc.entity('dat:nhuang54_tkixi_wud#hubwayStation', {prov.model.PROV_LABEL:'Hubway Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubwayStation, this_script)
        doc.wasGeneratedBy(hubwayStation, get_hubwayStation, endTime)
        doc.wasDerivedFrom(hubwayStation, resource, get_hubwayStation, get_hubwayStation, get_hubwayStation)

        # Dataset #5: Boston Street Light Locations
        this_script = doc.agent('alg:nhuang54_tkixi_wud#retrieveDatasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:boston-weather', {'prov:label':'Boston Weather', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bostonWeather = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bostonWeather, this_script)
        doc.usage(get_bostonWeather, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        bostonWeather = doc.entity('dat:nhuang54_tkixi_wud#bostonWeather', {prov.model.PROV_LABEL:'Boston Weather', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bostonWeather, this_script)
        doc.wasGeneratedBy(bostonWeather, get_bostonWeather, endTime)
        doc.wasDerivedFrom(bostonWeather, resource, get_bostonWeather, get_bostonWeather, get_bostonWeather)

        # Dataset #4: Boston Traffic Signals Locations
        this_script = doc.agent('alg:nhuang54_tkixi_wud#retrieveDatasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:boston-traffic-signal-locations', {'prov:label':'Traffic Signals Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_trafficSignals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_trafficSignals, this_script)
        doc.usage(get_trafficSignals, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        trafficSignals = doc.entity('dat:nhuang54_tkixi_wud#trafficSignals', {prov.model.PROV_LABEL:'Traffic Signals Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trafficSignals, this_script)
        doc.wasGeneratedBy(trafficSignals, get_trafficSignals, endTime)
        doc.wasDerivedFrom(trafficSignals, resource, get_trafficSignals, get_trafficSignals, get_trafficSignals)

        # Dataset #5: Boston Vision Zero Crash Records
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
retrieveDatasets.execute()
# doc = example.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


#eof