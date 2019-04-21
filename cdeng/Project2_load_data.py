## This python file is under cdeng file

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

import pandas as pd
import csv
import io
import requests


class Project2_load_data(dml.Algorithm):
    contributor = 'cdeng'
    # This is the first algorithm need to execute, it reads nothing and write all the data set. 
    reads = []
    writes = ['cdeng.stations_info', 'cdeng.bike_trip']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        # (username, password)
        repo.authenticate('cdeng', 'cdeng')

        ################################### Put data into MongoDB ###################################
        print('Algorithm 1: load data for project 2')
        # Loading station data
        print("(1). Load station data here...")
        url = 'https://s3.amazonaws.com/hubway-data/Hubway_Stations_as_of_July_2017.csv' 
        response = urllib.request.urlopen(url).read().decode("utf-8")
        content_df = pd.read_csv(io.StringIO(response))
        content_df_dict = content_df.to_dict(orient = 'records')

        repo.dropCollection("cdeng.stations_info")
        repo.createCollection("cdeng.stations_info")
        repo['cdeng.stations_info'].insert_many(content_df_dict)


        print("(2). Load 201801 bike trip data here...")
        url = 'http://datamechanics.io/data/201801_hubway_tripdata.csv' # This data is found by myself
        response = urllib.request.urlopen(url).read().decode("utf-8")
        content_df_trip0118 = pd.read_csv(io.StringIO(response))
        content_df_trip0118_dict = content_df_trip0118.to_dict(orient = 'records')

        
        print("(3). Load 201802 bike trip data here...")
        url = 'http://datamechanics.io/data/201802_hubway_tripdata.csv' # This data is found by myself
        response = urllib.request.urlopen(url).read().decode("utf-8")
        content_df_trip0218 = pd.read_csv(io.StringIO(response))
        content_df_trip0218_dict = content_df_trip0218.to_dict(orient = 'records')

        print("(4). Combine trip data here...")
        repo.dropCollection("cdeng.bike_trip")
        repo.createCollection("cdeng.bike_trip")
        repo["cdeng.bike_trip"].insert_many(content_df_trip0118_dict)
        repo["cdeng.bike_trip"].insert_many(content_df_trip0218_dict)
        ################################### Data Loading Finish ###################################
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
        repo.authenticate('cdeng', 'cdeng')

        print("Finish data provenance here...")

        ################################### Finish data provenance here
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') 
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') 
        doc.add_namespace('log', 'http://datamechanics.io/log/') 
        doc.add_namespace('bdp', 'http://datamechanics.io/data/')
        doc.add_namespace('bdp2', 'https://s3.amazonaws.com/hubway-data')

        this_script = doc.agent('alg:cdeng#Project2_load_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('bdp:201801_hubway_tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        resource2 = doc.entity('bdp:201802_hubway_tripdata', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        resource3 = doc.entity('bdp2:Hubway_Stations_as_of_July_2017', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

        get_stations_info = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bike_trip = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_stations_info, this_script)
        doc.wasAssociatedWith(get_bike_trip, this_script)

        doc.usage(get_bike_trip, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        doc.usage(get_bike_trip, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        doc.usage(get_stations_info, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        bike_trip = doc.entity('dat:cdeng#bike_trip', {prov.model.PROV_LABEL:'bike_trip', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bike_trip, this_script)
        doc.wasGeneratedBy(bike_trip, get_bike_trip, endTime)
        doc.wasDerivedFrom(bike_trip, resource1, get_bike_trip, get_bike_trip, get_bike_trip)
        doc.wasDerivedFrom(bike_trip, resource2, get_bike_trip, get_bike_trip, get_bike_trip)

        stations_info = doc.entity('dat:cdeng#stations_info', {prov.model.PROV_LABEL:'stations_info', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(stations_info, this_script)
        doc.wasGeneratedBy(stations_info, get_stations_info, endTime)
        doc.wasDerivedFrom(stations_info, resource3, get_stations_info, get_stations_info, get_stations_info)


        ###################################
        repo.logout()      
        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
if __name__ == '__main__':
    print('####################Begin Project2_load_data####################')
    Project2_load_data.execute(trial=True)
    doc = Project2_load_data.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))
    print('####################End Project2_load_data####################')
## eof