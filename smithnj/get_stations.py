import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

############################################
# get_stations.py
# Script for collecting CTA L station location
############################################

class get_stations(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.stations']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.stations'
        # ---[ Grab Data ]-------------------------------------------
        df = pd.read_csv('http://datamechanics.io/data/smithnj/CTA_RailStations_CAN.csv').to_json(orient='records')
        loaded = json.loads(df)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('smithnj.stations')
        repo.createCollection('smithnj.stations')
        print('done')
        repo[repo_name].insert_many(loaded)
        repo[repo_name].metadata({'complete': True})
        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/smithnj/stations') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/smithnj/stations')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont','http://datamechanics.io/ontology#')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=smithnj/')

        this_script = doc.agent('alg:smithnj#get_stations',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:stations',
                              {'prov:label': 'data set of l stations', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_stations = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_stations, this_script)
        doc.usage(get_stations, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': ''
                   }
                  )

        stations = doc.entity('dat:smithnj#stations',
                          {prov.model.PROV_LABEL: 'L Stations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, get_stations, endTime)
        doc.wasDerivedFrom(stations, resource, get_stations, get_stations, get_stations)

        repo.logout()

        return doc