import urllib.request
import json
import csv
import dml
import prov.model
import datetime
import uuid
import ssl
from io import StringIO


class fetch_data(dml.Algorithm):
    contributor = 'mriver_osagga'
    reads = []
    writes = [ 'mriver_osagga.ny_uber_data' ]

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        context = ssl._create_unverified_context()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mriver_osagga', 'mriver_osagga')
        
        # for the data set `ny_uber_data`
        url = 'http://datamechanics.io/data/uber-raw-data-jun14.csv'
        response = urllib.request.urlopen(url, context=context).read().decode("utf-8")
        r = [json.loads(json.dumps(d))
            for d in csv.DictReader(StringIO(response))]
        repo.dropCollection("ny_uber_data")
        repo.createCollection("ny_uber_data")
        repo['mriver_osagga.ny_uber_data'].insert_many(r)
        repo['mriver_osagga.ny_uber_data'].metadata({'complete': True})
        print(repo['mriver_osagga.ny_uber_data'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mriver_osagga', 'mriver_osagga')
        # The scripts are in <folder>#<filename> format.
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        # The event log.
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:mriver_osagga#fetch_data', {
                                prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_6 = doc.entity('dat:uber-raw-data-jun14', {
                                'prov:label': 'Uber trip data', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'csv'})

        fetch_data = doc.activity(
            'log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(fetch_data, this_script)

        doc.usage(fetch_data, resource_6, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=uber+trip+data&$select=name'
                   }
                  )

        output_6 = doc.entity('dat:mriver_osagga#ny_uber_data', {
                              prov.model.PROV_LABEL: 'Uber trip data', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(output_6, this_script)

        doc.wasGeneratedBy(output_6, fetch_data, endTime)

        doc.wasDerivedFrom(output_6, resource_6, fetch_data,
                           fetch_data, fetch_data)

        repo.logout()

        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
fetch_data.execute()
doc = fetch_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
# eof
