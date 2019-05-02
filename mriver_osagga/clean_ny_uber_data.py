import urllib.request
import json
import csv
import dml
import prov.model
import datetime
import uuid
from io import StringIO
import itertools

class clean_ny_uber_data(dml.Algorithm):
    contributor = 'mriver_osagga'
    reads = ['mriver_osagga.ny_uber_data']
    writes = ['mriver_osagga.ny_uber_data_clean']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mriver_osagga', 'mriver_osagga')

        data = repo['mriver_osagga.ny_uber_data'].find()
        if (trial):
            data = itertools.islice(data, 0, 600000, 1000)
        
        # Project + Select only the pickup times within the rush-hour boundaries
        # defined from 7AM-10AM and from 5PM-8PM
        r_p = list()
        for val in data:
            time = val['Date/Time']
            pickup_hour = datetime.datetime.strptime(time, '%m/%d/%Y %H:%M:%S').hour
            if ((7 <= pickup_hour <= 10) or (17 <= pickup_hour <= 19)):
                # Check if the value is within rush hour boundaries
                r_p.append({"Date/Time": time, "Lat": val['Lat'],"Lon": val['Lon']})

        r = json.loads(json.dumps(r_p))

        repo.dropCollection("ny_uber_data_clean")
        repo.createCollection("ny_uber_data_clean")
        repo['mriver_osagga.ny_uber_data_clean'].insert_many(r)
        repo['mriver_osagga.ny_uber_data_clean'].metadata(
            {'complete': True})
        print(repo['mriver_osagga.ny_uber_data_clean'].metadata())

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
        # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')

        this_script = doc.agent('alg:mriver_osagga#clean_ny_uber_data', {
                                prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        
        resource = doc.entity('dat:ny_uber_data', {
                              'prov:label': 'Uber trip data', prov.model.PROV_TYPE: 'ont:DataSet'})
        
        clean_ny_uber_data = doc.activity(
            'log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(clean_ny_uber_data, this_script)
        doc.usage(clean_ny_uber_data, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Uber+trip+data&$select=name'
                   }
                  )

        ny_uber_data_clean = doc.entity('dat:mriver_osagga#ny_uber_data_clean', {
                                             prov.model.PROV_LABEL: 'Uber Trip Pickup locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(ny_uber_data_clean, this_script)
        doc.wasGeneratedBy(ny_uber_data_clean,
                           clean_ny_uber_data, endTime)
        doc.wasDerivedFrom(ny_uber_data_clean, resource,
                           clean_ny_uber_data, clean_ny_uber_data, clean_ny_uber_data)

        repo.logout()

        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
clean_ny_uber_data.execute(True)
doc = clean_bos_neighborhoods.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
# eof
