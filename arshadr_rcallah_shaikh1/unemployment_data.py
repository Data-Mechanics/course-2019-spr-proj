import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd


class unemployment_data(dml.Algorithm):
    contributor = 'arshadr_rcallah_shaikh1'
    reads = []
    writes = ['arshadr_rcallah_shaikh1.unemployment']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')
        print('Connected to DB')

        url = 'https://chelseama.ogopendata.com/dataset/96856462-09a8-4384-a5b5-105069245ad4/resource/abda138a-b402-4ca6-9663-0746bf4e32f4/download/labor-force-and-unemployment-data-chelsea-2001-2017.csv.xlsx'
        json_str = pd.read_excel(url).to_json()

        json_dict = json.loads(json_str)

        repo.dropCollection("unemployment")
        repo.createCollection("unemployment")

        repo['arshadr_rcallah_shaikh1.unemployment'].insert_one(json_dict)
        repo['arshadr_rcallah_shaikh1.unemployment'].metadata({'complete': True})

        print(repo['arshadr_rcallah_shaikh1.unemployment'].metadata())

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
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('drv', 'https://drive.google.com/')
        doc.add_namespace('chl', 'https://chelseama.ogopendata.com/dataset/')

        this_script = doc.agent('alg:arshadr_rcallah_shaikh1#unemployment',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('drv:file/d/1kiukuGI3Kl5qBzsKHOtCW17DdhaCLiMY/view',
                              {prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})

        get_unemployment = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_unemployment, this_script)
        doc.usage(get_unemployment, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Retrieval': ''
                   }
                  )

        unemployment = doc.entity('dat:arshadr_rcallah_shaikh1#unemployment',
                                {prov.model.PROV_LABEL: 'Unemployment Data', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(unemployment, this_script)
        doc.wasGeneratedBy(unemployment, get_unemployment, endTime)
        doc.wasDerivedFrom(unemployment, resource, get_unemployment, get_unemployment, get_unemployment)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
unemployment_data.execute()
doc = unemployment_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof