import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

############################################
# get_censushardship.py
# Script for collecting Chicago Census Socioeconomic Indicators
############################################

class get_censushardship(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.census']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('admin', 'example')
        repo_name = 'smithnj.census'
        # ---[ Grab Data ]-------------------------------------------
        df = pd.read_json('https://data.cityofchicago.org/api/views/kn9c-c2s2/rows.json?accessType=DOWNLOAD').to_json(orient='records')
        loaded = json.loads(df)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('census')
        repo.createCollection('census')
        print('done')
        repo[repo_name].insert_many(loaded)
        repo[repo_name].metadata({'complete': True})
        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('admin', 'example')
            doc.add_namespace('alg',
                              'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
            doc.add_namespace('dat',
                              'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
            doc.add_namespace('ont',
                              'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
            doc.add_namespace('bdp', 'http://data.cityofchicago.org/resource/kn9c-c2s2.json')

            this_script = doc.agent('alg:smithnj#census',
                                    {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
            resource = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

            get_censushardship = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(get_censushardship, this_script)
            doc.usage(get_censushardship, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
            doc.wasAttributedTo(get_censushardship, this_script)
            doc.wasGeneratedBy(get_censushardship, resource, endTime)
            doc.wasDerivedFrom(get_censushardship, resource)

            repo.logout()

            return doc