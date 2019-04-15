import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

############################################
# get_taxitrips.py
# Script for collecting Chicago Taxi Trip data
############################################

class get_taxitrips(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.taxistats']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('admin', 'example')
        repo_name = 'smithnj.taxistats'
        # ---[ Grab Data ]-------------------------------------------
        df = pd.read_csv('_INSERT LINK HERE_').to_json(orient='records') #TODO insert link here.
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
            doc.add_namespace('bdp', '_') #TODO insert link here.

            this_script = doc.agent('alg:smithnj#taxistats',
                                    {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
            resource = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

            get_taxitrips = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(get_taxitrips, this_script)
            doc.usage(get_taxitrips, resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Computation'})
            doc.wasAttributedTo(get_taxitrips, this_script)
            doc.wasGeneratedBy(get_taxitrips, resource, endTime)
            doc.wasDerivedFrom(get_taxitrips, resource)

            repo.logout()

            return doc