# Created by StathisKara

import json
import dml
import prov.model
import datetime
import uuid
import pandas as koalas


class questionsAllYears(dml.Algorithm):
    contributor = 'stathisk_simonwu_nathanmo_nikm'
    reads = ['stathisk_simonwu_nathanmo_nikm.links']
    writes = ['stathisk_simonwu_nathanmo_nikm.q1', 'stathisk_simonwu_nathanmo_nikm.q2',
              'stathisk_simonwu_nathanmo_nikm.q3',
              'stathisk_simonwu_nathanmo_nikm.q4', 'stathisk_simonwu_nathanmo_nikm.q5',
              'stathisk_simonwu_nathanmo_nikm.q6',
              'stathisk_simonwu_nathanmo_nikm.q7', 'stathisk_simonwu_nathanmo_nikm.q8',
              'stathisk_simonwu_nathanmo_nikm.q9',
              'stathisk_simonwu_nathanmo_nikm.q10', 'stathisk_simonwu_nathanmo_nikm.q11',
              'stathisk_simonwu_nathanmo_nikm.q12',
              'stathisk_simonwu_nathanmo_nikm.q13', 'stathisk_simonwu_nathanmo_nikm.q14',
              'stathisk_simonwu_nathanmo_nikm.q15',
              'stathisk_simonwu_nathanmo_nikm.q16', 'stathisk_simonwu_nathanmo_nikm.q17',
              'stathisk_simonwu_nathanmo_nikm.q18',
              'stathisk_simonwu_nathanmo_nikm.q19', 'stathisk_simonwu_nathanmo_nikm.q20',
              'stathisk_simonwu_nathanmo_nikm.q21',
              'stathisk_simonwu_nathanmo_nikm.q22', 'stathisk_simonwu_nathanmo_nikm.q23',
              'stathisk_simonwu_nathanmo_nikm.q24']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        dml.pymongo.MongoClient()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')

        # Retrieve the urls
        urls = (repo['stathisk_simonwu_nathanmo_nikm.links'].find({}, {'_id': False}))
        urls = koalas.DataFrame(urls).values

        limit = len(urls)
        if trial:
            limit = 5

        for i in range(0, limit):
            # Create a collection for each ballot question
            question = "q" + str(i + 1)
            repo.dropCollection(question)
            repo.createCollection(question)

            # Add each question to its corresponding collection
            df = koalas.read_csv(urls[i][0])
            collection = 'stathisk_simonwu_nathanmo_nikm.' + question
            repo[collection].insert_many(json.loads(df.to_json(orient='records')))
            repo[collection].metadata({'complete': True})

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
        # return doc
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('ballotquestions', 'http://electionstats.state.ma.us/ballot_questions/')

        this_script = doc.agent('alg:stathisk_simonwu_nathanmo_nikm#questionsAllYears',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource = doc.entity('dat:links',
                              {'prov:label': 'ballot questions links ', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resources = []
        for i in range(0, 25):
            resources.append(doc.entity('dat:question ' + str(i),
                                        {'prov:label': 'ballot question ' + str(i),
                                         prov.model.PROV_TYPE: 'ont:DataResource',
                                         'ont:Extension': 'json'}))
        # -----------------------------------------

        for i in range(0, 25):
            get_q = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(get_q, this_script)
            r = doc.entity('dat:question ' + str(i),
                           {'prov:label': 'ballot question ' + str(i),
                            prov.model.PROV_TYPE: 'ont:DataResource',
                            'ont:Extension': 'json'})
            doc.usage(get_q, r, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })

            q = doc.entity('dat:stathisk_simonwu_nathanmo_nikm#q' + str(i),
                           {prov.model.PROV_LABEL: 'q' + str(i), prov.model.PROV_TYPE: 'ont:DataSet'})
            doc.wasAttributedTo(q, this_script)
            doc.wasGeneratedBy(q, get_q, endTime)
            doc.wasDerivedFrom(q, r, get_q, get_q, get_q)

        # ------------------------------------------
        repo.logout()

        return doc


'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof

# questionsAllYears.execute(True)
# doc = questions.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
