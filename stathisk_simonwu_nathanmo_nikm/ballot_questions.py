import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pandas

start_score = 1
end_score = 5

class ballot_questions(dml.Algorithm):
    contributor = 'stathisk_simonwu_nathanmo_nikm'
    reads = []
    writes = ['stathisk_simonwu_nathanmo_nikm.scores']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()


        # Set up the database connection.
        dml.pymongo.MongoClient()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')

        data = pandas.read_excel(r'/Users/nikhileshm/Desktop/BallotQuestions.xlsx')
        #print(data)

        df = pandas.DataFrame(data, columns=['year', 'number', 'question_long', 'Progressive/Conservative'])
        df.rename(columns={"year":"Year", "number":"Qnumber", "question_long": "Question", "Progressive/Conservative":"YesScore"}, inplace=True)
        df['NoScore'] = df.apply(lambda t: end_score - (t.YesScore - start_score), axis=1)
        print(df)

        #df.to_excel("../BallotQuestionsOutput.xlsx")
        collection = 'stathisk_simonwu_nathanmo_nikm.scores'
        repo[collection].insert_many(json.loads(df.to_json(orient='records')))
        repo[collection].metadata({'complete': True})

        repo.logout()
        endTime = datetime.datetime.now()

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
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
        #doc.add_namespace('ballotquestions', 'http://electionstats.state.ma.us/ballot_questions/')

        this_script = doc.agent('alg:stathisk_simonwu_nathanmo_nikm#scores',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource1 = doc.entity('dat:score',
                               {'prov:label': 'ballot question scores ', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})

        get_q1 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_q1, this_script)

        doc.usage(get_q1, resource1, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        q1 = doc.entity('dat:stathisk_simonwu_nathanmo_nikm#q1',
                        {prov.model.PROV_LABEL: 'Scores', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(q1, this_script)
        doc.wasGeneratedBy(q1, get_q1, endTime)
        doc.wasDerivedFrom(q1, resource1, get_q1, get_q1, get_q1)

        repo.logout()
        return doc

ballot_questions.execute(True)
doc = ballot_questions.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))