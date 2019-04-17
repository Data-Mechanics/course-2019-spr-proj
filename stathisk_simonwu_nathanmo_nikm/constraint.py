import prov.model
import dml
import datetime
import uuid
import json
import pandas as pd

constraint1 = 'constraint_yes_greater_than_no'
constraint2 = 'constraint_yes_greater_than_one_and_half_of_no'
constraint3 = 'constraint_yes_and_no_greater_than_two_times_of_blank'

class constraint(dml.Algorithm):
    # constraint1 = 'constraint_yes_greater_than_no'
    # constraint2 = 'constraint_yes_greater_than_two_times_of_no'
    # constraint3 = 'constraint_yes_and_no_greater_than_two_times_of_blank'
    global constraint1
    global constraint2
    global constraint3
    contributor = 'stathisk_simonwu_nathanmo_nikm'
    reads = ['stathisk_simonwu_nathanmo_nikm.avgAnswers']
    writes = ['stathisk_simonwu_nathanmo_nikm.' + constraint1,
              'stathisk_simonwu_nathanmo_nikm.' + constraint2,
              'stathisk_simonwu_nathanmo_nikm.' + constraint3]

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        dml.pymongo.MongoClient()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')

        avgAnswers = repo['stathisk_simonwu_nathanmo_nikm.avgAnswers'].find({}, {'_id':False})

        df = pd.DataFrame(list(avgAnswers))

        # c1
        global constraint1
        repo.dropCollection(constraint1)
        repo.createCollection(constraint1)
        dataC1 = df[df['Yes'] > df['No']]
        repo[constraint1].insert_many(json.loads(dataC1.to_json(orient='records')))
        repo[constraint1].metadata({'complete': True})

        # # c2
        global constraint2
        repo.dropCollection(constraint2)
        repo.createCollection(constraint2)
        dataC2 = df[df['Yes'] > 1.5 * df['No']]
        repo[constraint2].insert_many(json.loads(dataC2.to_json(orient='records')))
        repo[constraint2].metadata({'complete': True})

        # # c3
        global constraint3
        repo.dropCollection(constraint3)
        repo.createCollection(constraint3)
        dataC3 = df[df['Yes'] + df['No'] > 2 * df['Blanks']]
        repo[constraint3].insert_many(json.loads(dataC3.to_json(orient='records')))
        repo[constraint3].metadata({'complete': True})

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
        repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:stathisk_simonwu_nathanmo_nikm#constraint',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        avgAnswers = doc.entity('dat:avgAnswers',
                        {'prov:label': 'average of Aggregation data', prov.model.PROV_TYPE: 'ont:DataResource',
                         'ont:Extension': 'json'})
        dataConstraintOne = doc.entity('dat:' + constraint1,
                                {'prov:label': 'records that the number of people answer with "yes" are greater'
                                               + 'than number of people answer with "no"', prov.model.PROV_TYPE: 'ont:DataResource',
                                 'ont:Extension': 'json'})
        dataConstraintTwo = doc.entity('dat:' + constraint2,
                                {'prov:label': 'records that the number of people answer with "yes" are at least 1.5 times of'
                                               + 'number of people answer with "no"', prov.model.PROV_TYPE: 'ont:DataResource',
                                 'ont:Extension': 'json'})
        dataConstraintThree = doc.entity('dat:' + constraint3,
                                {'prov:label': 'records that the number of people answer with "yes" together with people answer'
                                               + ' with "no" are at least twice as the number of blank(give up voting)', prov.model.PROV_TYPE: 'ont:DataResource',
                                 'ont:Extension': 'json'})

        get_avgAggregation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        get_constraint_one = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_constraint_two = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_constraint_three = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_avgAggregation, this_script)
        doc.wasAssociatedWith(get_constraint_one, this_script)
        doc.wasAssociatedWith(get_constraint_two, this_script)
        doc.wasAssociatedWith(get_constraint_three, this_script)

        doc.usage(get_constraint_one, avgAnswers, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:DataResource'})
        doc.usage(get_constraint_two, avgAnswers, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:DataResource'})
        doc.usage(get_constraint_three, avgAnswers, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:DataResource'})

        doc.wasDerivedFrom(dataConstraintOne, avgAnswers, get_constraint_one, get_constraint_one, get_constraint_one)
        doc.wasDerivedFrom(dataConstraintTwo, avgAnswers, get_constraint_two, get_constraint_two, get_constraint_two)
        doc.wasDerivedFrom(dataConstraintThree, avgAnswers, get_constraint_three, get_constraint_three, get_constraint_three)

        doc.wasAttributedTo(get_avgAggregation, this_script)
        doc.wasAttributedTo(get_constraint_one, this_script)
        doc.wasAttributedTo(get_constraint_two, this_script)
        doc.wasAttributedTo(get_constraint_three, this_script)

        doc.wasGeneratedBy(avgAnswers, get_avgAggregation, endTime)
        doc.wasGeneratedBy(dataConstraintOne, get_constraint_one, endTime)
        doc.wasGeneratedBy(dataConstraintTwo, get_constraint_two, endTime)
        doc.wasGeneratedBy(dataConstraintThree, get_constraint_three, endTime)

        repo.logout()
        return doc


