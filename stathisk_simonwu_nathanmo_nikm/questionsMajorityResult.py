# Created by StathisKara

import prov.model
import dml
import datetime
import uuid
import json
import pandas as koalas
import pymongo


def union(R, S):
    return R + S


def project(R, p):
    return [p(t) for t in R]


def select(R, s):
    return [t for t in R if s(t)]


def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k, v) in R if k == key])) for key in keys]


def selector(data):
    values = []
    for i in range(3, len(data)):
        values.append(int((str(data[i])).replace(',', '')))
    return [data[0], tuple(values)]


def multipleSums(data):
    sums = [0] * len(data[0])
    for j in range(0, len(data[0])):
        for i in range(0, len(data)):
            sums[j] += data[i][j]
    return sums


def multipleAverages(data):
    avgs = [0.0] * len(data[0])
    for j in range(0, len(data[0])):
        for i in range(0, len(data)):
            avgs[j] += data[i][j]
        avgs[j] = avgs[j] / len(data)
    return avgs


def unzipper(data):
    res = [data[0]]
    for dat in data[1]:
        res.append(dat)
    return res


def majority(data):
    if data[1][0] > data[1][1]:
        return [data[0], 'yes']
    return [data[0], 'no']


def weighted(data):
    sum = data[1][0] + data[1][1]
    return [data[0], data[1][0], data[1][1], data[1][0] + data[1][1]]


def weightedsum(data):
    # print(data)
    return [data[0], (data[1] + data[2]) / data[3]]


def average(data):
    return [data[0], data[1] / 24]


class questionsMajorityResult(dml.Algorithm):
    contributor = 'stathisk_simonwu_nathanmo_nikm'
    reads = ['stathisk_simonwu_nathanmo_nikm.q1', 'stathisk_simonwu_nathanmo_nikm.q2',
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
             'stathisk_simonwu_nathanmo_nikm.q24', 'stathisk_simonwu_nathanmo_nikm.scores']

    writes = ['stathisk_simonwu_nathanmo_nikm.majority', 'stathisk_simonwu_nathanmo_nikm.weighted']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        dml.pymongo.MongoClient()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('stathisk_simonwu_nathanmo_nikm', 'stathisk_simonwu_nathanmo_nikm')

        limit = 24
        if trial:
            limit = 5

        # Create collections names with data
        urls = []
        for i in range(1, limit):
            urls.append(('stathisk_simonwu_nathanmo_nikm.q' + str(i)))

        # Get the scores for each question
        scores = (list(repo['stathisk_simonwu_nathanmo_nikm.scores'].find({}, {'_id': False})))
        scores2 = (koalas.DataFrame(scores))
        scores = scores2[['NoScore', 'YesScore']].values[0:limit]

        combined = []
        combined2 = []
        for url, score in zip(urls, scores):
            # Retrieve the data from mongo
            df = list((repo[url].find()))
            df = koalas.DataFrame(df)
            df = df[['Locality', 'Ward', 'Pct', 'Yes', 'No', 'Blanks', 'Total Votes Cast']]

            # Aggregate number of yes/no based on locality
            res = aggregate(project(df.values[0:len(df.values) - 1], selector), multipleSums)

            # ------------ Weighted results ------------

            # Project for weighted results
            res2 = project(res, weighted)

            # Create the weighted score for yes and no
            for r in res2:
                r[1] = r[1] * score[0]
                r[2] = r[2] * score[1]

            combined2 = aggregate(union(combined2, project(res2, weightedsum)), sum)

            # ------------ Majority results ------------
            # Project for majority results
            res = project(res, majority)

            # Create the majority score for yes and no
            for r in res:
                if r[1] == 'yes':
                    r[1] = score[0]
                else:
                    r[1] = score[1]

            combined = aggregate(union(combined, res), sum)

        weightedC = koalas.DataFrame(data=project(combined2, average), columns=['Locality', 'Result'])
        repo['stathisk_simonwu_nathanmo_nikm.weighted'].drop()
        repo['stathisk_simonwu_nathanmo_nikm.weighted'].insert_many(json.loads(weightedC.to_json(orient='records')))
        repo['stathisk_simonwu_nathanmo_nikm.weighted'].metadata({'complete': True})


        majorityC = koalas.DataFrame(data=project(combined, average), columns=['Locality', 'Result'])
        print(majorityC)
        repo['stathisk_simonwu_nathanmo_nikm.majority'].drop()
        repo['stathisk_simonwu_nathanmo_nikm.majority'].insert_many(json.loads(majorityC.to_json(orient='records')))
        repo['stathisk_simonwu_nathanmo_nikm.majority'].metadata({'complete': True})

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

        this_script = doc.agent('alg:stathisk_simonwu_nathanmo_nikm#questionsMajorityResult',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resources = []
        for i in range(0, 25):
            resources.append(doc.entity('dat:question' + str(i),
                                        {'prov:label': 'ballot question ' + str(i),
                                         prov.model.PROV_TYPE: 'ont:DataResource',
                                         'ont:Extension': 'json'}))

        get_majority = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_majority, this_script)

        get_weighted = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_weighted, this_script)

        for i in range(0, 25):
            doc.usage(get_majority, resources[i], startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })
            doc.usage(get_weighted, resources[i], startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval', })

        majorityC = doc.entity('dat:stathisk_simonwu_nathanmo_nikm#majority',
                               {prov.model.PROV_LABEL: 'majority', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(majorityC, this_script)
        doc.wasGeneratedBy(majorityC, get_majority, endTime)

        weightedC = doc.entity('dat:stathisk_simonwu_nathanmo_nikm#weighted',
                               {prov.model.PROV_LABEL: 'majority', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(weightedC, this_script)
        doc.wasGeneratedBy(weightedC, get_weighted, endTime)

        for i in range(0, 25):
            doc.wasDerivedFrom(majorityC, resources[i], get_majority, get_majority, get_majority)
            doc.wasDerivedFrom(weightedC, resources[i], get_weighted, get_weighted, get_weighted)

        repo.logout()
        return doc


questionsMajorityResult.execute()
# doc = questionsMajorityResult.provenance()
# doc.serialize()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
