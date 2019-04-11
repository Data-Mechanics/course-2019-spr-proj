import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd


class uber_kmean(dml.Algorithm):
    contributor = 'chuci_yfch_yuwan_zhurh'
    reads = ['chuci_yfch_yuwan_zhurh.uber_loc']
    writes = ['chuci_yfch_yuwan_zhurh.kmean']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Get the uber dataset
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('chuci_yfch_yuwan_zhurh', 'chuci_yfch_yuwan_zhurh')
        # Basic function

        def product(R, S):
            return [(t, u) for t in R for u in S]

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k, v) in R if k == key])) for key in keys]

        def dist(p, q):
            (x1, y1) = p
            (x2, y2) = q
            return (x1 - x2) ** 2 + (y1 - y2) ** 2

        def plus(args):
            p = [0, 0]
            for (x, y) in args:
                p[0] += x
                p[1] += y
            return tuple(p)

        def scale(p, c):
            (x, y) = p
            return (x / c, y / c)

        k = 3
        json_data = list(repo['chuci_yfch_yuwan_zhurh.uber_loc'].find())
        data = pd.DataFrame(json_data)
        data = data[(data['latitude']>42) & (data['longitude']>-80) ]
        df = pd.DataFrame(columns=['x', 'y', 'k'])
        def kmean(input):
            def rep(x):
                for each in MP:
                    if each[1][0] == x['latitude'] and each[1][1] == x['longitude']:
                        return each[0]
                else:
                    return 1
            for k in range(1,input+1):
                P = []
                OLD = []
                for index, each in data.iterrows():
                    P.append((each['latitude'], each['longitude']))
                M = P[:k]
                num = 0
                while OLD != M:
                    num += 1
                    OLD = M
                    MPD = [(m, p, dist(m, p)) for (m, p) in product(M, P)]
                    PDs = [(p, dist(m, p)) for (m, p, d) in MPD]
                    PD = aggregate(PDs, min)
                    MP = [(m, p) for ((m, p, d), (p2, d2)) in product(MPD, PD) if p == p2 and d == d2]
                    MT = aggregate(MP, plus)
                    M1 = [(m, 1) for (m, _) in MP]
                    MC = aggregate(M1, sum)
                    M = [scale(t, c) for ((m, t), (m2, c)) in product(MT, MC) if m == m2]
                    if num == 20:
                        break
                data[str(k) + '_mean'] = data.apply(rep, axis=1)
                print('----------')
                print(M)
                for each in range(k):
                    df.loc[df.shape[0]] = [M[each][0], M[each][1], k]
            ax = df.plot.scatter('x','y',c = 'k', colormap = 'tab10')
            fig = ax.get_figure()
            fig.savefig('fig.png')
        kmean(5)
        repo.dropCollection("kmean")
        repo.createCollection("kmean")
        repo['chuci_yfch_yuwan_zhurh.kmean'].insert_many(data.to_dict('records'))
        repo['chuci_yfch_yuwan_zhurh.kmean'].metadata({'complete': True})
        #print(list(repo['chuci_yfch_yuwan_zhurh.kmean'].find()))
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
        # New
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('chuci_yfch_yuwan_zhurh', 'chuci_yfch_yuwan_zhurh')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        agent = doc.agent('alg:chuci_yfch_yuwan_zhurh#kmean',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        entity_uber = doc.entity('dat:chuci_yfch_yuwan_zhurh#uber_location',
                           {prov.model.PROV_LABEL: 'uber data', prov.model.PROV_TYPE: 'ont:DataSet'})
        entity_kmean = doc.entity('dat:chuci_yfch_yuwan_zhurh#uber_kmean',
                           {prov.model.PROV_LABEL: 'health data', prov.model.PROV_TYPE: 'ont:DataSet'})

        activity = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(activity, agent)
        doc.usage(activity, entity_uber, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:aggregate'}
                  )
        doc.wasAttributedTo(entity_kmean, agent)
        doc.wasGeneratedBy(entity_kmean, activity, endTime)
        doc.wasDerivedFrom(entity_uber, entity_kmean, activity, activity, activity)

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