import dml
import prov.model
import datetime
import uuid

class center_to_policeStation(dml.Algorithm):
    contributor = 'xcao19_yjhang_zy0105'
    reads = ['xcao19_yjhang_zy0105.center','xcao19_yjhang_zy0105.policeStation']
    writes = ['xcao19_yjhang_zy0105.center_to_policeStation']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('xcao19_yjhang_zy0105', 'xcao19_yjhang_zy0105')

        def select(R, s):
            return [t for t in R if s(t)]

        def product(R, S):
            return [(t, u) for t in R for u in S]

        def project(R, p):
            return [p(t) for t in R]

        # projection of policeStation
        X = list(repo['xcao19_yjhang_zy0105.policeStation'].find({}, {'_id':0,'BID':1,'ZIP': 1}))

        # projection of center
        Y = list(repo['xcao19_yjhang_zy0105.center'].find({}, {'_id':0,'FID':1,'ZIP':1}))

        #join of policeStation and center
        M = select(product(X, Y), lambda t: t[0]['ZIP'] == t[1]['ZIP'])
        RESULT = project(M, lambda t: {'ZIP': t[0]['ZIP'], 'BID': t[0]['BID'],
                                    'FID': t[1]['FID']})

        repo.dropCollection("center_to_policeStation")
        repo.createCollection("center_to_policeStation")
        repo["xcao19_yjhang_zy0105.center_to_policeStation"].insert_many(RESULT)

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('xcao19_yjhang_zy0105', 'xcao19_yjhang_zy0105')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        #doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')


        this_script = doc.agent('alg:xcao19_yjhang_zy0105#center_to_policeStation', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        center = doc.entity('dat:xcao19_yjhang_zy0105#center',
                           {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        policeStation = doc.entity('dat:xcao19_yjhang_zy0105#policeStation',
                               {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        center_policeStation_by_zip = doc.entity('dat:xcao19_yjhang_zy0105#center_policeStation_by_zip',
                               {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})

        ctr_pS_join_by_zip = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(ctr_pS_join_by_zip, this_script)

        doc.usage(ctr_pS_join_by_zip, center, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(ctr_pS_join_by_zip, policeStation, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})

       
        doc.wasAttributedTo(center_policeStation_by_zip, this_script)
        #doc.wasAttributedTo(citycrime, this_script)
        doc.wasGeneratedBy(center_policeStation_by_zip, ctr_pS_join_by_zip, endTime)

        doc.wasDerivedFrom(center_policeStation_by_zip, center, ctr_pS_join_by_zip, ctr_pS_join_by_zip, ctr_pS_join_by_zip)
        doc.wasDerivedFrom(center_policeStation_by_zip, policeStation, ctr_pS_join_by_zip, ctr_pS_join_by_zip, ctr_pS_join_by_zip)

        repo.logout()

        return doc