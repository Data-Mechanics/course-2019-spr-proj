import dml
import prov.model
import datetime
import uuid

class property_to_school(dml.Algorithm):
    contributor = 'xcao19_yjhang_zy0105'
    reads = ['xcao19_yjhang_zy0105.property','xcao19_yjhang_zy0105.school']
    writes = ['xcao19_yjhang_zy0105.property_to_school']

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

        # projection of property,
        X = list(repo['xcao19_yjhang_zy0105.property'].find({}, {'_id':0,'PID':1,'MAIL_ZIPCODE': 1}))

        # projection of school
        Y = list(repo['xcao19_yjhang_zy0105.school'].find({}, {'_id':0,'SCHID':1,'ZIP':1}))

        #join of property and school
        M = select(product(X, Y), lambda t: t[0]['MAIL_ZIPCODE'] == t[1]['ZIP'])
        RESULT = project(M, lambda t: {'ZIP': t[0]['MAIL_ZIPCODE'], 'PID': t[0]['PID'],
                                    'SCHID': t[1]['SCHID']})

        repo.dropCollection("property_to_school")
        repo.createCollection("property_to_school")
        repo["xcao19_yjhang_zy0105.property_to_school"].insert_many(RESULT)

        # print(list(repo["xcao19_yjhang_zy0105.property_to_school"].find()))

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


        this_script = doc.agent('alg:xcao19_yjhang_zy0105#property_to_school', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        property = doc.entity('dat:xcao19_yjhang_zy0105#property',
                           {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        school = doc.entity('dat:xcao19_yjhang_zy0105#school',
                               {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        property_school_by_zip = doc.entity('dat:xcao19_yjhang_zy0105#property_school_by_zip',
                               {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})

        ppty_scl_join_by_zip = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(ppty_scl_join_by_zip, this_script)

        doc.usage(ppty_scl_join_by_zip, school, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(ppty_scl_join_by_zip, property, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})

       
        doc.wasAttributedTo(property_school_by_zip, this_script)
        #doc.wasAttributedTo(citycrime, this_script)
        doc.wasGeneratedBy(property_school_by_zip, ppty_scl_join_by_zip, endTime)

        doc.wasDerivedFrom(property_school_by_zip, school, ppty_scl_join_by_zip, ppty_scl_join_by_zip, ppty_scl_join_by_zip)
        doc.wasDerivedFrom(property_school_by_zip, property, ppty_scl_join_by_zip, ppty_scl_join_by_zip, ppty_scl_join_by_zip)

        repo.logout()

        return doc
