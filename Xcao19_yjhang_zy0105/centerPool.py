import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class centerPool(dml.Algorithm):
    contributor = 'xcao19_yjhang_zy0105'
    reads = []
    writes = ['xcao19_yjhang_zy0105.centerPool']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('xcao19_yjhang_zy0105', 'xcao19_yjhang_zy0105')

        url = 'http://datamechanics.io/data/Jinghang_Yuan/centerPool.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("centerPool")
        repo.createCollection("centerPool")
        repo['xcao19_yjhang_zy0105.centerPool'].insert_many(r)
        repo['xcao19_yjhang_zy0105.centerPool'].metadata({'complete': True})
        # print('-----------------')
        # print(list(repo['Jinghang_Yuan.centerPool'].find()))
        # print('-----------------')

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('xcao19_yjhang_zy0105', 'xcao19_yjhang_zy0105')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:xcao19_yjhang_zy0105#centerPool',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_centerPool = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_centerPool, this_script)
        doc.usage(get_centerPool, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': 'FID,OBJECTID_1,OBJECTID,PHONE,FAX,STREET,NEIGH,ZIP,SITE'
                   }
                  )
        centerPool = doc.entity('dat:Jinghang_Yuan#centerPool',
                          {prov.model.PROV_LABEL: 'centerPool', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(centerPool, this_script)
        doc.wasGeneratedBy(centerPool, get_centerPool, endTime)
        doc.wasDerivedFrom(centerPool, resource, get_centerPool, get_centerPool, get_centerPool)

        repo.logout()

        return doc

# centerPool.execute()
# centerPool.provenance()
# doc = centerPool.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
