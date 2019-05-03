import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class center(dml.Algorithm):
    contributor = 'xcao19_yjhang_zy0105'
    reads = []
    writes = ['Jinghang_Yuan.center']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('xcao19_yjhang_zy0105', 'xcao19_yjhang_zy0105')

        url = 'http://datamechanics.io/data/Jinghang_Yuan/communityCenter.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("center")
        repo.createCollection("center")
        repo['xcao19_yjhang_zy0105.center'].insert_many(r)
        repo['xcao19_yjhang_zy0105.center'].metadata({'complete': True})
        # print('-----------------')
        # print(list(repo['Jinghang_Yuan.center'].find()))
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


        this_script = doc.agent('alg:xcao19_yjhang_zy0105#center',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8 w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_center = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)



        doc.wasAssociatedWith(get_center, this_script)
        doc.usage(get_center, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': 'FID,OBJECTID,SITE,PHONE,FAX,STREET,NEIGH,ZIP'
                   }
                  )

        center = doc.entity('dat:xcao19_yjhang_zy0105#center',
                          {prov.model.PROV_LABEL: 'center', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(center, this_script)
        doc.wasGeneratedBy(center, get_center, endTime)
        doc.wasDerivedFrom(center, resource, get_center, get_center, get_center)

        repo.logout()

        return doc

# center.execute()
# center.provenance()
# doc = center.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
