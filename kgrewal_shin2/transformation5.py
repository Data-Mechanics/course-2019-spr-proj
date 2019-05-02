import json
import dml
import prov.model
import datetime
import uuid
from math import floor


class transformation5():
    contributor = 'kgrewal_shin2'
    reads = ['kgrewal_shin2.unclaimed_streets', 'kgrewal_shin2.neigh_zip']
    writes = ['kgrewal_shin2.neigh_streets']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')

        unclaimed_streets = list(repo.kgrewal_shin2.unclaimed_streets.find())
        neigh_zip = list(repo.kgrewal_shin2.neigh_zip.find())

        streets = []
        for s in unclaimed_streets:
            zip_street = s['zipcode']

            for n in neigh_zip:
                if n['zipcode'] is None:
                    continue
                zip_neigh = "0" + str(floor(n['zipcode']))
                neigh = n['neighborhood']
                if zip_street == zip_neigh:
                    entry = {'full_name': s['full_name'],
                             'gender': s['gender'],
                             'zipcode': s['zipcode'],
                             'street_name': s['street_name'],
                             'LAT': s['LAT'],
                             'LNG': s['LNG'],
                             'neighborhood': neigh}
                    streets.append(entry)

        repo.dropCollection("neigh_streets")
        repo.createCollection("neigh_streets")
        repo['kgrewal_shin2.neigh_streets'].insert_many(streets)
        repo['kgrewal_shin2.neigh_streets'].metadata({'complete': True})
        print(repo['kgrewal_shin2.neigh_streets'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:kgrewal_shin2#transformation5',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_streets = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_streets, this_script)
        doc.usage(get_streets, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Neigh+Streets&$select=full_name,gender,zipcode,street_name, LAT, LNG, neighborhood'
                   }
                  )

        streets = doc.entity('dat:kgrewal_shin2#neigh_streets',
                             {prov.model.PROV_LABEL: 'Neighborhoods of Streets', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(streets, this_script)
        doc.wasGeneratedBy(streets, get_streets, endTime)
        doc.wasDerivedFrom(streets, resource, get_streets, get_streets, get_streets)

        repo.logout()

        return doc


doc = transformation5.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
