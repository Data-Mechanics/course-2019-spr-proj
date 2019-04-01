import json
import dml
import prov.model
import datetime
import uuid

class transformation4():

    contributor = 'kgrewal_shin2'
    reads = ['kgrewal_shin2.streets_without_schools', 'kgrewal_shin2.streets_without_landmarks']
    writes = ['kgrewal_shin2.unclaimed_streets']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')

        no_schools = repo.kgrewal_shin2.streets_without_schools.find()
        no_landmarks = repo.kgrewal_shin2.streets_without_landmarks.find()

        pub_streets = []
        for s in no_schools:
            pub_streets.append(s['street_name'])

        free_streets = []
        for x in no_landmarks:
            if x['street_name'] not in pub_streets:
                continue
            else:
                free_streets.append(x)

        street_zip = []
        zips = list(repo.kgrewal_shin2.ma_zip_loc.find())

        for x in free_streets:
            zip = x['zipcode']
            if not zip:
                zip = '02111'
            elif len(zip)>5:
                zip = zip[:5]
            try:
                info = zips[0][zip]
                new = {'full_name': x['full_name'], 'gender': x['gender'], 'zipcode': zip, 'street_name':
                    x['street_name'], 'LAT': info['LAT'], 'LNG': info['LNG']}

                street_zip.append(new)
            except KeyError:
                continue


        repo.dropCollection("unclaimed_streets")
        repo.createCollection("unclaimed_streets")
        repo['kgrewal_shin2.unclaimed_streets'].insert_many(street_zip)
        repo['kgrewal_shin2.unclaimed_streets'].metadata({'complete': True})
        print(repo['kgrewal_shin2.unclaimed_streets'].metadata())


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

        this_script = doc.agent('alg:kgrewal_shin2#transformation4',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_streets = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_streets, this_script)
        doc.usage(get_streets, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Unclaimed+Streets&$select=full_name,gender,zipcode,street_name, LAT, LNG'
                   }
                  )

        streets = doc.entity('dat:kgrewal_shin2#unclaimed_streets',
                          {prov.model.PROV_LABEL: 'Unclaimed Streets', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(streets, this_script)
        doc.wasGeneratedBy(streets, get_streets, endTime)
        doc.wasDerivedFrom(streets, resource, get_streets, get_streets, get_streets)

        repo.logout()

        return doc


transformation4.execute()
doc = transformation4.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
