import dml
import datetime
import geopy.distance
import json
import prov.model
import pprint
import uuid
from math import sin, cos, sqrt, atan2, radians


class CvsWalCrime(dml.Algorithm):

    contributor = "henryhcy_jshen97_leochans_wangyp"
    reads = ['henryhcy_jshen97_leochans_wangyp.cvs',
             'henryhcy_jshen97_leochans_wangyp.walgreen',
             'henryhcy_jshen97_leochans_wangyp.crime']
    writes = ['henryhcy_jshen97_leochans_wangyp.cvsCrime',
              'henryhcy_jshen97_leochans_wangyp.walgreenCrime']

    @staticmethod
    def execute(trial=False):

        start_time = datetime.datetime.now()

        # set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('henryhcy_jshen97_leochans_wangyp', 'henryhcy_jshen97_leochans_wangyp')

        # create the result collections
        repo.dropCollection('cvsCrime')
        repo.dropCollection('walgreenCrime')
        repo.createCollection('cvsCrime')
        repo.createCollection('walgreenCrime')

        # geo-location of boston
        lat_bos = 42.361145
        lng_bos = -71.057083
        coord_bos = (lat_bos, lng_bos)

        # insert those crime incidents that are within 30 km of boston.
        for document in repo.henryhcy_jshen97_leochans_wangyp.crime.find({'OFFENSE_CODE_GROUP': 'Larceny'}):
            lat_cri = document['Lat'] if document['Lat'] != None else 0.0
            lng_cri = document['Long'] if document['Long'] != None else 0.0
            coord_cri = (lat_cri, lng_cri)

            distance = geopy.distance.distance(coord_bos, coord_cri)
            if (distance < 5.5):
                d = {
                    'document_type': 'crime',
                    'crime_id': document['INCIDENT_NUMBER'],
                    'crime_code': document['OFFENSE_CODE'],
                    'location': [document['Lat'], document['Long']]
                }
                repo.henryhcy_jshen97_leochans_wangyp.cvsCrime.insert_one(d)
                repo.henryhcy_jshen97_leochans_wangyp.walgreenCrime.insert_one(d)

        # insert cvs within 15 km of boston
        for item in repo.henryhcy_jshen97_leochans_wangyp.cvs.find():
            lat_cvs = item['geometry']['location']['lat']
            lng_cvs = item['geometry']['location']['lng']
            coord_cvs = (lat_cvs, lng_cvs)

            distance = geopy.distance.distance(coord_bos, coord_cvs)
            if (distance <= 5):
                d = {
                    'document_type': 'cvs',
                    'location': item['geometry']['location'],
                    'place_id': item['place_id'],
                    'rating': item['rating'] if 'rating' in item.keys() else None,
                    'rating_count': item['user_ratings_total'] if 'user_ratings_total' in item.keys() else None
                }
                repo['henryhcy_jshen97_leochans_wangyp.cvsCrime'].insert_one(d)

        repo['henryhcy_jshen97_leochans_wangyp.cvsCrime'].metadata({'complete': True})
        print(repo['henryhcy_jshen97_leochans_wangyp.cvsCrime'].metadata())

        # insert walgreen within 15 km of boston
        for item in repo.henryhcy_jshen97_leochans_wangyp.walgreen.find():
            lat_wal = item['geometry']['location']['lat']
            lng_wal = item['geometry']['location']['lng']
            coord_wal = (lat_wal, lng_wal)

            distance = geopy.distance.distance(coord_bos, coord_wal)
            if (distance <= 5):
                d = {
                    'document_type': 'walgreen',
                    'location': item['geometry']['location'],
                    'place_id': item['place_id'],
                    'rating': item['rating'] if 'rating' in item.keys() else None,
                    'rating_count': item['user_ratings_total'] if 'user_ratings_total' in item.keys() else None
                }
                repo['henryhcy_jshen97_leochans_wangyp.walgreenCrime'].insert_one(d)

        repo['henryhcy_jshen97_leochans_wangyp.walgreenCrime'].metadata({'complete': True})
        print(repo['henryhcy_jshen97_leochans_wangyp.walgreenCrime'].metadata())

        repo.logout()

        end_time = datetime.datetime.now()

        return {"start": start_time, "end": end_time}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), start_time=None, end_time=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('henryhcy_jshen97_leochans_wangyp', 'henryhcy_jshen97_leochans_wangyp')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:henryhcy_jshen97_leochans_wangyp#CvsWalCrime',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_cvs = doc.entity('dat:cvs', {'prov:label': 'CVS Boston', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_wal = doc.entity('dat:wal', {'prov:label': 'Walgreen Boston', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_crime = doc.entity('dat:crime', {'prov:label': 'Crime Boston', prov.model.PROV_TYPE: 'ont:DataSet'})
        combine_cvs = doc.activity('log:uuid' + str(uuid.uuid4()), start_time, end_time)
        combine_wal = doc.activity('log:uuid' + str(uuid.uuid4()), start_time, end_time)

        doc.wasAssociatedWith(combine_cvs, this_script)
        doc.usage(combine_cvs, resource_cvs, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(combine_cvs, resource_crime, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(combine_wal, this_script)
        doc.usage(combine_wal, resource_wal, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(combine_wal, resource_crime, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        cvsCrime = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#cvsCrime', {prov.model.PROV_LABEL: 'Combine CVS Crime', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(cvsCrime, this_script)
        doc.wasGeneratedBy(cvsCrime, combine_cvs, end_time)
        doc.wasDerivedFrom(cvsCrime, resource_cvs, combine_cvs, combine_cvs, combine_cvs)
        doc.wasDerivedFrom(cvsCrime, resource_crime, combine_cvs, combine_cvs, combine_cvs)

        walCrime = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#walgreenCrime', {prov.model.PROV_LABEL: 'Combine Walgreen Crime', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(walCrime, this_script)
        doc.wasGeneratedBy(walCrime, combine_wal, end_time)
        doc.wasDerivedFrom(walCrime, resource_wal, combine_wal, combine_wal, combine_wal)
        doc.wasDerivedFrom(walCrime, resource_crime, combine_wal, combine_wal, combine_wal)
        repo.logout()

        return doc

# debug

CvsWalCrime.execute()
'''
doc = CvsWalCrime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
