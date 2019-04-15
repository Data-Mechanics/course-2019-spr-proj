import dml
import datetime
import geopy.distance
import json
import prov.model
import pprint
import random
import uuid


class CountEvictionCrimeCVS(dml.Algorithm):
    contributor = "henryhcy_jshen97_leochans_wangyp"
    reads = ['henryhcy_jshen97_leochans_wangyp.cvsEviction',
             'henryhcy_jshen97_leochans_wangyp.counted']
    writes = ['henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS',
              'henryhcy_jshen97_leochans_wangyp.ratingEviction',
              'henryhcy_jshen97_leochans_wangyp.ratingCrime']

    @staticmethod
    def execute(trial=False):
        start_time = datetime.datetime.now()

        # set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('henryhcy_jshen97_leochans_wangyp', 'henryhcy_jshen97_leochans_wangyp')

        # create the result collections
        repo.dropCollection('countEvictionCrimeCVS')
        repo.dropCollection('ratingEviction')
        repo.dropCollection('ratingCrime')
        repo.createCollection('countEvictionCrimeCVS')
        repo.createCollection('ratingEviction')
        repo.createCollection('ratingCrime')

        for item in repo.henryhcy_jshen97_leochans_wangyp.cvsEviction.find({'document_type': 'cvs'}):
            d = {
                'location': item['location'],
                'place_id': item['place_id'],
                'rating': item['rating'] if 'rating' in item.keys() else None,
                'rating_count': item['user_ratings_total'] if 'user_ratings_total' in item.keys() else None,
                'eviction_case': 0,
                'crime_case': 0
            }
            repo['henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS'].insert_one(d)

        for document_eviction in repo.henryhcy_jshen97_leochans_wangyp.cvsEviction.find({'document_type': 'eviction'}):
            master_cvs_id = ''
            min_distance = float('inf')
            for document_cvs in repo.henryhcy_jshen97_leochans_wangyp.cvsEviction.find({'document_type': 'cvs'}):
                lat_cvs = document_cvs['location']['lat']
                lng_cvs = document_cvs['location']['lng']
                coord_cvs = (lat_cvs, lng_cvs)

                lat_evi = document_eviction['location'][0]
                lng_evi = document_eviction['location'][1]
                coord_evi = (lat_evi, lng_evi)

                distance = geopy.distance.distance(coord_cvs, coord_evi)
                if (min_distance > distance):
                    min_distance = distance
                    master_cvs_id = document_cvs['place_id']
            repo['henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS'].update_one({'place_id': master_cvs_id}, {'$inc': {'eviction_case' : 1}})

        for document_crime in repo.henryhcy_jshen97_leochans_wangyp.counted.find({'document_type': 'crime'}):
            master_cvs_id = ''
            min_distance = float('inf')
            for document_cvs in repo.henryhcy_jshen97_leochans_wangyp.counted.find({'document_type': 'cvs'}):
                lat_cvs = document_cvs['location']['lat']
                lng_cvs = document_cvs['location']['lng']
                coord_cvs = (lat_cvs, lng_cvs)

                lat_cri = document_crime['location'][0]
                lng_cri = document_crime['location'][1]
                coord_cri = (lat_cri, lng_cri)

                distance = geopy.distance.distance(coord_cvs, coord_cri)
                if (min_distance > distance):
                    min_distance = distance
                    master_cvs_id = document_cvs['place_id']
            repo['henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS'].update_one({'place_id': master_cvs_id}, {'$inc': {'crime_case': 1}})

        repo['henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS'].metadata({'complete': True})
        print(repo['henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS'].metadata())

        for document in repo['henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS'].find():
            if (document['eviction_case'] != 0):
                data_evi = { 'rating_eviction': (document['rating'], document['eviction_case'])}
                repo['henryhcy_jshen97_leochans_wangyp.ratingEviction'].insert_one(data_evi)
            if (document['crime_case'] != 0):
                data_cri = { 'rating_crime': (document['rating'], document['crime_case'])}
                repo['henryhcy_jshen97_leochans_wangyp.ratingCrime'].insert_one(data_cri)

        repo['henryhcy_jshen97_leochans_wangyp.ratingEviction'].metadata({'complete': True})
        print(repo['henryhcy_jshen97_leochans_wangyp.ratingEviction'].metadata())

        repo['henryhcy_jshen97_leochans_wangyp.ratingCrime'].metadata({'complete': True})
        print(repo['henryhcy_jshen97_leochans_wangyp.ratingCrime'].metadata())

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

        this_script = doc.agent('alg:henryhcy_jshen97_leochans_wangyp#CountEvictionCrimesCVS',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_cvsEvi = doc.entity('dat:cvsevi', {'prov:label': 'CVS&Eviction Central Boston', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_cvsCri = doc.entity('dat:cvscri', {'prov:label': 'CVS&Crime Central Boston', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_counted = doc.entity('dat:cvscri', {'prov:label': 'Counted set of CVS', prov.model.PROV_TYPE: 'ont:DataSet'})
        count_evi = doc.activity('log:uuid' + str(uuid.uuid4()), start_time, end_time)
        count_cri = doc.activity('log:uuid' + str(uuid.uuid4()), start_time, end_time)
        extract_ratingEvi = doc.activity('log:uuid' + str(uuid.uuid4()), start_time, end_time)
        extract_ratingCri = doc.activity('log:uuid' + str(uuid.uuid4()), start_time, end_time)

        doc.wasAssociatedWith(count_evi, this_script)
        doc.usage(count_evi, resource_cvsEvi, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(count_cri, this_script)
        doc.usage(count_cri, resource_cvsCri, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(extract_ratingEvi, this_script)
        doc.usage(extract_ratingEvi, resource_counted, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        doc.wasAssociatedWith(extract_ratingCri, this_script)
        doc.usage(extract_ratingCri, resource_counted, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        counted = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#countEvictionCrimeCVS',
                              {prov.model.PROV_LABEL: 'Counted E&C cvs central boston', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(counted, this_script)
        doc.wasGeneratedBy(counted, count_evi, end_time)
        doc.wasGeneratedBy(counted, count_cri, end_time)
        doc.wasDerivedFrom(counted, resource_cvsEvi, count_evi, count_evi, count_evi)
        doc.wasDerivedFrom(counted, resource_cvsCri, count_cri, count_cri, count_cri)

        rateEvi = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#ratingEviction',
                              {prov.model.PROV_LABEL: 'rating_eviction', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(rateEvi, this_script)
        doc.wasGeneratedBy(rateEvi, extract_ratingEvi, end_time)
        doc.wasDerivedFrom(rateEvi, resource_counted, extract_ratingEvi, extract_ratingEvi, extract_ratingEvi)

        rateCri = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#ratingCrime',
                             {prov.model.PROV_LABEL: 'rating_eviction', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(rateCri, this_script)
        doc.wasGeneratedBy(rateCri, extract_ratingCri, end_time)
        doc.wasDerivedFrom(rateCri, resource_counted, extract_ratingCri, extract_ratingCri, extract_ratingCri)

        repo.logout()

        return doc

# debug


CountEvictionCrimeCVS.execute()
'''
doc = CountEvictionCrimeCVS.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''