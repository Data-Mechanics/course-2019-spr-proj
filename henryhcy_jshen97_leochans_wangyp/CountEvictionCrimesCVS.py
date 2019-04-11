import dml
import datetime
import json
import prov.model
import pprint
import random
import uuid
from math import sin, cos, sqrt, atan2, radians


class CountEvictionCrimeCVS(dml.Algorithm):
    contributor = "henryhcy_jshen97_leochans_wangyp"
    reads = ['henryhcy_jshen97_leochans_wangyp.cvsEviction',
             'henryhcy_jshen97_leochans_wangyp.cvsCrime']
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
            master_cvs_id_list = []
            for document_cvs in repo.henryhcy_jshen97_leochans_wangyp.cvsEviction.find({'document_type': 'cvs'}):
                # R is the approximate radius of the earth in km
                # @see Haversine formula for latlng distance
                # All trig function in python use radian
                R = 6373.0

                lat_cvs = document_cvs['location']['lat']
                lng_cvs = document_cvs['location']['lng']

                lat_eviction = document_eviction['location'][0]
                lng_eviction = document_eviction['location'][1]

                dlon = lng_cvs - lng_eviction
                dlat = lat_cvs - lat_eviction
                a = sin(dlat / 2) ** 2 + cos(lat_eviction) * cos(lat_cvs) * sin(dlon / 2) ** 2
                c = 2 * atan2(sqrt(a), sqrt(1 - a))

                distance = R * c
                place_id = document_cvs['place_id']
                master_cvs_id_list.append((place_id, distance))
            sorted(master_cvs_id_list, key=lambda t: t[1], reverse=True)
            target_id_list = []
            for i in random.sample([i for i in range(24)], 5):
                target_id_list.append(master_cvs_id_list[i][0])
            repo['henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS'].update_many({'place_id': {'$in': target_id_list}}, {'$inc': {'eviction_case' : 1}})

        for document_crime in repo.henryhcy_jshen97_leochans_wangyp.cvsCrime.find({'document_type': 'crime'}):
            master_cvs_id_list = []
            for document_cvs in repo.henryhcy_jshen97_leochans_wangyp.cvsCrime.find({'document_type': 'cvs'}):
                # R is the approximate radius of the earth in km
                # @see Haversine formula for latlng distance
                # All trig function in python use radian
                R = 6373.0

                lat_cvs = document_cvs['location']['lat']
                lng_cvs = document_cvs['location']['lng']

                lat_crime = document_crime['location'][0]
                lng_crime = document_crime['location'][1]

                dlon = lng_cvs - lng_crime
                dlat = lat_cvs - lat_crime
                a = sin(dlat / 2) ** 2 + cos(lat_crime) * cos(lat_cvs) * sin(dlon / 2) ** 2
                c = 2 * atan2(sqrt(a), sqrt(1 - a))

                distance = R * c
                place_id = document_cvs['place_id']
                master_cvs_id_list.append((place_id, distance))
            sorted(master_cvs_id_list, key=lambda t: t[1], reverse=True)
            target_id_list = []
            for i in random.sample([i for i in range(18)], 2):
                target_id_list.append(master_cvs_id_list[i][0])
            repo['henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS'].update_many({'place_id': {'$in': target_id_list}}, {'$inc': {'crime_case': 1}})

        for document in repo['henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS'].find():
            data_evi = { 'rating_eviction': (document['rating'], document['eviction_case'])}
            data_cri = { 'rating_crime': (document['rating'], document['crime_case'])}
            repo['henryhcy_jshen97_leochans_wangyp.ratingEviction'].insert_one(data_evi)
            repo['henryhcy_jshen97_leochans_wangyp.ratingCrime'].insert_one(data_cri)

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

        # TODO: prov

        repo.logout()

        return doc

CountEvictionCrimeCVS.execute()