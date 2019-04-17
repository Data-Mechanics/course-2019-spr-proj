import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import logging

import argparse
import json
import requests
import sys
import os

from urllib.error import HTTPError
from urllib.parse import quote

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

API_KEY = 'qtpGY7-Tf1AxOXvUBgVpouLaPW_s-A_7wckLvoGujK3AAaSfDheyfbMkK4tszEYH_jJ1byrvpuRJ5VxblOwT_xqtiXfXXUTl8HHzUDwJ9QPU71O0cs9YA9RFiycFWXYx'

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.

DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'Boston, MA'
SEARCH_LIMIT = 1

class yelp_business(dml.Algorithm):
    contributor = 'kzhang21_ryuc_zui_sarms'
    reads = ['kzhang21_ryuc_zui_sarms.food_violations']
    writes = ['kzhang21_ryuc_zui_sarms.yelp_business']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')

       

        zip_codes = {'02108', '02109', '02110', '02111', '02113', '02114', '02115', '02116', '02118', '02119', '02120',
                     '02121', '02122', '02124', '02125', '02126', '02127', '02128', '02129', '02130', '02131', '02132',
                     '02133', '02134', '02135', '02136', '02163', '02199', '02203', '02210', '02215', '02222', '02112',
                     '02117', '02123', '02137', '02196', '02205', '02283', '02284', '02298', '02201', '02204', '02206',
                     '02211', '02212', '02217', '02241', '02266', '02293', '02297'}

        food_violations = repo['kzhang21_ryuc_zui_sarms.food_violations']
        df_violations = pd.DataFrame(list(food_violations.find()))

        
        # with open('yelp_businesses_new.json') as json_file:
        #     CACHE = json.load(json_file)
        #     if len(CACHE) < 2:
        #         ## for each entry in violation get data
        #         print('HERE: lets get this business')
        #         for index, row in df_violations.iterrows():

        #             term = row["businessname"]
        #             location =  row["address"] + ', ' + row["city"] + ', ' + row["state"] + ', ' + row["zip"]

        #             parser.add_argument('-q', '--term', dest='term', default=term,
        #                                 type=str, help='Search term (default: %(default)s)')
        #             parser.add_argument('-l', '--location', dest='location',
        #                                 default=location, type=str,
        #                                 help='Search location (default: %(default)s)')

        #             input_values = parser.parse_args()

        #             try:
        #                 result = query_api(input_values.term, input_values.location)
        #                 if result["location"]["zip_code"] in zip_codes:
        #                     print(row["businessname"])
        #                     result["violation_rate"] = row["violationRate"]
        #                     CACHE.append(result)
        #             except HTTPError as error:
        #                 sys.exit(
        #                     'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
        #                         error.code,
        #                         error.url,
        #                         error.read(),
        #                     )
        #                 )
        #         json.dump(CACHE, json_file)

        # repo['kzhang21_ryuc_zui_sarms.yelp_business'].insert_many(CACHE)
        # repo['kzhang21_ryuc_zui_sarms.yelp_business'].metadata({'complete':True})

        # print(repo['kzhang21_ryuc_zui_sarms.yelp_business'].metadata())
        
        
        log.debug("Push data into mongoDB")

        repo.dropCollection("yelp_business")
        repo.createCollection("yelp_business")
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:kzhang21_ryuc_zui_sarms#yelp_business', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:business.json', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_business = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_business, this_script)
        doc.usage(get_business, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        yelp_business = doc.entity('dat:kzhang21_ryuc_zui_sarms#yelp_business', {prov.model.PROV_LABEL:'Yelp Businesses', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(yelp_business, this_script)
        doc.wasGeneratedBy(yelp_business, get_business, endTime)
        doc.wasDerivedFrom(yelp_business, resource, get_business, get_business, get_business)

        repo.logout()
                  
        return doc

def request(host, path, api_key, url_params=None):
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }
    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()

def search(api_key, term, location):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

def query_api(term, location):
    response = search(API_KEY, term, location)

    businesses = response.get('businesses')

    return businesses[0]



## eof