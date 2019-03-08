# -*- coding: utf-8 -*-
"""
Yelp Fusion API code sample.
This program demonstrates the capability of the Yelp Fusion API
by using the Search API to query for businesses by a search term and location,
and the Business API to query additional information about the top result
from the search query.
Please refer to http://www.yelp.com/developers/v3/documentation for the API
documentation.
This program requires the Python requests library, which you can install via:
`pip install -r requirements.txt`.
Sample usage of the program:
`python sample.py --term="bars" --location="San Francisco, CA"`
"""

#fetch top 100 restuarants in boston 
#location of restaurants, rating, review count,
#fetch data for calculating a restaurants competitiveness


from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import urllib
import dml
import uuid
import prov.model
import datetime


# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode


# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app
API_KEY= "naaEMcM9ma55gHjnGirAk1sfK5DzQfEFsTBEVEmrnBra12rCjpXLkS2a2LqpH4JonTPm7WD-J4kTXZx2km380QSaQqHipjfou26Yf91YUH1o_VsdsSDjtDEo8919XHYx"


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.


# Defaults for our simple example.
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'Boston, MA'
SEARCH_LIMIT = 50
mainResult = []

def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        dict: The JSON response from the request.
    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)
    

    return response.json()


def search(api_key, term, location):
    """Query the Search API by a search term and location.
    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.
    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def get_business(api_key, business_id):
    """Query the Business API by a business ID.
    Args:
        business_id (str): The ID of the business to query.
    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)


def query_api(term, location):
    """Queries the API by the input values from the user.
    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    response = search(API_KEY, term, location)

    businesses = response.get('businesses')

    if not businesses:
        print(u'No businesses for {0} in {1} found.'.format(term, location))
        return
    business_idlist = [] #list of business id
    for x in range(len(businesses)):
        business_idlist.append(businesses[x]['id'])

    business_id = businesses[0]['id']

    # print(u'{0} businesses found, querying business info ' \
    #     'for the top result "{1}" ...'.format(
    #         len(businesses), business_id))

    #prints top3 restaurants out of the 50 restaurants from the list
    for i in range(10):
        response = get_business(API_KEY, business_idlist[i])

        #print(u'Result for business "{0}" found:'.format(business_id))
        #pprint.pprint(response, indent=2)
        #print(response)
        s = json.dumps(response)
        mainResult.append(json.loads(s))
            #dumps returns string, outfile.write(json.dumps(response))


def main():
    # parser = argparse.ArgumentParser()

    # parser.add_argument('-q', '--term', dest='term', default=DEFAULT_TERM,
    #                     type=str, help='Search term (default: %(default)s)')
    # parser.add_argument('-l', '--location', dest='location',
    #                     default=DEFAULT_LOCATION, type=str,
    #                     help='Search location (default: %(default)s)')

    # input_values = parser.parse_args()

    try:
        query_api(DEFAULT_TERM, DEFAULT_LOCATION)
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )

class TransformFusion(dml.Algorithm):
    contributor = 'dixyTW_veeyn'
    reads = []
    writes = ['dixyTW_veeyn.FusionTransform']

    @staticmethod
    def execute(trial = False):
        ' Merge data sets'
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('dixyTW_veeyn', 'dixyTW_veeyn')
        main() #fills out mainResult with Restaurant Data
        repo.dropCollection("dixyTW_veeyn.FusionTransform")
        repo.createCollection("dixyTW_veeyn.FusionTransform")
        #print(mainResult)
        repo['dixyTW_veeyn.FusionTransform'].insert_many(mainResult)
        repo['dixyTW_veeyn.FusionTransform'].metadata({'complete':True})
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    



    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('dixyTW_veeyn', 'dixyTW_veeyn')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://api.yelp.com')

        this_script = doc.agent('alg:dixyTW_veeyn#TransformFusion', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        TransformFusion = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(TransformFusion, this_script)
        doc.usage(TransformFusion, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )


        FusionTransform = doc.entity('dat:dixyTW_veeyn#FusionTransform', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(FusionTransform, this_script)
        doc.wasGeneratedBy(FusionTransform, TransformFusion, endTime)
        doc.wasDerivedFrom(FusionTransform, resource, TransformFusion, TransformFusion, TransformFusion)


        repo.logout()
                  
        return doc

TransformFusion.execute()
