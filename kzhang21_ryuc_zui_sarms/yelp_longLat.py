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

class yelp_longLat(dml.Algorithm):
    contributor = 'kzhang21_ryuc_zui_sarms'
    reads = ['kzhang21_ryuc_zui_sarms.food_violations', 'kzhang21_ryuc_zui_sarms.yelp_business']
    writes = ['kzhang21_ryuc_zui_sarms.yelp_longLat']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kzhang21_ryuc_zui_sarms', 'kzhang21_ryuc_zui_sarms')

        violationData = pd.DataFrame(repo.kzhang21_ryuc_zui_sarms.food_violations.find({"location": float("nan")}))
        
        for index,row in violationData.iterrows():
            if pd.isnull(row['location']) or row['location'] is None:
                address = row['address'] + ", " + row['city'] + ", " + row['state']
                address =' '.join(address.split())
                print(geocoding(address))
                addr, lat, lgn = geocoding(address)
                row["location"] = (lat, lgn)
                row["address"] = addr

                repo.kzhang21_ryuc_zui_sarms.food_violations.replace_one({"_id": row["_id"]}, row.to_dict(), upsert=True)


        
        log.debug("Push data into mongoDB")

        repo.dropCollection("food_violations")
        repo.createCollection("food_violations")
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

        this_script = doc.agent('alg:kzhang21_ryuc_zui_sarms#yelp_longLat', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:business.json', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_business = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_business, this_script)
        doc.usage(get_business, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        yelp_longLat = doc.entity('dat:kzhang21_ryuc_zui_sarms#yelp_longLat', {prov.model.PROV_LABEL:'Yelp Long Lat', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(yelp_longLat, this_script)
        doc.wasGeneratedBy(yelp_longLat, get_business, endTime)
        doc.wasDerivedFrom(yelp_longLat, resource, get_business, get_business, get_business)

        repo.logout()
                  
        return doc


def geocoding(address):
    """
    Take in an address and return the proper address and the coordinate tuple
    """
    AUTH = json.loads(open("auth.json", "r").read())

    r = requests.get(f"https://maps.googleapis.com/maps/api/geocode/json", params={
        "address": address,
        "key": AUTH["GMAP_API"]
    })

    if r.status_code == 200:
        r = r.json()
        log.debug(r)
        results = r["results"]
        if len(results) < 1:
            log.error("No result geocoding for %s", address)
            return (address, -1, -1)

        result = results[0]
        proper_address = result["formatted_address"]
        loc = result["geometry"]["location"]
        lat = loc["lat"]
        lng = loc["lng"]

        return (proper_address, lat, lng)

    else:
        log.error("Error in Geocoding %s", address)
        return (address, -1, -1)

# viola_longLat.execute()
# doc = viola_longLat.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))