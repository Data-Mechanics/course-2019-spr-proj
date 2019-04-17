import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import pandas as pd
import io
from statistics import *


class price_per_sqft_data(dml.Algorithm):
    contributor = 'arshadr_rcallah_shaikh1'
    reads = []
    writes = ['arshadr_rcallah_shaikh1.price_per_sqft_data']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')

        url = 'http://files.zillowstatic.com/research/public/City/City_ZriPerSqft_AllHomes.csv'
        s = requests.get(url).content                           # get content
        df = pd.read_csv(io.StringIO(s.decode('ISO-8859-1')))   # decode csv
        df = df.loc[(df['RegionName'] == 'Chelsea') & (df['State'] == 'MA')]    # filter based on only chelsea ma
        df = df.drop(['RegionID', 'RegionName', 'State', 'Metro', 'CountyName', 'SizeRank'], axis=1) # remove irrelevant cols

        years = {} # dictionary of avg zri per year
        for col in df.columns:  # change from per month to per year
            year = col[:4]
            if year in years:
                years[year].append(df.iloc[0][col])
            else:
                years[year] = [df.iloc[0][col]]

        for year in years:  # calculate average zri per year
            years[year] = mean(years[year])

        repo.dropCollection("price_per_sqft_data")
        repo.createCollection("price_per_sqft_data")

        repo['arshadr_rcallah_shaikh1.price_per_sqft_data'].insert_one(years)
        repo['arshadr_rcallah_shaikh1.price_per_sqft_data'].metadata({'complete': True})
        print(repo['arshadr_rcallah_shaikh1.price_per_sqft_data'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('drv', 'https://drive.google.com/')
        doc.add_namespace('sqf', 'http://files.zillowstatic.com/research/public/City/City_ZriPerSqft_AllHomes.csv')


        this_script = doc.agent('alg:arshadr_rcallah_shaikh1#price_per_sqft_data',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('drv:http://files.zillowstatic.com/research/public/City/City_ZriPerSqft_AllHomes.cs',
                              {prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_sqft = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_sqft, this_script)

        doc.usage(get_sqft, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Retrieval': ''
                   }
                  )
        sqft = doc.entity('dat:arshadr_rcallah_shaikh1#price_per_sqft_data',
                                {prov.model.PROV_LABEL: 'Assessors Office', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(sqft, this_script)
        doc.wasGeneratedBy(sqft, get_sqft, endTime)
        doc.wasDerivedFrom(sqft, resource, get_sqft, get_sqft, get_sqft)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
price_per_sqft_data.execute()
doc = price_per_sqft_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof