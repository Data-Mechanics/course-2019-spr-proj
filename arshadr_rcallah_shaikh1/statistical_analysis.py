import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from stat_functions import *


class statistical_analysis(dml.Algorithm):
    contributor = 'arshadr_rcallah_shaikh1'
    reads = ['arshadr_rcallah_shaikh1.unemployment', 'arshadr_rcallah_shaikh1.price_per_sqft_data']
    writes = ['arshadr_rcallah_shaikh1.statistical_analysis']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')
        print('Connected to DB')

        # get unemployment data
        cur = repo['arshadr_rcallah_shaikh1.unemployment'].find()
        json_dict = None
        for doc in cur:
            json_dict = doc
        del json_dict['_id']
        unemplorment_rates = json_to_df(json_dict)

        years = {}
        for i, row in unemplorment_rates.iterrows():
            if row['Year'] in range(2010, 2017)and row['Area '] == 'Chelsea':
                years[row['Year']] = row['Average Area Rate']
        # print('years', years)

        unemplorment_rates = years

        # get price per sqft data
        cur = repo['arshadr_rcallah_shaikh1.price_per_sqft_data'].find()
        for doc in cur:
            json_dict = doc
        del json_dict['_id']
        price_per_sqft_data = json_dict

        # remove years that are not between 2010-2016 inclusive
        del_keys = [key for key in price_per_sqft_data if key not in [str(i) for i in range(2010, 2017)]]
        for key in del_keys:
            del price_per_sqft_data[key]

        # print('zillow', price_per_sqft_data)

        # extract values
        x = [unemplorment_rates[i] for i in unemplorment_rates]
        y = [price_per_sqft_data[i] for i in price_per_sqft_data]

        stats = {}
        stats['dataset_x'] = 'arshadr_rcallah_shaikh1.unemployment'
        stats['dataset_y'] = 'arshadr_rcallah_shaikh1.price_per_sqft_data'
        stats['correlation_coefficient'] = corr(x,y)
        stats['p-value'] = p(x,y)

        # print (stats)


        repo.dropCollection("statistical_analysis")
        repo.createCollection("statistical_analysis")

        repo['arshadr_rcallah_shaikh1.statistical_analysis'].insert_one(stats)
        repo['arshadr_rcallah_shaikh1.statistical_analysis'].metadata({'complete': True})

        print(repo['arshadr_rcallah_shaikh1.permits'].metadata())

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
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('inc', 'https://drive.google.com/')
        doc.add_namespace('sas', 'https://chelseama.ogopendata.com/dataset/')
        doc.add_namespace('emp', 'https://chelseama.ogopendata.com/dataset/')

        this_script = doc.agent('alg:arshadr_rcallah_shaikh1#statistical_analysis',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('emp:file/d/1kiukuGI3Kl5qBzsKHOtCW17DdhaCLiMY/view',
                              {prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        resource = doc.entity('sas:building-permits-2016-onwards/resource/24a90fa2-d3b1-4857-acc1-fbcae3e2cc91',
                              {prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        resource = doc.entity('inc:chelsea-residents-incomes-in-the-past-12-months/resource/659c4348-6b39-4b79-8483-1f3ced5389c9',
            {prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_unemployment = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_homes = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_means = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_unemployment, this_script)
        doc.wasAssociatedWith(get_homes, this_script)
        doc.wasAssociatedWith(get_means, this_script)
        doc.usage(get_unemployment, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Retrieval': ''
                   }
                  )
        doc.usage(get_homes, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Retrieval': ''
                   }
                  )
        doc.usage(get_means, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Retrieval': ''
                   }
                  )
        unemployment = doc.entity('emp:arshadr_rcallah_shaikh1#unemployment',
                                {prov.model.PROV_LABEL: 'Unemployment Rates', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(unemployment, this_script)
        doc.wasGeneratedBy(unemployment, get_unemployment, endTime)
        doc.wasDerivedFrom(unemployment, resource, get_unemployment, get_unemployment, get_unemployment)

        homes = doc.entity('dat:arshadr_rcallah_shaikh1#statistical_analysis',
                             {prov.model.PROV_LABEL: 'Chelsea Homes', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(homes, this_script)
        doc.wasGeneratedBy(homes, get_homes, endTime)
        doc.wasDerivedFrom(homes, resource, get_homes, get_homes, get_homes)


        means = doc.entity('dat:arshadr_rcallah_shaikh1#income_data',
                             {prov.model.PROV_LABEL: 'Resident Incomes', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(means, this_script)
        doc.wasGeneratedBy(means, get_means, endTime)
        doc.wasDerivedFrom(means, resource, get_means, get_means, get_means)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
statistical_analysis.execute()
doc = statistical_analysis.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof