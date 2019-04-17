import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import pandas as pd

class income_data(dml.Algorithm):
    contributor = 'arshadr_rcallah_shaikh1'
    reads = []
    writes = ['arshadr_rcallah_shaikh1.income_data']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        opener = urllib.request.build_opener()
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib.request.install_opener(opener)

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')
        print('Connected')

        url = 'https://chelseama.ogopendata.com/dataset/1305b4ca-7b0f-49fe-b215-7882d223b5f6/resource/659c4348-6b39-4b79-8483-1f3ced5389c9/download/income-in-the-past-12-months.xls'

        df = pd.read_excel(url)

        region_of_interest = df.iloc[5:27, :13]
        median_income_households = df.iloc[20, 3]
        (less_ten, ten_onefive, onefive_twofive, twofive_threefive, threefive_fivezero, fivezero_sevenfive,
         sevenfive_hundred, hundred_onefifty, onefifty_twohundred, over_twohundred) = [df.iloc[i, 3] for i in range(9, 19)]

        dists = ['less_ten', 'ten_onefive', 'onefive_twofive', 'twofive_threefive', 'threefive_fivezero', 'fivezero_sevenfive', 'sevenfive_hundred', 'hundred_onefifty', 'onefifty_twohundred', 'over_twohundred']
        dists_percent = (less_ten, ten_onefive, onefive_twofive, twofive_threefive, threefive_fivezero, fivezero_sevenfive,
         sevenfive_hundred, hundred_onefifty, onefifty_twohundred, over_twohundred)
        dat = {}
        for i in range(len(dists)):
            dat[dists[i]] = dists_percent[i]

        dat['median_income_households'] = median_income_households

        repo.dropCollection("income_data")
        repo.createCollection("income_data")

        repo['arshadr_rcallah_shaikh1.income_data'].insert_one(dat)
        repo['arshadr_rcallah_shaikh1.income_data'].metadata({'complete':True})
        print(repo['arshadr_rcallah_shaikh1.income_data'].metadata())


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
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('inc', 'https://chelseama.ogopendata.com/dataset/1305b4ca-7b0f-49fe-b215-7882d223b5f6/resource/659c4348-6b39-4b79-8483-1f3ced5389c9/download/income-in-the-past-12-months.xls')

        this_script = doc.agent('alg:arshadr_rcallah_shaikh1#income_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('inc:research/data/', { prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_income, this_script)
        doc.usage(get_income, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Retrieval':''
                   }
                  )

        income = doc.entity('inc:research/data/', {prov.model.PROV_LABEL:'median_income_households', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income, this_script)
        doc.wasGeneratedBy(income, get_income, endTime)
        doc.wasDerivedFrom(income, resource, get_income, get_income, get_income)

        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
income_data.execute()

doc = income_data.provenance()
print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof