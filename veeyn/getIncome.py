import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv

# some questions:
# what counts as non trivial transformation
# does execute count?
# what about my current script
# what about vee's 


def csv_to_json(url):
    file = urllib.request.urlopen(url).read().decode("utf-8")  # retrieve file from datamechanics.io
    finalJson = []
    entries = file.split('\n')

    # print(entries[0])
    val = entries[0].split('\r')  # retrieve column names for keys
    keys = val[0].split(',')
    for row in val[1:-1]:
        values = row.split(',')
        values[-1] = values[-1][:-1]
        dictionary = dict([(keys[i], values[i]) for i in range(len(keys))])
        finalJson.append(dictionary)
    print(finalJson)
    return finalJson


class getIncome(dml.Algorithm):
    contributor = 'dixyTW_veeyn'
    reads = ['dixyTW_veeyn.newBostonEthnicities']
    writes = ['dixyTW_veeyn.income']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('dixyTW_veeyn', 'dixyTW_veeyn')

        url = 'http://datamechanics.io/data/final2015.csv'
        natJson = csv_to_json(url)
        
        #print(json.dumps(reader[0],sort_key=True))
        # for row in reader:
        #     json.dumps(row, sort_key=True)

        #print(r)
        # Store Zip Code in DB
        #print(natJson)

        repo.dropCollection("income")
        repo.createCollection("income")
        repo['dixyTW_veeyn.income'].insert(natJson)
        repo['dixyTW_veeyn.income'].metadata({'complete':True})


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


        this_script = doc.agent('alg:dixyTW_veeyn#getIncome', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:final2015.csv', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_income, this_script)
        doc.usage(get_income, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )



        income = doc.entity('dat:dixyTW_veeyn#income', {prov.model.PROV_LABEL:'medium income across boston neighbors', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income, this_script)
        doc.wasGeneratedBy(income, get_income, endTime)
        doc.wasDerivedFrom(income, resource, get_income, get_income, get_income)

        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.



print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
getIncome.execute()
doc = getIncome.provenance()
print(doc.get_provn())
