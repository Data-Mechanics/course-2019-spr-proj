import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class accidents(dml.Algorithm):
    contributor = 'jkmoy_mfflynn'
    reads = []
    writes = ['jkmoy_mfflynn.fatal_accident', 'jkmoy_mfflynn.accident' ]

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jkmoy_mfflynn', 'jkmoy_mfflynn')

        #Fatal car crashes
        url = 'https://data.boston.gov/datastore/odata3.0/92f18923-d4ec-4c17-9405-4e0da63e1d6c?$format=json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        r = r['value']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("jkmoy_mfflynn.fatal_accident")
        repo.createCollection("jkmoy_mfflynn.fatal_accident")
        repo['jkmoy_mfflynn.fatal_accident'].insert_many(r)
        repo['jkmoy_mfflynn.fatal_accident'].metadata({'complete':True})

        print(repo['fatal_accident'].metadata())

        #All car crashes from data mechanics
        # https://data.boston.gov/datastore/odata3.0/e4bfe397-6bfc-49c5-9367-c879fac7401d?$format=json
        #url = 'http://datamechanics.io/data/boston_car_accidents.json'
        '''
        url = 'http://datamechanics.io/data/carAccidents.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        r = r['value']
        s = json.dumps(r, sort_keys=True, indent=2)
        '''
        
        url = 'http://datamechanics.io/data/accidents.csv'
        df = pd.read_csv(url)
        # dicts where vals = headers and keys = list of all values for that header
        d = df.to_dict()  #orient = 'records'
        l = list(d) # list of all field headers
        finalSet = []
        for i in range(17912):
            # Convert float nan values to empty string
            # ternary if
            dts = d['dispatch_ts'][i] if str(d['dispatch_ts'][i]) != 'nan' else ''
            mt = d['mode_type'][i] if str(d['mode_type'][i]) != 'nan' else ''
            lt = d['location_type'][i] if str(d['location_type'][i]) != 'nan' else ''
            s = d['street'][i] if str(d['street'][i]) != 'nan' else ''
            xs1 = d['xstreet1'][i] if str(d['xstreet1'][i]) != 'nan' else ''
            xs2 = d['xstreet2'][i] if str(d['xstreet2'][i]) != 'nan' else ''
            xc = d['x_cord'][i] if str(d['x_cord'][i]) != 'nan' else ''
            yc = d['y_cord'][i] if str(d['y_cord'][i]) != 'nan' else ''
            la = d['lat'][i] if str(d['lat'][i]) != 'nan' else ''
            lo = d['long'][i] if str(d['long'][i]) != 'nan' else ''

            e = {'dispatch_ts':dts,
                 'mode_type':mt,
                 'location_type':lt,
                 'street':s,
                 'xstreet1':xs1,
                 'xstreet2':xs2,
                 'x_cord':xc,
                 'y_cord':yc,
                 'lat':la,
                 'long':lo }
            finalSet.append(e)
        
        repo.dropCollection("jkmoy_mfflynn.accident")
        repo.createCollection("jkmoy_mfflynn.accident")
        repo['jkmoy_mfflynn.accident'].insert_many(finalSet) #r or finalSet
        repo['jkmoy_mfflynn.accident'].metadata({'complete':True})

        print(repo['jkmoy_mfflynn.accident'].metadata())

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
        repo.authenticate('jkmoy_mfflynn', 'jkmoy_mfflynn')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:jkmoy_mfflynn#accidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:accidents', {'prov:label':'Car Accidents', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_fatal_accidents = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_accidents = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_fatal_accidents, this_script)
        doc.wasAssociatedWith(get_accidents, this_script)
        doc.usage(get_fatal_accidents, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_accidents, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        fatal_accidents = doc.entity('dat:jkmoy_mfflynn#fatal_accidents', {prov.model.PROV_LABEL:'Deadly Accidents', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fatal_accidents, this_script)
        doc.wasGeneratedBy(fatal_accidents, get_fatal_accidents, endTime)
        doc.wasDerivedFrom(fatal_accidents, resource, get_fatal_accidents, get_fatal_accidents, get_fatal_accidents)

        accidents = doc.entity('dat:jkmoy_mfflynn#accidents', {prov.model.PROV_LABEL:'Non-deadly Accidents', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(accidents, this_script)
        doc.wasGeneratedBy(accidents, get_accidents, endTime)
        doc.wasDerivedFrom(accidents, resource, get_accidents, get_accidents, get_accidents)

        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
#accidents.execute()

## eof
