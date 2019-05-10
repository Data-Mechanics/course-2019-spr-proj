import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class gather_data(dml.Algorithm):
    contributor = 'aqu1'
    reads = ['aqu1.education_data', 'aqu1.income_data', 'aqu1.correlation']
    writes = ['aqu1.schools_data']
     
    def combine(school_stop, education, income):
        school_stop = school_stop.drop(['_id'], axis = 1)
        school_stop = school_stop[['Neighborhood', 'Distance']]
        school_stop['Distance'] = school_stop['Distance']. apply(lambda d: round(float(d), 2))
        no_dist = pd.DataFrame({'Neighborhood': ['Harbor Islands', 'Longwood Medical Area', 'Mission Hill',
                                                  'South Boston Waterfront', 'West End'],
                                 'Distance': ['N/A', 'N/A', 'N/A', 'N/A', 'N/A',]})
        school_stop = school_stop.append(no_dist)
        school_stop = school_stop.sort_values('Neighborhood')
        education = education.drop(['Decade', 'Education','Number of People', '_id'], axis = 1)
        education['Neighborhood'] = education['Neighborhood'].replace('Mission Hill ', 'Mission Hill')
        education = education.rename(index = str, columns = {'Percent of Population': 'Percent Bachelor\'s Degree'})
        income = income.drop(['AREA_ACRES', 'AREA_SQFT', 'FID', 'GEOID10', 'HU100_RE', 'LEP',
                               'Low_to_No', 'MedIllnes', 'OlderAdult', 'POC2', 'POP100_RE', 
                               'Shape__Area', 'Shape__Length', 'TotChild', 'TotDis', '_id'], axis = 1)
        income = income[income['Neighborhood'] != 'Bay Village']
        income = income[income['Neighborhood'] != 'Leather District']
        income['prop_low_income'] = income['prop_low_income'].apply(lambda p: round(p*100, 2))
        income = income.rename(index = str, columns = {'prop_low_income': 'Percent Low Income'})
        no_data = pd.DataFrame({'Neighborhood': ['Beacon Hill', 'Downtown'],
                                'Percent Low Income': ['N/A', 'N/A']})
        income = income.append(no_data)
                                
        combined_data = school_stop.merge(education, on = 'Neighborhood').merge(income, on = 'Neighborhood')
        
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys = True, indent = 2)
    
        boundaries = []
        for row in r['features']:
            bounds = {}
            bounds['Neighborhood'] = row['properties']['Name']
            bounds['geometry'] = row['geometry']
            
            data_row = combined_data.loc[combined_data['Neighborhood'] == bounds['Neighborhood']].values.tolist()
            
            if data_row:
                bounds['Distance'] = data_row[0][1]
                bounds['Percent Bachelor\'s Degree'] = data_row[0][2]
                bounds['Percent Low Income'] = data_row[0][3]
            else:
                bounds['Distance'] = 'N/A'
                bounds['Percent Bachelor\'s Degree'] = 'N/A'
                bounds['Percent Low Income'] = 'N/A'
            
            boundaries.append(bounds.copy())
        
        return boundaries
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aqu1', 'aqu1')
        
        school_stop = repo.aqu1.correlation.find()
        school_stop = pd.DataFrame(school_stop)

        education = repo.aqu1.education_data.find()
        education = pd.DataFrame(education)
        
        income = repo.aqu1.income_data.find()
        income = pd.DataFrame(income)
        
        boundaries = gather_data.combine(school_stop, education, income)
        boundaries = pd.DataFrame(boundaries)
        boundaries = json.loads(boundaries.to_json(orient = 'records'))
     
        repo.dropCollection("boundaries")
        repo.createCollection("boundaries")
         
        repo['aqu1.gather_data'].insert_many(boundaries)
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
        
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aqu1', 'aqu1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('dm','http://datamechanics.io/data/aqu1')
        
        this_script = doc.agent('alg:aqu1#gather_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_data = doc.entity('dm:gather_data', {'prov:label':'Gather Data for Map', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_data, this_script)
        doc.usage(get_data, resource_data, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        gather_data = doc.entity('dat:aqu1#gatherdata', {prov.model.PROV_LABEL:'Gather Data for Map', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(gather_data, this_script)
        doc.wasGeneratedBy(gather_data, get_data, endTime)
        doc.wasDerivedFrom(gather_data, resource_data, get_data, get_data, get_data)
        doc.wasDerivedFrom(gather_data, resource_data, get_data, get_data, get_data)
        
        repo.logout()
        return doc