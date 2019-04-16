import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import numpy as np


class cleanHealth(dml.Algorithm):
    contributor = 'misn15'
    reads = ['misn15.health']
    writes = ['misn15.clean_health']

    @staticmethod
    def execute(trial = False):
        '''Transform health data for city of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        health = list(repo['misn15.health'].find())

        # transform it into a dataframe for filtering
        health_pd = pd.DataFrame(health)

        # filter out rows that have no values
        health_pd = health_pd[pd.isnull(health_pd['tractfips']) == False]
        health_pd = health_pd[pd.isnull(health_pd['data_value']) == False]

        # get coordinates and append column to dataframe
        result = []
        for x in range(len(health_pd)):
            result.append(health_pd['geolocation'].iloc[x]['coordinates'])
        health_pd['coordinates'] = result

        # select certain columns and rows with health outcomes
        health_pd = health_pd[['category', 'measure', 'data_value_type', 'data_value', 'tractfips',
                               'short_question_text', 'populationcount', 'coordinates']]
        health_pd = health_pd[health_pd['category'] == 'Health Outcomes']

        # find list of tractfips and create column for every health outcome
        tractfips = np.unique(health_pd['tractfips'])
        new_dict = {}
        for x in tractfips:
            for j in range(len(health_pd)):
                if health_pd.iloc[j]['tractfips'] == x:
                    if x not in new_dict.keys():
                        new_dict[x] = health_pd.iloc[j]['tractfips']
                        new_dict[x] = {}
                        new_dict[x]['tractfips'] = health_pd.iloc[j]['tractfips']
                        new_dict[x]['populationcount'] = health_pd.iloc[j]['populationcount']
                        new_dict[x]['coordinates'] = health_pd.iloc[j]['coordinates']
                    else:
                        if health_pd.iloc[j]['short_question_text'] not in new_dict[x].keys():
                            new_dict[x][health_pd.iloc[j]['short_question_text']] = health_pd.iloc[j]['data_value']

        # transform dict to dataframe
        new_pd = pd.DataFrame.from_dict(new_dict, orient='index')
        new_pd = new_pd.fillna(0)
        
        repo.dropCollection("misn15.clean_health")
        repo.createCollection("misn15.clean_health")

        for x in new_pd.to_dict('records'):
            repo['misn15.clean_health'].insert_one(x)
            
        repo['misn15.waste_health'].metadata({'complete':True})
        print(repo['misn15.waste_health'].metadata())

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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('health', 'http://datamechanics.io/') 
        doc.add_namespace('waste', 'http://datamechanics.io/data/misn15/hwgenids.json') 

        this_script = doc.agent('alg:misn15#transformHealth', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        health = doc.entity('dat:misn15.health', {'prov:label':'Health', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        waste = doc.entity('dat:misn15.waste', {'prov:label': 'Health', prov.model.PROV_TYPE: 'ont: DataResource', 'ont:Extension':'json'})                                              
        health_waste = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(health_waste, this_script)
        doc.usage(health_waste, health, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   }
                  )
        doc.usage(health_waste, waste, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   }
                  )

        health_data = doc.entity('dat:misn15#health', {prov.model.PROV_LABEL:'Boston Health', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(health_data, this_script)
        doc.wasGeneratedBy(health_data, health_waste, endTime)
        doc.wasDerivedFrom(health_data, health, health_waste, health_waste, health_waste)
           
      
        waste_data = doc.entity('dat:misn15#waste', {prov.model.PROV_LABEL:'Boston Waste', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(waste_data, this_script)
        doc.wasGeneratedBy(waste_data, health_waste, endTime)
        doc.wasDerivedFrom(waste_data, waste, health_waste, health_waste, health_waste)
                  
        return doc


cleanHealth.execute()
doc = cleanHealth.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
