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

        # transform it into a data frame for filtering
        health_pd = pd.DataFrame(health)

        # filter out rows that have no values
        health_pd = health_pd[pd.isnull(health_pd['tractfips']) == False]
        health_pd = health_pd[pd.isnull(health_pd['data_value']) == False]

        # get coordinates and append column to data frame
        result = []
        for x in range(len(health_pd)):
            result.append(health_pd['geolocation'].iloc[x]['coordinates'])
        health_pd['coordinates'] = result

        # select certain columns and rows with health outcomes
        health_pd = health_pd[['category', 'measure', 'data_value_type', 'data_value', 'tractfips',
                               'short_question_text', 'populationcount', 'coordinates']]
        health_pd = health_pd[health_pd['category'] == 'Health Outcomes']

        # find list of fips codes and create column for every health outcome
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

        # transform dictionary to data frame
        new_pd = pd.DataFrame.from_dict(new_dict, orient='index')
        new_pd = new_pd.fillna(0)
        
        repo.dropCollection("misn15.clean_health")
        repo.createCollection("misn15.clean_health")

        for x in new_pd.to_dict('records'):
            repo['misn15.clean_health'].insert_one(x)
            
        repo['misn15.clean_health'].metadata({'complete':True})
        print(repo['misn15.clean_health'].metadata())

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

        this_script = doc.agent('alg:misn15#cleanHealth', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:misn15#health', {'prov:label':'Health Data', prov.model.PROV_TYPE:'ont:DataSet'})

        get_cleanHealth = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_cleanHealth, this_script)
        doc.usage(get_cleanHealth, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation',
                   }
                  )

        clean_health = doc.entity('dat:misn15#clean_health', {prov.model.PROV_LABEL:'Health Data Cleaned', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(clean_health, this_script)
        doc.wasGeneratedBy(clean_health, get_cleanHealth, endTime)
        doc.wasDerivedFrom(clean_health, resource, get_cleanHealth, get_cleanHealth, get_cleanHealth)

        return doc


# cleanHealth.execute()
# doc = cleanHealth.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


# eof
