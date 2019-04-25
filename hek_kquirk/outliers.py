import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import re
from sklearn.linear_model import LinearRegression
import numpy as np
from ast import literal_eval
from scipy.cluster.vq import vq, kmeans, whiten


class outliers(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = ['hek_kquirk.per_capita_income','hek_kquirk.education_level','hek_kquirk.race_and_ethnicity']
    writes = ['hek_kquirk.outliers']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

         # Drop/recreate mongo collection
        repo.dropCollection("outliers")
        repo.createCollection("outliers")

        # dictionary to insert
        outliers_dict = {'_id':'outliers_dictionary'}

        # INCOME OUTLIERS
        incidents_per_dis = repo['hek_kquirk.per_capita_income'].find({})
        incident_data = [(i['Neighborhood'], float(re.sub('[^0-9]','',i['Per Capita Income']))) for i in incidents_per_dis]
        incident_data = incident_data[2:]

        incident_x = [i[1] for i in incident_data]

        # IQR data
        # find IQR for race and ethinicity
        incident_x_sort = sorted(incident_x)
        q1, q3= np.percentile(incident_x_sort,[25,75])
        iqr = q3 - q1
        lower_bound = q1 -(1.5 * iqr) 
        upper_bound = q3 +(1.5 * iqr)

        # District with highest arrest count
        outliers = []
        for neighborhood in incident_data:
            if neighborhood[1] > upper_bound or neighborhood[1] < lower_bound:
                outliers.append(district)

        outliers_dict['income_outliers'] = {'lower_bound':lower_bound, 'upper_bound':upper_bound, 'outliers':outliers}

        # EDUCATION OUTLIERS
        incidents_per_dis = repo['hek_kquirk.education_level'].find({})
        incident_data = [(i['Neighborhood'], float(re.sub('[^0-9]','',i['%']))) for i in incidents_per_dis]
        incident_data = incident_data[2:]

        # percentage 
        incident_x = [i[1] for i in incident_data]

        # IQR data
        # find IQR for race and ethinicity
        incident_x_sort = sorted(incident_x)
        q1, q3= np.percentile(incident_x_sort,[25,75])
        iqr = q3 - q1
        lower_bound = q1 -(1.5 * iqr) 
        upper_bound = q3 +(1.5 * iqr)

        # District with highest arrest count
        outliers = []
        for neighborhood in incident_data:
            if neighborhood[1] > upper_bound or neighborhood[1] < lower_bound:
                outliers.append(district)
                
        outliers_dict['education_outliers'] = {'lower_bound':lower_bound, 'upper_bound':upper_bound, 'outliers':outliers}

         # RACE OUTLIERS
        incidents_per_dis = repo['hek_kquirk.race_and_ethnicity'].find({})
        incident_data = [(i['Neighborhood'], float(re.sub('[^0-9]','',i['% Non-Hispanic White']))) for i in incidents_per_dis]
        incident_data = incident_data[2:]

        # percentage 
        incident_x = [i[1] for i in incident_data]

        # IQR data
        # find IQR for race and ethinicity
        incident_x_sort = sorted(incident_x)
        q1, q3= np.percentile(incident_x_sort,[25,75])
        iqr = q3 - q1
        lower_bound = q1 -(1.5 * iqr) 
        upper_bound = q3 +(1.5 * iqr)

        # District with highest arrest count
        outliers = []
        for neighborhood in incident_data:
            if neighborhood[1] > upper_bound or neighborhood[1] < lower_bound:
                outliers.append(district)
                
        outliers_dict['race_ethnicity_outliers'] = {'lower_bound':lower_bound, 'upper_bound':upper_bound, 'outliers':outliers}

        repo['hek_kquirk.outliers'].insert(outliers_dict)
        repo['hek_kquirk.outliers'].metadata({'complete':True})
        print(repo['hek_kquirk.outliers'].metadata())
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
        repo.authenticate('hek_kquirk', 'hek_kquirk')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:hek_kquirk#outliers', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_per_capita_income = doc.entity('dat:hek_kquirk#per_capita_income', {'prov:label':'Boston neighborhoods to per capita income', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_education_level = doc.entity('dat:hek_kquirk#education_level', {'prov:label':'Boston neighborhoods to education', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_race_and_ethnicity = doc.entity('dat:hek_kquirk#race_and_ethnicity', {'prov:label':'Boston neighborhoods to race and ethnicity', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_per_capita_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_education_level = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_race_and_ethnicity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_per_capita_income, this_script)
        doc.wasAssociatedWith(get_education_level, this_script)
        doc.wasAssociatedWith(get_race_and_ethnicity, this_script)
        
        doc.usage(get_per_capita_income, resource_per_capita_income, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        doc.usage(get_education_level, resource_education_level, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        doc.usage(get_race_and_ethnicity, resource_race_and_ethnicity, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        
        outliers = doc.entity('dat:hek_kquirk#outliers', {prov.model.PROV_LABEL:'Outliers of economic, race, and education level', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(outliers, this_script)
        
        doc.wasGeneratedBy(outliers, get_per_capita_income, endTime)
        doc.wasGeneratedBy(outliers, get_education_level, endTime)
        doc.wasGeneratedBy(outliers, get_race_and_ethnicity, endTime)
        
        doc.wasDerivedFrom(outliers, resource_per_capita_income, get_per_capita_income, get_per_capita_income, get_per_capita_income)
        doc.wasDerivedFrom(outliers, resource_education_level, get_education_level, get_education_level, get_education_level)
        doc.wasDerivedFrom(outliers, resource_race_and_ethnicity, get_race_and_ethnicity, get_race_and_ethnicity, get_race_and_ethnicity)

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

## eof