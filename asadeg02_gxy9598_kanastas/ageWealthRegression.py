import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sys
from .helper.regression import regression as regr

class ageWealthRegression(dml.Algorithm):

    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.owner_num_properties_age']
    writes = ['asadeg02_gxy9598.age_wealth_regr']

    def get_ages_in_range(ages, num_properties, range):

        result = []        
        for (age,num_prop) in zip(ages, num_properties):
            if age >= range[0] and age <= range[1]:
                result.append((age, num_prop))
        return result
    ############################################################# 

    def get_age_ranges(min, max, num_ranges): 
       
        step = (max - min)//num_ranges
        temp_min = min
        age_ranges = []
        for i in range(0, num_ranges - 1):
            print(temp_min, temp_min + step)
            age_ranges.append((temp_min, temp_min + step))
            temp_min += step
        age_ranges.append((temp_min, max))
        return age_ranges

    ################################################################

    def get_regr(all_ages, all_num_properties):

        #age_ranges = ageWealthRegression.get_age_ranges(min(all_ages), max(all_ages), 10)        
        age_ranges = [(min(all_ages),max(all_ages)),(min(all_ages),35), (25,45), (20, 45), (35, 65), (40, 70), (30, 60), (40, max(all_ages)), (50, max(all_ages)), (30, max(all_ages))]
        res_dict = {}

        i = 2
        for range in age_ranges:

            age_num_prop = ageWealthRegression.get_ages_in_range(all_ages, all_num_properties, range)            
            if len(age_num_prop) > 0:
                ages = [age1 for (age1, num_prop1) in age_num_prop]
                num_props = [num_prop2 for (age2, num_prop2) in age_num_prop]                
                              
                key = str(range[0]) + '-' + str(range[1])
                res_dict[key] = {}                              
                res_dict[key]['linear_model'] = regr.getRegressionResults(ages, num_props, "age-wealth-linear-model-for-range" + key, i)
                i += 1
                
               
        return res_dict

    ##################################################################

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')
        repo.dropCollection("asadeg02_gxy9598.age_wealth_regr")
        repo.createCollection("asadeg02_gxy9598.age_wealth_regr")

        age_num_properties = repo.asadeg02_gxy9598.owner_num_properties_age.find()
        
        
        all_ages = []
        all_num_properties = []        
        for doc in age_num_properties:                             
            all_ages.append(doc["value"]["age"])
            all_num_properties.append(doc["value"]["num_properties"])
        
        
        regr_dict = ageWealthRegression.get_regr(all_ages, all_num_properties)

        for key in regr_dict:
            regr_dict[key]['_id'] = key
            repo['asadeg02_gxy9598.age_wealth_regr'].insert_one(regr_dict[key])               
        

        repo['asadeg02_gxy9598.age_wealth_regr'].metadata({'complete':True})
        print(repo['asadeg02_gxy9598.age_wealth_regr'].metadata())
        print("Finish calculating linear cofficients between age and number of properties per owner.")
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:asadeg02_gxy9598#ageWealthRegression', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_owner_num_properties_age = doc.entity('dat:asadeg02_gxy9598#owner_num_properties_age', {prov.model.PROV_LABEL:'owner_num_properties_age', prov.model.PROV_TYPE:'ont:DataSet'})
        ageWealthRegression = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Finds the best fit linear model between age and wealth', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(ageWealthRegression, this_script)
        doc.usage(ageWealthRegression, resource_owner_num_properties_age , startTime)
        age_wealth_regr = doc.entity('dat:asadeg02_gxy9598#age_wealth_regr', {prov.model.PROV_LABEL:'Best Fit Linear Model Between Age And Wealth', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(age_wealth_regr, this_script)
        doc.wasGeneratedBy(age_wealth_regr, ageWealthRegression, endTime)
        doc.wasDerivedFrom(age_wealth_regr, resource_owner_num_properties_age, ageWealthRegression, ageWealthRegression, ageWealthRegression)
        
        return doc       
