import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sys
from .helper.statistics import statistics

class correlationsBetweenAgeAndWealth(dml.Algorithm):

    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.owner_num_properties_age']
    writes = ['asadeg02_gxy9598.age_wealth_correlations']

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

    def get_correlations(all_ages, all_num_properties):

        #age_ranges = correlationsBetweenAgeAndWealth.get_age_ranges(min(all_ages), max(all_ages), 10)        
        age_ranges = [(min(all_ages),35), (25,45), (20, 45), (35, 65), (40, 70), (30, 60), (40, max(all_ages)), (50, max(all_ages)), (30, max(all_ages))]
        res_dict = {}

        for range in age_ranges:

            age_num_prop = correlationsBetweenAgeAndWealth.get_ages_in_range(all_ages, all_num_properties, range)            
            if len(age_num_prop) > 0:
                ages = [age1 for (age1, num_prop1) in age_num_prop]
                num_props = [num_prop2 for (age2, num_prop2) in age_num_prop]                
                              
                key = str(range[0]) + '-' + str(range[1])
                res_dict[key] = {}                
                res_dict[key]['correlation'] = statistics.corr(ages,num_props)
                res_dict[key]['p-value'] = statistics.p(ages,num_props)
               
        return res_dict

    ##################################################################

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')
        repo.dropCollection("asadeg02_gxy9598.age_wealth_correlations")
        repo.createCollection("asadeg02_gxy9598.age_wealth_correlations")

        age_num_properties = repo.asadeg02_gxy9598.owner_num_properties_age.find()
        
        
        all_ages = []
        all_num_properties = []        
        for doc in age_num_properties:                             
            all_ages.append(doc["value"]["age"])
            all_num_properties.append(doc["value"]["num_properties"])
        
        
        correlations_dict = correlationsBetweenAgeAndWealth.get_correlations(all_ages, all_num_properties)

        for key in correlations_dict:
            correlations_dict[key]['_id'] = key
            repo['asadeg02_gxy9598.age_wealth_correlations'].insert_one(correlations_dict[key])


        result = {}
        result['_id'] = str(min(all_ages)) + '-' + str(max(all_ages))
        result['correlation_value'] = statistics.corr(all_ages,all_num_properties)
        result['p_value'] = statistics.p(all_ages,all_num_properties) 
        repo['asadeg02_gxy9598.age_wealth_correlations'].insert_one(result)       
        
        print('correlation coefficient between age and number of  peroperties in Boston is : ' + str(result['correlation_value']))
        print('p-value of correlation coefficient between age and number of  peroperties per owner in Boston is : ' + str(result['p_value']))
                
                
        repo['asadeg02_gxy9598.age_wealth_correlations'].metadata({'complete':True})
        print(repo['asadeg02_gxy9598.age_wealth_correlations'].metadata())
        print("Finish calculating correlation value for age and number of peroperties per owner.")
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

        this_script = doc.agent('alg:asadeg02_gxy9598#CorrelationsBetweenAgeAndWealth', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_owner_num_properties_age = doc.entity('dat:asadeg02_gxy9598#owner_num_properties_age', {prov.model.PROV_LABEL:'owner_num_properties_age', prov.model.PROV_TYPE:'ont:DataSet'})
        get_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Corellation between age and number of properties owned', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(get_correlation, this_script)
        doc.usage(get_correlation, resource_owner_num_properties_age , startTime)
        age_wealth_correlations = doc.entity('dat:asadeg02_gxy9598#age_wealth_correlations', {prov.model.PROV_LABEL:'Age Wealth Correlations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(age_wealth_correlations, this_script)
        doc.wasGeneratedBy(age_wealth_correlations, get_correlation, endTime)
        doc.wasDerivedFrom(age_wealth_correlations, resource_owner_num_properties_age, get_correlation, get_correlation, get_correlation)
        
        return doc       
