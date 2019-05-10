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


class crime_incidents_stats(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = ['hek_kquirk.incidents_per_district']
    writes = ['hek_kquirk.crime_incidents_stats']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        # Drop/recreate mongo collection
        repo.dropCollection("crime_incidents_stats")
        repo.createCollection("crime_incidents_stats")

        # create dictionary 
        stats_dict = {'_id':'incident_statistics_dictionary'}

        # RACE AND ETHNICITY DATA
        # Get crime incident locations and convert to float tuples
        incidents_per_dis = repo['hek_kquirk.incidents_per_district'].find({})
        incident_data = [(i['value']['Count'], i['race_and_ethnicity'][0]['value']['Non_Hispanic_White'], i['race_and_ethnicity'][0]['value']['Total_Population'], i['_id']) for i in incidents_per_dis]
        incident_data = incident_data[1:]

        # incident count per district
        incident_y = [[float(re.sub('[^0-9]','',i[0]))] if isinstance(i[0], str) else [float(i[0])] for i in incident_data]
        incident_y_sort = sorted([[float(re.sub('[^0-9]','',i[0]))] if isinstance(i[0], str) else [float(i[0])] for i in incident_data])

        # percentage of white non-hispanics in district
        incident_x = [float(re.sub('[^0-9]','',i[1])) / float(re.sub('[^0-9]','',i[2])) * 100 if isinstance(i[1], str) else float(i[1]) / float(i[2]) * 100 for i in incident_data]

        reg = LinearRegression().fit(incident_y, incident_x)
        reg_score = reg.score(incident_y, incident_x)

        # IQR data
        # find IQR for race and ethinicity
        incident_x_sort = sorted(incident_x)
        q1, q3= np.percentile(incident_x_sort,[25,75])
        iqr = q3 - q1
        lower_bound = q1 -(1.5 * iqr) 
        upper_bound = q3 +(1.5 * iqr)

        # find IQR for incidents
        q1, q3= np.percentile(incident_y_sort,[25,75])
        iqr = q3 - q1
        lower_bound_y = q1 -(1.5 * iqr) 
        upper_bound_y = q3 +(1.5 * iqr)

        # District with highest arrest count
        max = ('',0,'','','')
        min = ('',999999999,'','','')
        outliers = []
        outliers_y = []
        for district in incident_data:
            percent_white = float(re.sub('[^0-9]','',district[1])) / float(re.sub('[^0-9]','',district[2])) * 100 if isinstance(district[1], str) else float(district[1]) / float(district[2]) * 100
            if district[0] > max[1]:
                max = (district[3],district[0],percent_white,'','')
            if district[0] < min[1]:
                min = (district[3],district[0],percent_white,'','')
            if percent_white > upper_bound or percent_white < lower_bound:
                outliers.append(district)
            if district[0] > upper_bound_y or district[0] < lower_bound_y:
                outliers_y.append(district)
            
        stats_dict['race_and_ethnicity'] = {'correlation_coeff':reg_score, 'lower_bound':lower_bound, 'upper_bound':upper_bound, 'outliers':outliers}
    
        stats_dict['incident_count'] = {'lower_bound':lower_bound_y, 'upper_bound':upper_bound_y, 'outliers':outliers_y}

        ###########
        # INCOME
        incidents_per_dis = repo['hek_kquirk.incidents_per_district'].find({})
        incident_data = [(i['value']['Count'], i['income_data'][0]['value']['per_capita_income'], i['_id']) for i in incidents_per_dis]
        incident_data = incident_data[1:]

        incident_y = [[i[0]] for i in incident_data]
        incident_x = [i[1] for i in incident_data]
        reg = LinearRegression().fit(incident_y, incident_x)
        reg_score = reg.score(incident_y, incident_x)

        # find IQR for race and ethinicity
        incident_x_sort = sorted(incident_x)
        q1, q3= np.percentile(incident_x_sort,[25,75])
        iqr = q3 - q1
        lower_bound = q1 -(1.5 * iqr) 
        upper_bound = q3 +(1.5 * iqr)

        outliers = []
        for district in incident_data:
            if district[1] > upper_bound or district[1] < lower_bound:
                outliers.append(district)
            if max[0] == district[2]:
                max = (max[0],max[1],max[2],district[1],'')
            if min[0] == district[2]:
                min = (min[0],min[1],min[2],district[1],'')
        
        stats_dict['income'] = {'correlation_coeff':reg_score, 'lower_bound':lower_bound, 'upper_bound':upper_bound, 'outliers':outliers}

        ############
        # Education
        incidents_per_dis = repo['hek_kquirk.incidents_per_district'].find({})
        incident_data = [(i['value']['Count'], i['education_data'][0]['value']['percent'], i['_id']) for i in incidents_per_dis]
        incident_data = incident_data[1:]

        incident_y = [[i[0]] for i in incident_data]
        incident_x = [i[1] for i in incident_data]
        reg = LinearRegression().fit(incident_y, incident_x)
        reg_score = reg.score(incident_y, incident_x)

        # find IQR for race and ethinicity
        incident_x_sort = sorted(incident_x)
        q1, q3= np.percentile(incident_x_sort,[25,75])
        iqr = q3 - q1
        lower_bound = q1 -(1.5 * iqr) 
        upper_bound = q3 +(1.5 * iqr)

        outliers = []
        for district in incident_data:
            if district[1] > upper_bound or district[1] < lower_bound:
                outliers.append(district)
            if max[0] == district[2]:
                max = (max[0],max[1],max[2],max[3],district[1])
            if min[0] == district[2]:
                min = (min[0],min[1],min[2],min[3],district[1])

        stats_dict['race_and_ethnicity'] = {'correlation_coeff':reg_score, 'lower_bound':lower_bound, 'upper_bound':upper_bound, 'outliers':outliers}

        stats_dict['district_with_most_incidents'] = max
        stats_dict['district_with_most_incidents'] = min

        repo['hek_kquirk.crime_incidents_stats'].insert(stats_dict)

        repo['hek_kquirk.crime_incidents_stats'].metadata({'complete':True})
        print(repo['hek_kquirk.crime_incidents_stats'].metadata())

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

        this_script = doc.agent('alg:hek_kquirk#crime_incidents_stats', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_incidents_per_district = doc.entity('dat:hek_kquirk#incidents_per_district', {'prov:label':'Boston Neighborhoods crime incidents Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_incidents_per_district = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_incidents_per_district, this_script)
        
        doc.usage(get_incidents_per_district, resource_incidents_per_district, startTime, None,
                  {
                      prov.model.PROV_TYPE:'ont:Retrieval'
                  }
        )
        
        crime_incidents_stats = doc.entity('dat:hek_kquirk#crime_incidents_stats', {prov.model.PROV_LABEL:'Incidents and factors to Police District statistics', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(crime_incidents_stats, this_script)
        
        doc.wasGeneratedBy(crime_incidents_stats, get_incidents_per_district, endTime)
        
        doc.wasDerivedFrom(crime_incidents_stats, resource_incidents_per_district, get_incidents_per_district, get_incidents_per_district, get_incidents_per_district)

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