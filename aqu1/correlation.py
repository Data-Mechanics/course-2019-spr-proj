import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import math
import numpy as np
import scipy.stats
import matplotlib.pyplot as plt

from sklearn.linear_model import LinearRegression

class correlation(dml.Algorithm):
    contributor = 'aqu1'
    reads = ['aqu1.education_data', 'aqu1.income_data', 'aqu1.schools_data', 'aqu1.mbta_stops_data']
    writes = ['aqu1.correlation']
    
    def deg_to_rad(deg):
        return deg * (math.pi/180)

    def getDistance(lat1, lon1, lat2, lon2):
        R = 6371
        dLat = correlation.deg_to_rad(lat2-lat1)
        dLon = correlation.deg_to_rad(lon2-lon1)
        a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(correlation.deg_to_rad(lat1)) * math.cos(correlation.deg_to_rad(lat2)) * math.sin(dLon/2) * math.sin(dLon/2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = R * c
        return d
    
    def process_data(education, t_stops, income, all_schools, trial):
    
        tstops_list = []
        
        # create list of lists of t-stops
        for row in t_stops:
            if isinstance(row, str) == False:
                tstops_list.append([row['Latitude'], row['Longitude']])

        schools_list = []
        
        # create list of lists of all schools
        for row in all_schools:
            if isinstance(row, str) == False:
                schools_list.append([row['Latitude'], row['Longitude'], row['City']])
                
        # calculate distances from every t-stop to every school in Boston
        school_stop_dist = []
        if trial:
            for stop in tstops_list[:6]:
                for school in schools_list[:3]:
                    school_stop_dist.append([school[2], correlation.getDistance(stop[0], stop[1], school[0], school[1])])
        else:
            for stop in tstops_list:
                for school in schools_list:
                    school_stop_dist.append([school[2], correlation.getDistance(stop[0], stop[1], school[0], school[1])])

        school_stop_dist = pd.DataFrame(school_stop_dist)
        school_stop_dist.columns = ['Neighborhood', 'Distance']
        school_stop_dist = school_stop_dist.groupby('Neighborhood').mean()
        
        #convert distances from km to miles 
        school_stop_dist['Distance'] = school_stop_dist['Distance'].apply(lambda d: d/1.609) 
        school_stop_dist = school_stop_dist.rename_axis('Neighborhood').reset_index()
        
        education = education[education['Education'] == 'Bachelor\'s Degree or Higher']
        
        dist_education = pd.merge(school_stop_dist, education, on = 'Neighborhood')
        dist_education = dist_education.drop(['Decade', 'Education', 'Number of People'], axis = 1)

        dist_education['Percent of Population'] = dist_education['Percent of Population'].astype(dtype = np.float64)
        
        income_education = pd.merge(income, education, on = 'Neighborhood')
        income_education['prop_low_income'] = income_education['prop_low_income'].apply(lambda p: p*100)
        income_education['Percent of Population'] = income_education['Percent of Population'].astype(dtype = np.float64)  
        result = [dist_education, income_education, school_stop_dist]
        return result
        
    def lin_reg(x, y, xlabel, ylabel, title, data):
        x = np.array(x).reshape(-1,1)
        y = np.array(y).reshape(-1,1)
        regr = LinearRegression()
        regr.fit(x, y)
        plt.scatter(x, y)
        plt.plot(x, regr.predict(x), color = 'orange',linewidth = 2)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title, loc = 'center')
        s = 'r = ' + str(data[0]) + '\n' + 'p = ' + str(data[1])
        if title == 'Percent Low Income vs. Percent With College Degree':
            plt.text(70, 50, s, fontsize = 12)
        else:
            plt.text(6, 65, s, fontsize = 12)
        plt.show()
    
    def corr_pval(x, y, description):
        corr_coef, p_value = scipy.stats.pearsonr(x, y)
        print(description)
        print('The correlation coefficient is:', round(corr_coef, 2))
        print('The p value is:', round(p_value, 2), '\n')
        data = [round(corr_coef, 2), round(p_value, 2)]
        return data

    def stat_analysis(dist_education, income_education):
        title = 'Percent Low Income vs. Percent With College Degree'
        x = income_education['Percent of Population']
        y = income_education['prop_low_income']
        xlabel = 'Percent in Neighborhood with Bachelor\'s Degree or Higher'
        ylabel = 'Percent Low Income'
        description = 'Percent Low Income vs. Percent Bachelor\'s Degree or Higher'
        result = correlation.corr_pval(x, y, description)
        #correlation.lin_reg(x, y, xlabel, ylabel, title, result)

        title = 'Percent With College Degree vs. Distance from T-stop to School'
        x = dist_education['Distance']
        y = dist_education['Percent of Population']
        xlabel = 'Avg Dist from T-stop to School (Miles)'
        ylabel = 'Percent in Neighborhood with \n Bachelor\'s Degree or Higher'
        description = 'Percent Bachelor\'s Degree or Higher vs. Avg Dist from T-stop to School'
        data = correlation.corr_pval(x, y, description)
        #correlation.lin_reg(x, y, xlabel, ylabel, title, data)
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aqu1', 'aqu1')
        
        education = repo.aqu1.education_data.find()
        education = pd.DataFrame(education)

        t_stops = repo.aqu1.mbta_stops_data.find()
        
        income = repo.aqu1.income_data.find()
        income = pd.DataFrame(income)
        
        all_schools = repo.aqu1.schools_data.find()
        
        result = correlation.process_data(education, t_stops, income, all_schools, trial)
        dist_education = result[0]
        income_education = result[1]
        school_stop_dist = result[2]
        school_stop_dist = pd.DataFrame(school_stop_dist)
        school_stop_dist = json.loads(school_stop_dist.to_json(orient = 'records'))
        correlation.stat_analysis(dist_education, income_education)
        
        repo.dropCollection("correlation")
        repo.createCollection("correlation")
         
        repo['aqu1.correlation'].insert_many(school_stop_dist)
        
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
        doc.add_namespace('stats', 'http://google.com/')
        
        this_script = doc.agent('alg:aqu1#correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # Statistical Analysis Report
        resource_stats_analysis = doc.entity('stats:correlation', {'prov:label':'Statistical Analysis', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stats = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stats, this_script)
        doc.usage(get_stats, resource_stats_analysis, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        stats_analysis = doc.entity('dat:aqu1#correlation', {prov.model.PROV_LABEL:'Statistical Analysis', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(stats_analysis, this_script)
        doc.wasGeneratedBy(stats_analysis, get_stats, endTime)
        doc.wasDerivedFrom(stats_analysis, resource_stats_analysis, get_stats, get_stats, get_stats)
        doc.wasDerivedFrom(stats_analysis, resource_stats_analysis, get_stats, get_stats, get_stats)
        
        repo.logout()
        return doc
