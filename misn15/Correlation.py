import dml
import prov.model
import datetime
import uuid
from math import *
import pandas as pd
import numpy as np

class Correlation(dml.Algorithm):
    contributor = 'misn15'
    reads = ['misn15.crime_health_waste_space']
    writes = ['misn15.correlation', 'misn15.score']

    @staticmethod
    def execute(trial = False):
        '''Get correlation coefficients and scores for every census tract'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        crime_health = list(repo['misn15.crime_health_waste_space'].find())

        if trial:
            crime_health = crime_health[0:20]

        # define functions to compute correlation coefficient
        def avg(x):
            return sum(x) / len(x)

        def stddev(x):
            m = avg(x)
            return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

        def cov(x, y):
            return sum([(xi - avg(x)) * (yi - avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

        def corr(x, y):
            if stddev(x) * stddev(y) != 0:
                return float(cov(x, y) / (stddev(x) * stddev(y)))

        # correlation between crime and health
        corr_coefficients = []
        crime_health = pd.DataFrame(crime_health)
        crime_health_pd = crime_health[['fips', 'crime', 'income', 'open space', 'waste', 'total occurrences', 'cancer occurrences']]
        crime_health_pd = crime_health_pd.drop_duplicates()
        crime_health = np.matrix(crime_health_pd)
        crime_count = crime_health[:,1]
        health_count = crime_health[:, -2]
        coefficient = round(corr(crime_count, health_count), 3)
        corr_coefficients.append(['crime vs health', coefficient])

        cancer_count = crime_health[:, -1]
        coefficient = round(corr(crime_count, cancer_count), 3)
        corr_coefficients.append(['crime vs cancer', coefficient])

        # correlation between income and health
        income_count = crime_health[:, 2]
        coefficient = round(corr(income_count, health_count), 3)
        corr_coefficients.append(['income vs health', coefficient])
        coefficient = round(corr(income_count, cancer_count), 3)
        corr_coefficients.append(['income vs cancer', coefficient])

        # correlation between open spaces and health
        open_space_count = crime_health[:, 3]
        coefficient = round(corr(open_space_count, health_count), 3)
        corr_coefficients.append(['open space vs health', coefficient])
        coefficient = round(corr(open_space_count, cancer_count), 3)
        corr_coefficients.append(['open_space vs cancer', coefficient])

        # correlation between waste and health
        waste_count = crime_health[:, 4]
        coefficient = round(corr(waste_count, health_count), 3)
        corr_coefficients.append(['waste vs health', coefficient])
        coefficient = round(corr(waste_count, cancer_count), 3)
        corr_coefficients.append(['waste vs cancer', coefficient])

        # correlation between open space and income
        coefficient = round(corr(income_count, open_space_count), 3)
        corr_coefficients.append(['open space vs income', coefficient])

        # normalize correlation coefficients
        crime_corr = (corr_coefficients[0][1] + 1) / 2
        income_corr = (corr_coefficients[2][1] + 1) / 2
        openSpace_corr = (corr_coefficients[4][1] + 1) / 2
        waste_corr = (corr_coefficients[6][1] + 1) / 2

        # normalize data
        def normalize(value, low, high):
            return float((value - low) / (high - low))

        # normalize correlation coefficients
        scores_list = []
        for x in range(len(crime_health_pd)):
            health_norm = normalize(crime_health_pd.iloc[x]['total occurrences'], crime_health_pd['total occurrences'].min(),
                                    crime_health_pd['total occurrences'].max())
            crime_norm = normalize(crime_health_pd.iloc[x]['crime'], crime_health_pd['crime'].min(), crime_health_pd['crime'].max())
            income_norm = normalize(crime_health_pd.iloc[x]['income'], crime_health_pd['income'].min(),
                                   crime_health_pd['income'].max())
            openSpace_norm = normalize(crime_health_pd.iloc[x]['open space'], crime_health_pd['open space'].min(),
                                   crime_health_pd['open space'].max())
            waste_norm = normalize(crime_health_pd.iloc[x]['waste'], crime_health_pd['waste'].min(),
                                       crime_health_pd['waste'].max())
            score = health_norm + crime_norm * crime_corr + income_norm * income_corr + openSpace_norm * openSpace_corr + waste_norm * waste_corr
            scores_list += [[crime_health_pd.iloc[x]['fips'], score]]

        repo.dropCollection("correlation")
        repo.createCollection("correlation")

        repo.dropCollection("score")
        repo.createCollection("score")

        for x in corr_coefficients:
            entry = {x[0]: x[1]}
            repo['misn15.correlation'].insert_one(entry)

        for x in scores_list:
            entry = {'fips': x[0], 'score': x[1]}
            repo['misn15.score'].insert_one(entry)

        repo['misn15.score'].metadata({'complete': True})
        print(repo['misn15.score'].metadata())

        repo['misn15.correlation'].metadata({'complete':True})
        print(repo['misn15.correlation'].metadata())

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

        this_script = doc.agent('alg:misn15#Correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:misn15#crime_health_waste_space', {'prov:label':'Crime, Health, Waste, and Open Space Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_scores = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_correlation, this_script)
        doc.wasAssociatedWith(get_scores, this_script)
        doc.usage(get_correlation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                   }
                  )
        doc.usage(get_scores, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'
                   }
                  )
        correlation = doc.entity('dat:misn15#correlation', {prov.model.PROV_LABEL:'Correlation Coefficients', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(correlation, this_script)
        doc.wasGeneratedBy(correlation, get_correlation, endTime)
        doc.wasDerivedFrom(correlation, resource, get_correlation, get_correlation, get_correlation)

        scores = doc.entity('dat:misn15#score', {prov.model.PROV_LABEL: 'Health Scores for Census tracts', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(scores, this_script)
        doc.wasGeneratedBy(scores, get_scores, endTime)
        doc.wasDerivedFrom(scores, resource, get_scores, get_scores, get_scores)
                  
        return doc

# Correlation.execute()
# doc = Correlation.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
