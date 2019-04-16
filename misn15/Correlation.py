import json
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
    writes = ['misn15.correlation', 'misn15.scores']

    @staticmethod
    def execute(trial = False):
        '''Get correlation coefficients and scores for every zip code'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        crime_health = list(repo['misn15.crime_health_waste_space'].find())
        zips = list(repo['misn15.zipcodes'].find())

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

        # define relational building blocks
        def product(R, S):
            return [(t, u) for t in R for u in S]

        def select(R, s):
            return [t for t in R if s(t)]

        def project(R, p):
            return [p(t) for t in R]

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([u for (k, u) in R if k == key])) for key in keys]

        # correlation between crime and health
        corr_coefficients = []
        crime_health = pd.DataFrame(crime_health)
        crime_health_pd = crime_health[['fips', 'crime', 'income', 'open space', 'waste', 'total occurrences']]
        crime_health_pd = crime_health_pd.drop_duplicates()
        crime_health = np.matrix(crime_health_pd)
        crime_count = crime_health[:,1]
        health_count = crime_health[:, -1]
        coefficient = round(corr(crime_count, health_count), 3)
        corr_coefficients.append(['crime vs health', coefficient])

        # correlation between income and health
        income_count = crime_health[:, 2]
        coefficient = round(corr(income_count, health_count), 3)
        corr_coefficients.append(['income vs health', coefficient])

        # correlation between open spaces and health
        open_space_count = crime_health[:, 3]
        coefficient = round(corr(open_space_count, health_count), 3)
        corr_coefficients.append(['open space vs health', coefficient])

        # correlation between waste and health
        waste_count = crime_health[:, 4]
        coefficient = round(corr(waste_count, health_count), 3)
        corr_coefficients.append(['waste vs health', coefficient])

        # correlation between open space and income
        coefficient = round(corr(income_count, open_space_count), 3)
        corr_coefficients.append(['open space vs income', coefficient])

        # custom scoring metric
        crime_health_pd['score'] = crime_health_pd['total occurrences'] + corr_coefficients[0][1] * crime_health_pd['crime'] + corr_coefficients[2][1] * crime_health_pd['open space'] + corr_coefficients[3][1] * crime_health_pd['waste']

        score_df = crime_health_pd[['fips', 'score']]

        score_list = []
        for x in range(len(score_df)):
            score_list += [[score_df.iloc[x,0], score_df.iloc[x,1]]]

        zips_list = []
        for x in zips:
            zips_list += [['0'+ str(x['zip']), x['tract']]]

        # get zip code for every fips tract and compute score
        zips_score = product(score_list, zips_list)
        zips_select = select(zips_score, lambda t: t[0][0] == str(t[1][1]))
        zips_project = project(zips_select, lambda t: (t[1][0], t[0][1]))
        zips_agg = aggregate(zips_project, sum)
        zips_agg.sort(key=lambda t: t[1])

        repo.dropCollection("correlation")
        repo.createCollection("correlation")

        repo.dropCollection("score")
        repo.createCollection("score")

        for x in corr_coefficients:
            entry = {x[0]: x[1]}
            repo['misn15.correlation'].insert_one(entry)

        for x in zips_agg:
            entry = {x[0]: x[1]}
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
        doc.add_namespace('bdp', 'https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT * from "12cb3883-56f5-47de-afa5-3b1cf61b257b" WHERE CAST(year AS Integer) > 2016')

        this_script = doc.agent('alg:misn15#getCrime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:Boston_crime', {'prov:label':'Boston_crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query': '?sql=SELECT * from "12cb3883-56f5-47de-afa5-3b1cf61b257b" WHERE CAST(year AS Integer) > 2016'
                  }
                  )
        crime_data = doc.entity('dat:misn15#RetrieveCrime', {prov.model.PROV_LABEL:'Boston Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_data, this_script)
        doc.wasGeneratedBy(crime_data, get_crime, endTime)
        doc.wasDerivedFrom(crime_data, resource, get_crime, get_crime, get_crime)
                  
        return doc

Correlation.execute(trial=True)
doc = Correlation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
