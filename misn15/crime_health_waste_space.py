import urllib.request
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import requests

class crime_health_waste_space(dml.Algorithm):
    contributor = 'misn15'
    reads = ['misn15.waste_all', 'misn15.clean_health', 'misn15.crime', 'misn15.income', 'misn15.openSpace_centroids']
    writes = ['misn15.crime_health_waste_space']

    @staticmethod
    def execute(trial = False):
        '''transform crime, health, waste, and open space data sets'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        # Define relational building blocks
        def aggregate(R, f):
            keys = {r[5] for r in R}
            return [(key, f([v for (h, i, j, k, l, m, v) in R if m == key])) for key in keys]

        def project(R, p):
            return [p(t) for t in R]

        def agg_crime(R, f):
            keys = {r[0] for r in R}
            return [(key, f([u for (k, u) in R if k == key])) for key in keys]

        # read in data
        waste_all = list(repo['misn15.waste_all'].find())
        clean_health = list(repo['misn15.clean_health'].find())
        income = list(repo['misn15.income'].find())
        open_space = list(repo['misn15.openSpace_centroids'].find())
        crime = list(repo['misn15.crime'].find())

        if trial:
            waste_all = waste_all[0:40]
            clean_health = clean_health[0:40]
            open_space = open_space[0:800]
            crime = crime[0:400]

        # get crime coordinates and classify them into fips tracts
        crime_fips = []
        for x in crime:
            if x['Lat'] != None and x['Long'] != '-1' and x['Long'] != '0':
                params = urllib.parse.urlencode({'latitude': x['Lat'], 'longitude': x['Long'], 'format': 'json'})
                url = 'https://geo.fcc.gov/api/census/block/find?' + params
                response = requests.get(url)
                data = response.json()
                geoid = data['Block']['FIPS'][0:11]
                crime_fips += [(geoid, 1)]

        # sum number of crimes for each fips tract
        crime_sum = agg_crime(crime_fips, sum)

        # get number of open spaces for each fips tract
        openSpace_project = project(open_space, lambda t: (t['FIPS'], t['Coordinates'], 1))
        keys = [r[0] for r in openSpace_project]
        unique_keys = []
        for x in keys:
            if x not in unique_keys:
                unique_keys.append(x)
        openSpace_sum = [(key, sum([v for (k, u, v) in openSpace_project if k == key])) for key in unique_keys]

        # get income for each fips tract code
        income_list = []
        for x in income:
            if x['B06011_001E'] != -666666666.0:
                income_list += [(x['state']+x['county']+x['tract'], x['B06011_001E'])]

        # filter waste
        waste_list = []
        for x in waste_all:
            waste_list += [[x['Name'], x['Address'], x['Zip Code'], x['Coordinates'], x['Status'], x['FIPS']]]

        # get only important columns from waste data sets and sum them
        waste_project = project(waste_list, lambda t: (t[0], t[1], t[2], t[3], t[4], t[5], 1))
        waste_sum = aggregate(waste_project, sum)

        # join waste and income
        wasteSum_pd = pd.DataFrame(waste_sum, columns=['fips', 'waste'])
        income_pd = pd.DataFrame(income_list, columns=['fips', 'income'])
        waste_income = wasteSum_pd.merge(income_pd, left_on= 'fips', right_on = 'fips', how = 'outer')

        # get average probability of disease and total occurrences
        health_pd = pd.DataFrame(clean_health)
        avg_prev_list = []

        for x in range(len(health_pd)):

            avg_prev = 0
            for y in range(0, 11):
                avg_prev += float(health_pd.iloc[x,y])/100

            avg_prev /= 12
            avg_prev_list.append(round(avg_prev, 3))


        health_pd['probability of disease'] = avg_prev_list


        # combine waste, income and health
        health_pd = health_pd.drop(columns =['_id', 'coordinates', 'Physical Health', 'Teeth Loss'])
        waste_health_pd = waste_income.merge(health_pd, left_on = 'fips', right_on='tractfips', how = 'outer')

        # add crime and number of health incidences to dataframe
        crime_pd = pd.DataFrame(crime_sum, columns=['fips', 'crime'])
        crime_health = waste_health_pd.merge(crime_pd, left_on='fips', right_on='fips', how = 'outer')
        crime_health['Occur_Cancer'] = (pd.to_numeric(crime_health['Cancer (except skin)'])/100) * pd.to_numeric(crime_health['populationcount'])
        crime_health['Cancer_prev'] = (pd.to_numeric(crime_health['Cancer (except skin)']))
        crime_health['Occur_Asthma'] = (pd.to_numeric(crime_health['Current Asthma']) / 100) * pd.to_numeric(crime_health['populationcount'])
        crime_health['Occur_COPD'] = (pd.to_numeric(crime_health['COPD']) / 100) * pd.to_numeric(crime_health['populationcount'])
        crime_health = crime_health.drop(columns=['tractfips'])

        # get total health incidences for all fips tracts
        health_occur = []
        for x in range(len(crime_health)):
            total_occur = 0
            for y in range(3, 14):
                occur = float(crime_health.iloc[x, y])/100 * float(crime_health.iloc[x, -7])
                total_occur += occur
            health_occur.append(total_occur)

        crime_health['Total Occurrences'] = health_occur

        # add open space count to dataset
        openSpace_pd = pd.DataFrame(openSpace_sum, columns=['fips', 'Open Space'])
        final_df = openSpace_pd.merge(crime_health, left_on='fips', right_on='fips', how='outer')
        final_df = final_df.fillna(0)

        repo.dropCollection("misn15.crime_health_waste_space")
        repo.createCollection("misn15.crime_health_waste_space")

        for x in range(len(final_df)):
            entry = {'fips': final_df.iloc[x,0], 'open space': final_df.iloc[x, 1], 'waste': final_df.iloc[x, 2], 'income': final_df.iloc[x, 3],
                     'crime': final_df.iloc[x,-6], 'cancer prevalence': final_df.iloc[x, -4], 'cancer occurrences': final_df.iloc[x, -5],
                     'asthma occurrences': final_df.iloc[x, -3], 'COPD occurrences': final_df.iloc[x,-2], 'total occurrences': final_df.iloc[x, -1],
                     'population': final_df.iloc[x,-8], 'probability of disease': final_df.iloc[x,-7]}
            repo['misn15.crime_health_waste_space'].insert_one(entry)

        repo['misn15.crime_health_waste_space'].metadata({'complete':True})
        print(repo['misn15.crime_health_waste_space'].metadata())

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
        
        this_script = doc.agent('alg:misn15#crime_health_waste_space', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:misn15#waste_all', {'prov:label':'Boston Waste Sites', prov.model.PROV_TYPE:'ont:DataSet'})
        resource2 = doc.entity('dat:misn15#crime', {'prov:label':'Boston Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
        resource3 = doc.entity('dat:misn15#health', {'prov:label': 'Boston Health Data', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource4 = doc.entity('dat:misn15#income', {'prov:label': 'Boston Average Income Data', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource5 = doc.entity('dat:misn15#openSpace_centroids', {'prov:label': 'Boston Open Spaces', prov.model.PROV_TYPE: 'ont:DataSet'})
       
        get_crime_waste_health = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime_waste_health, this_script)
        doc.usage(get_crime_waste_health, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                        }
                  )
        doc.usage(get_crime_waste_health, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                        }
                  )
        doc.usage(get_crime_waste_health, resource3, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'
                   }
                  )
        doc.usage(get_crime_waste_health, resource4, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'
                   }
                  )
        doc.usage(get_crime_waste_health, resource5, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'
                   }
                  )
        crime_health_waste_space = doc.entity('dat:misn15#crime_health_waste_space', {prov.model.PROV_LABEL:'Waste, Health, and Open Spaces Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_health_waste_space, this_script)
        doc.wasGeneratedBy(crime_health_waste_space, get_crime_waste_health, endTime)
        doc.wasDerivedFrom(crime_health_waste_space, resource, get_crime_waste_health, get_crime_waste_health, get_crime_waste_health)
        doc.wasDerivedFrom(crime_health_waste_space, resource2, get_crime_waste_health, get_crime_waste_health, get_crime_waste_health)
        doc.wasDerivedFrom(crime_health_waste_space, resource3, get_crime_waste_health, get_crime_waste_health, get_crime_waste_health)
        doc.wasDerivedFrom(crime_health_waste_space, resource4, get_crime_waste_health, get_crime_waste_health, get_crime_waste_health)
        doc.wasDerivedFrom(crime_health_waste_space, resource5, get_crime_waste_health, get_crime_waste_health, get_crime_waste_health)

        return doc

crime_health_waste_space.execute(trial=True)
##doc = crime_health_waste_space.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
