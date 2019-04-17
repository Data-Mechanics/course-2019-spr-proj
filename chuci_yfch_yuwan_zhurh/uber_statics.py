import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import scipy.stats
from random import shuffle
from math import sqrt

class uber_statics(dml.Algorithm):
    contributor = 'chuci_yfch_yuwan_zhurh'
    reads = ['chuci_yfch_yuwan_zhurh.uber_loc', 'chuci_yfch_yuwan_zhurh.unemploy', 'chuci_yfch_yuwan_zhurh.gov']
    writes = ['chuci_yfch_yuwan_zhurh.static']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Get the uber dataset
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('chuci_yfch_yuwan_zhurh', 'chuci_yfch_yuwan_zhurh')
        # Basic function
        def find_dist(x):
            loc = (42.358953, -71.049575)
            return sqrt((x['latitude'] - loc[0]) ** 2 + (x['longitude'] - loc[1]) ** 2)

        def permute(x):
            shuffled = [xi for xi in x]
            shuffle(shuffled)
            return shuffled

        def avg(x):  # Average
            return sum(x) / len(x)

        def stddev(x):  # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

        def cov(x, y):  # Covariance.
            return sum([(xi - avg(x)) * (yi - avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

        def corr(x, y):  # Correlation coefficient.
            if stddev(x) * stddev(y) != 0:
                return cov(x, y) / (stddev(x) * stddev(y))

        def p(x, y):
            c0 = corr(x, y)
            corrs = []
            if trial:
                up = 5
            else:
                up = 2000
            for k in range(0, up):
                y_permuted = permute(y)
                corrs.append(corr(x, y_permuted))
            return len([c for c in corrs if abs(c) >= abs(c0)]) / len(corrs)
        json_data = list(repo['chuci_yfch_yuwan_zhurh.uber_loc'].find())
        data = pd.DataFrame(json_data)
        data = data[(data['latitude'] > 42) & (data['longitude'] > -80)]
        if trial:
            data = data.head(50)
        data['distance'] = data.apply(find_dist, axis=1)
        x = list(data['distance'])
        y = list(data['Mean Travel Time (Seconds)'])

        corr_uber = corr(x,y)
        p_uber = p(x, y)
        (corr_uber_2,p_uber_2) = scipy.stats.pearsonr(x, y)
        print("Correlation coefficient of uber's distance and uber's travel time:")
        print(corr_uber)
        print('p-value:')
        print(p_uber)
        print('by using the library,we can get:')
        print((corr_uber_2,p_uber_2))
        print('----')

        json_data_unemploy_= list(repo['chuci_yfch_yuwan_zhurh.unemploy'].find())
        data_unemploy = pd.DataFrame(json_data_unemploy_)
        data_unemploy = data_unemploy[['City','Rate % Dec-17', 'Rate % Dec-18', 'Rate % Nov-18']]
        data_unemploy = data_unemploy[data_unemploy.sort_values('City').duplicated()].sort_values('City')

        json_data_gov = list(repo['chuci_yfch_yuwan_zhurh.gov'].find())
        data_gov = pd.DataFrame(json_data_gov)
        data_gov = data_gov[data_gov['winner']]

        if trial:
            data_unemploy = data_unemploy.head(50)
            data_gov = data_gov.head(50)

        after_merge = data_gov.merge(data_unemploy, right_on='City', left_on='reportingunitname')

        x = list(after_merge['Rate % Dec-17'])
        y = list(after_merge['votepct'])
        corr_1217 = corr(x, y)
        p_1217 = p(x, y)
        (corr_1217_2, p_1217_2) = scipy.stats.pearsonr(x, y)

        print("Correlation coefficient of GOP's vote percent and Dec-17's unemployment rate")
        print(corr_1217)
        print('p-value:')
        print(p_1217)
        print('by using the library,we can get:')
        print((corr_1217_2, p_1217_2))
        print('----')

        x = list(after_merge['Rate % Dec-18'])
        y = list(after_merge['votepct'])
        corr_1218 = corr(x, y)
        p_1218 = p(x, y)
        (corr_1218_2, p_1218_2) = scipy.stats.pearsonr(x, y)

        print("Correlation coefficient of GOP's vote percent and Dec-18's unemployment rate")
        print(corr_1218)
        print('p-value:')
        print(p_1218)
        print('by using the library,we can get:')
        print((corr_1218_2, p_1218_2))
        print('----')

        x = list(after_merge['Rate % Nov-18'])
        y = list(after_merge['votepct'])
        corr_1118 = corr(x, y)
        p_1118 = p(x, y)
        (corr_1118_2, p_1118_2) = scipy.stats.pearsonr(x, y)

        print("Correlation coefficient of GOP's vote percent and Nov-18's unemployment rate")
        print(corr_1118)
        print('p-value:')
        print(p_1118)
        print('by using the library,we can get:')
        print((corr_1118_2, p_1118_2))
        print('----')

        new_data = pd.DataFrame(
            {'X_variable':['uber distance','unemployment rate 12/17', 'unemployment rate 12/18', 'unemployment rate 11/18'],
             'Y_variable':['uber travel time', 'GOP vote percent', 'GOP vote percent', 'GOP vote percent'],
             'correlation_coefficient_self': [corr_uber, corr_1217, corr_1218, corr_1118],
             'p_value_self':[p_uber, p_1217, p_1218, p_1118],
             'correlation_coefficient_lib': [corr_uber_2, corr_1217_2, corr_1218_2, corr_1118_2],
             'p_value_lib': [p_uber_2, p_1217_2, p_1218_2, p_1118_2],
             }
        )
        repo.dropCollection("static")
        repo.createCollection("static")
        repo['chuci_yfch_yuwan_zhurh.static'].insert_many(new_data.to_dict('records'))
        repo['chuci_yfch_yuwan_zhurh.static'].metadata({'complete': True})

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
        # New
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('chuci_yfch_yuwan_zhurh', 'chuci_yfch_yuwan_zhurh')

        agent = doc.agent('alg:chuci_yfch_yuwan_zhurh#static',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        entity_uber = doc.entity('dat:chuci_yfch_yuwan_zhurh#uber_location',
                           {prov.model.PROV_LABEL: 'uber data', prov.model.PROV_TYPE: 'ont:DataSet'})
        unemploy = doc.entity('dat:chuci_yfch_yuwan_zhurh#unemploy',
                           {prov.model.PROV_LABEL: 'gov data', prov.model.PROV_TYPE: 'ont:DataSet'})
        gov = doc.entity('dat:chuci_yfch_yuwan_zhurh#gov',
                           {prov.model.PROV_LABEL: 'gov data', prov.model.PROV_TYPE: 'ont:DataSet'})
        stat = doc.entity('dat:chuci_yfch_yuwan_zhurh#stat',
                         {prov.model.PROV_LABEL: 'stat data', prov.model.PROV_TYPE: 'ont:DataSet'})

        activity = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(activity, agent)
        
        doc.usage(activity, entity_uber, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:aggregate'}
                  )
        doc.usage(activity, unemploy, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:aggregate'}
                  )
        doc.usage(activity, gov, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:aggregate'}
                  )

        doc.wasAttributedTo(stat, agent)
        doc.wasGeneratedBy(stat, activity, endTime)
        doc.wasDerivedFrom(entity_uber, stat, activity, activity, activity)
        doc.wasDerivedFrom(unemploy, stat, activity, activity, activity)
        doc.wasDerivedFrom(gov, stat, activity, activity, activity)

        repo.logout()

        return doc

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