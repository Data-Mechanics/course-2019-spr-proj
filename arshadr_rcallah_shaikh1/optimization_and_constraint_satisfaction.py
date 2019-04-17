import dml
import prov.model
import datetime
import uuid
import numpy as np
from stat_functions import *
from z3 import *

class optimization_and_constraint_satisfaction(dml.Algorithm):
    contributor = 'arshadr_rcallah_shaikh1'
    reads = ['arshadr_rcallah_shaikh1.income_data']
    writes = ['arshadr_rcallah_shaikh1.optimization_and_constraint_satisfaction']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')

        cur = repo['arshadr_rcallah_shaikh1.income_data'].find()
        json_dict = None
        for doc in cur:
            json_dict = doc
        del json_dict['_id']

        # create a list of percents where each percent represents the percentage of residents in the respective income bracket
        percents = [int(float(v[:-1])*10) for k, v in json_dict.items() if k != 'median_income_households']

        # list of income bracket ranges
        ranges = [(0, 10000), (10000, 15000), (15000, 25000), (25000, 35000), (35000, 50000), (50000, 75000), (75000, 100000), (100000, 150000), (150000, 200000), (200000, 1000000)]

        # this will store the distributions of incomes
        population_income = []

        # populate the incomes
        # since we do not have exact figures in regards to how many people make what, we create a uniform distribution over each bracket with respect to the percentage of people in the bracket
        for i in range(len(percents)):
            population_income += list(np.random.uniform(ranges[i][0], ranges[i][1], percents[i]))

        # z3 solver
        price_of_new_home = Int('price_of_new_home')
        s = Solver()
        # constraint is that at least more than 50% of people must be able to afford this price
        s.add(afford(price_of_new_home, population_income) > .5)
        last_model = None
        i = 0
        while True:
            print(i)
            r = s.check()
            if r == unsat:
                if last_model != None:
                    print(last_model)
                    break
                else:
                    last_model = unsat
                    print(unsat)
                    break
            if r == unknown:
                last_model = 'Not Found'
                print('UnKnown')
                break
            last_model = s.model()
            s.add(price_of_new_home > last_model[price_of_new_home])
            i += 1
            if i > 2000:
                last_model = 'Not Found'
                print('Not Found')
                break
        print(last_model)
        # end of z3 solver


        # getting bad results from z3 to brute force option with same constraints and optimizations
        brute_force_price = 0
        while afford1(brute_force_price, population_income) > .5:
            brute_force_price += 10 # increase by $10

        print(brute_force_price)

        dat = {}
        dat['Problem'] = 'If the city wants to build a new home, what is the maximum price that home can be in order for at least 50% of residents to be able to afford it. We assume that someone can afford a home if the home\'s price is 4.5x their income.'
        dat['Optimization'] = 'maximum price for a home'
        dat['Constraint'] = 'at least 50% of residents will need to afford it'
        dat['z3_optimization'] = str(last_model)
        dat['brute_force_optimization'] = brute_force_price


        repo.dropCollection("optimization_and_constraint_satisfaction")
        repo.createCollection("optimization_and_constraint_satisfaction")

        repo['arshadr_rcallah_shaikh1.optimization_and_constraint_satisfaction'].insert_one(dat)
        repo['arshadr_rcallah_shaikh1.optimization_and_constraint_satisfaction'].metadata({'complete':True})
        print(repo['arshadr_rcallah_shaikh1.optimization_and_constraint_satisfaction'].metadata())

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
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('zlo', 'https://www.zillow.com/')
        doc.add_namespace('chl', 'https://www.chelseama.gov/assessor/pages/')

        this_script = doc.agent('alg:arshadr_rcallah_shaikh1#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('zlo:research/data/', { prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource = doc.entity('zlo:research/data/', { prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource = doc.entity('chl:chelsea-property-data', { prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_rent = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_buying = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_property = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rent, this_script)
        doc.wasAssociatedWith(get_buying, this_script)
        doc.wasAssociatedWith(get_property, this_script)
        doc.usage(get_rent, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Retrieval':''
                   }
                  )
        doc.usage(get_buying, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Retrieval':''
                   }
                  )
        doc.usage(get_property, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Retrieval':''
                   }
                  )

        rent = doc.entity('zlo:research/data/', {prov.model.PROV_LABEL:'Rent Prices', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(rent, this_script)
        doc.wasGeneratedBy(rent, get_rent, endTime)
        doc.wasDerivedFrom(rent, resource, get_rent, get_rent, get_rent)

        buying = doc.entity('zlo:research/data/', {prov.model.PROV_LABEL:'Buying Prices', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(buying, this_script)
        doc.wasGeneratedBy(buying, get_buying, endTime)
        doc.wasDerivedFrom(buying, resource, get_buying, get_buying, get_buying)

        property = doc.entity('chl:chelsea-property-data', {prov.model.PROV_LABEL:'Property Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(property, this_script)
        doc.wasGeneratedBy(property, get_property, endTime)
        doc.wasDerivedFrom(property, resource, get_property, get_property, get_property)


        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
optimization_and_constraint_satisfaction.execute()
'''
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof