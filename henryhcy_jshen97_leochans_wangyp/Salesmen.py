import dml
import datetime
import geopy.distance as gd
import itertools
import json
import prov.model
import pprint
import uuid
import math
import z3


class Salesmen(dml.Algorithm):
    contributor = "henryhcy_jshen97_leochans_wangyp"
    reads = ['henryhcy_jshen97_leochans_wangyp.cvs',
             'henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS',
             'henryhcy_jshen97_leochans_wangyp.correlationCVS']
    writes = ['henryhcy_jshen97_leochans_wangyp.solution']

    @staticmethod
    def execute(trial=False):
        start_time = datetime.datetime.now()

        # set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('henryhcy_jshen97_leochans_wangyp', 'henryhcy_jshen97_leochans_wangyp')

        # create the result collections
        repo.dropCollection('solution')
        repo.createCollection('solution')

        # define stability threshold and accessibility threshold for constraint satisfaction problem
        S = 0.2
        A = 6.0

        # solve the optimization problem
        corr_evi = repo.henryhcy_jshen97_leochans_wangyp.correlationCVS.find_one({"document_type": "rating_eviction_correlation"})['corr']
        corr_lar = repo.henryhcy_jshen97_leochans_wangyp.correlationCVS.find_one({"document_type": "rating_crime_correlation"})['corr']
        c = corr_lar/corr_evi

        dataset = []
        for document in repo.henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS.find({'eviction_case': {'$gt': 100}, 'crime_case': {'$gt': 100}}):
            place_id = document['place_id']
            location_coordinate = (document['location']['lat'], document['location']['lng'])
            stability = (math.pow(document['crime_case'], c)/document['eviction_case'])*1000
            dataset.append((place_id, location_coordinate, stability))

        all_possible_index = list(itertools.combinations([i for i in range(len(dataset))], 3))

        place_id_list = []
        max_stability = 0.0;
        max_accessibility = 0.0;
        for i in all_possible_index:
            (a, b, c) = i
            total_accessibility = gd.distance(dataset[a][1], dataset[b][1])+gd.distance(dataset[a][1], dataset[c][1])+gd.distance(dataset[b][1], dataset[c][1])
            total_stability = dataset[a][2]+dataset[b][2]+dataset[c][2]
            if total_stability > max_stability and total_accessibility > max_accessibility:
                place_id_list = [dataset[a][0], dataset[b][0], dataset[c][0]]
                max_stability = total_stability
                max_accessibility = total_accessibility

        vicinity_list = []
        for i in place_id_list:
            vicinity_list.append(repo.henryhcy_jshen97_leochans_wangyp.cvs.find_one({'place_id': i})['vicinity'])

        d = {
            'solution_type': 'optimization',
            'solution': place_id_list,
            'vicinity_list': vicinity_list,
            'max_sta': max_stability,
            'max_acc': max_accessibility.km
        }
        repo.henryhcy_jshen97_leochans_wangyp.solution.insert_one(d)

        # solve this constraint satisfaction problem
        point_list = []
        stability_list = []
        for item in dataset:
            point_list.append(item[1])
            stability_list.append(item[2])

        solver = z3.Solver()
        X = [z3.Real('x{}'.format(i)) for i in range(len(dataset))]

        # constraints
        # choose exactly 3
        for i in X:
            solver.add(z3.Or(i == 0.0, i == 1.0))
        solver.add(sum(X) == 3.0)
        # stability greater than or equal to S
        solver.add(sum([X[i]*stability_list[i] for i in range(len(dataset))]) >= S)

        # accessiblity greater than or equal to A
        two_subset = list(itertools.combinations([i for i in range(len(dataset))], 2))
        solver.add(sum([ X[i[0]]*X[i[1]]*(gd.distance(point_list[i[0]], point_list[i[1]]).km) for i in two_subset]) >= A)

        # get the solution
        place_id_list_z3 = []
        point_list_z3 = []
        total_stability_z3 = 0.0
        total_accessibility_z3 = 0.0
        if (solver.check() == z3.sat):
            m = solver.model()
            for i in range(len(dataset)):
                name = 'x{}'.format(i)
                if (m[z3.Real(name)] == 1):
                    place_id_list_z3.append(dataset[i][0])
                    point_list_z3.append(dataset[i][1])
                    total_stability_z3 += dataset[i][2]

            total_accessibility_z3 += gd.distance(point_list_z3[0], point_list_z3[1]).km
            total_accessibility_z3 += gd.distance(point_list_z3[0], point_list_z3[2]).km
            total_accessibility_z3 += gd.distance(point_list_z3[1], point_list_z3[2]).km

            vicinity_list_z3 = []
            for i in place_id_list_z3:
                vicinity_list_z3.append(repo.henryhcy_jshen97_leochans_wangyp.cvs.find_one({'place_id': i})['vicinity'])

            d_z3 = {
                'solution_type': 'constraint satisfaction',
                'solution': place_id_list_z3,
                'vicinity_list': vicinity_list_z3,
                'max_sta': total_stability_z3,
                'max_acc': total_accessibility_z3
            }
            repo.henryhcy_jshen97_leochans_wangyp.solution.insert_one(d_z3)
        else:
            d_z3 = {
                'solution_type': 'constraint satisfaction',
                'solution': 'solution not found'
            }
            repo.henryhcy_jshen97_leochans_wangyp.solution.insert_one(d_z3)

        repo['henryhcy_jshen97_leochans_wangyp.solution'].metadata({'complete': True})
        print(repo['henryhcy_jshen97_leochans_wangyp.solution'].metadata())


        repo.logout()

        end_time = datetime.datetime.now()

        return {"start": start_time, "end": end_time}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), start_time=None, end_time=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('henryhcy_jshen97_leochans_wangyp', 'henryhcy_jshen97_leochans_wangyp')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:henryhcy_jshen97_leochans_wangyp#Salesmen',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_cvs = doc.entity('dat:cvs',
                                        {'prov:label': 'cvs boston', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_counted = doc.entity('dat:countEvictionCrimeCV',
                                  {'prov:label': 'counted E&C of cvs', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_corr = doc.entity('dat:correlationCVS',
                                      {'prov:label': 'correlation results of cvs', prov.model.PROV_TYPE: 'ont:DataSet'})
        solve_salesmen = doc.activity('log:uuid' + str(uuid.uuid4()), start_time, end_time)

        doc.wasAssociatedWith(solve_salesmen, this_script)
        doc.usage(solve_salesmen, resource_counted, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(solve_salesmen, resource_corr, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(solve_salesmen, resource_cvs, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        solution = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#solution',
                          {prov.model.PROV_LABEL: 'solve the salesmen problem',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(solution, this_script)
        doc.wasGeneratedBy(solution, solve_salesmen, end_time)
        doc.wasDerivedFrom(solution, resource_counted, solve_salesmen, solve_salesmen, solve_salesmen)
        doc.wasDerivedFrom(solution, resource_corr, solve_salesmen, solve_salesmen, solve_salesmen)
        doc.wasDerivedFrom(solution, resource_cvs, solve_salesmen, solve_salesmen, solve_salesmen)

        repo.logout()

        return doc

# debug

Salesmen.execute()
'''
doc = Salesmen.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''