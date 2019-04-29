import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import sqrt

def project(R, p):
    return [p(t) for t in R]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def select(R, s):
    return [t for t in R if s(t)]

def not_null(r):
    return (r[0] != None and r[1] != None) and ((r[0]) != '-1' and (r[1] != '-1'))

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

class accidentStats(dml.Algorithm):
    contributor = 'jkmoy_mfflynn'
    reads = ['jkmoy_mfflynn.accidentLongLat']
    writes = ['jkmoy_mfflynn.accidentStats']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jkmoy_mfflynn', 'jkmoy_mfflynn')

        
        # transformation here
        collection = list(repo['jkmoy_mfflynn.accidentLongLat'].find())
        data = [(doc['mean'], doc['types'], doc['num_points']) for doc in collection]

        count_for_means = []

        #Count up the occurence of each type of incident for each cluster
        for i in range(len(data)):
            mean = data[i][0]
            types = data[i][1]
            num = data[i][2]
            x = [(j, 1) for j in types]

            y = aggregate(x, sum)

            count_for_means.append([i, mean, num, y])
    
        x = []
        for i in range(len(count_for_means)):
            count_mean = count_for_means[i][3]
            for j in range(len(count_mean)):
                x.append([count_mean[j][0], count_mean[j][1]])

        #Number of occurences per incident in each cluster                
        data_per_incident = aggregate(x, (lambda x: x))
    
        stats_results = []
        for i in range(len(data_per_incident)):
            incident = data_per_incident[i]
            results = data_per_incident[i][1]
            
            #Account for if the type of incident is not in every cluster
            if (len(results) != len(data)):
                zeros = [0 for j in range(len(data) - len(results))]
                results += zeros
                
            
            #find avg and stddev for that type of incident in all clusters
            avg_i = avg(results)
            stddev_i = stddev(results)
            stats_results.append((incident[0], round(avg_i,3), round(stddev_i,3)))
            
        final_dataset = [{'incident':tup[0], 'mean':tup[1], 'stddev':tup[2]} for tup in stats_results]
        
        repo.dropCollection('jkmoy_mfflynn.accidentStats')
        repo.createCollection('jkmoy_mfflynn.accidentStats')
        
        repo['jkmoy_mfflynn.accidentStats'].insert_many(final_dataset)
        repo['jkmoy_mfflynn.accidentStats'].metadata({'complete': True})

        print(repo['jkmoy_mfflynn.accidentStats'].metadata())

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
        repo.authenticate('jkmoy_mfflynn', 'jkmoy_mfflynn')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jkmoy_mfflynn') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/jkmoy_mfflynn') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:jkmoy_mfflynn#accidentStats', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:accStatistics', {'prov:label':'Stats of accidents', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_a_stats = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_a_stats, this_script)
        doc.usage(get_a_stats, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        a_stats = doc.entity('dat:jkmoy_mfflynn#means', {prov.model.PROV_LABEL:'Stats %', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(a_stats, this_script)
        doc.wasGeneratedBy(a_stats, get_a_stats, endTime)
        doc.wasDerivedFrom(a_stats, resource, get_a_stats, get_a_stats, get_a_stats)

        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
averagePerDepartment.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
accidentStats.execute()
## eof
