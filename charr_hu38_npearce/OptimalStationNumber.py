import dml
import prov.model
import datetime
import uuid

from sklearn.linear_model import LinearRegression
import matplotlib

class OptimalStationNumber(dml.Algorithm):
    contributor = 'charr_hu38_npearce'
    reads = ['charr_hu38_npearce.unionpopbike']
    writes = ['charr_hu38_npearce.optstationnum']

    @staticmethod
    def execute(trial = False):
        '''Union dataset containing bike data into the dataset containing city census information'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('charr_hu38_npearce', 'charr_hu38_npearce')

        repo.dropCollection("optstationnum")
        repo.createCollection("optstationnum")
        
        data = repo.charr_hu38_npearce.unionpopbike.find()
        
        X = []
        y = []
        for city in data:
            X.append(city['stations']/city['population'])
            y.append(city['tot_bike_time'])
        
        repo['charr_hu38_npearce.optstationnum'].insert_many(docs)                          #Join on two data sets (Non Trivial Transformation #3)
        repo['charr_hu38_npearce.optstationnum'].metadata({'complete':True})
        
        #We treat this as a single nontrivial transformation, as it is a join that involves a product, selection, and projection

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
        repo.authenticate('charr_hu38_npearce', 'charr_hu38_npearce')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:charr_hu38_npearce#UnionPopBike', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Union of Bike and Census Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_unionpopbike = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_unionpopbike, this_script)
        doc.usage(get_unionpopbike, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )

        unionpopbike = doc.entity('dat:charr_hu38_npearce#unionpopbike', {prov.model.PROV_LABEL:'Total time spent on bike and population data for 4 cities', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(unionpopbike, this_script)
        doc.wasGeneratedBy(unionpopbike, get_unionpopbike, endTime)
        doc.wasDerivedFrom(unionpopbike, resource, get_unionpopbike, get_unionpopbike, get_unionpopbike)
        
        repo.logout()
                  
        return doc
        

# This is UnionPopBike code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
UnionPopBike.execute()
#doc = UnionPopBike.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof