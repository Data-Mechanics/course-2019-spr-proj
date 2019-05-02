import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mergeOwnerAgeWithWealth(dml.Algorithm):
    
    contributor = 'asadeg02_gxy9598'
    reads = ['asadeg02_gxy9598.voters_info', 'asadeg02_gxy9598.properties_per_voter']
    writes = ['asadeg02_gxy9598.age_numProperties_phone_ocupation_addr']     
  
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')        

        voters_info =  repo.asadeg02_gxy9598.voters_info.find()
        prop_per_voter = repo.asadeg02_gxy9598.properties_per_voter.find()      

        
        #create a dictionary for voter file with first name + last name as key
        voters_info_dict = {}        
        for doc in voters_info:            
            if doc['Last Name'] != None and doc['First Name'] != None:
                key = doc['Last Name'] + ' ' + doc['First Name']
                voters_info_dict[key]  = doc 
            if doc['Last Name'] != None:
                key = doc['Last Name']
                voters_info_dict[key]  = doc 
            if doc['First Name'] != None:
                key = doc['First Name']
                voters_info_dict[key]  = doc             
        
        #merge with properties with voter 
        result = []
        for doc in prop_per_voter:                        
            if doc['OWNER'] in voters_info_dict:
                voter_info_doc = voters_info_dict[doc['OWNER']]
                doc['PHONE'] = (voters_info_dict[doc['OWNER']]['Phone']).split('.')[0]
                age = datetime.datetime.now().year - int((voters_info_dict[doc['OWNER']]['DOB']).split('-')[0])
                doc['AGE'] = age
                doc['OCCUPATION'] = voters_info_dict[doc['OWNER']]['Occupation']                              
                result.append(doc)
        
       
        repo.dropCollection('asadeg02_gxy9598.age_numProperties_phone_ocupation_addr')
        repo.createCollection('asadeg02_gxy9598.age_numProperties_phone_ocupation_addr')

        _id = 0
        for doc in result:
            doc['_id'] = _id
            _id += 1              
            repo["asadeg02_gxy9598.age_numProperties_phone_ocupation_addr"].insert(doc)

        repo["asadeg02_gxy9598.age_numProperties_phone_ocupation_addr"].metadata({'complete':True})
        print(repo["asadeg02_gxy9598.age_numProperties_phone_ocupation_addr"].metadata())
        print('Finished Merging Voter Info With Prop Per Voter')

        repo.logout()
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

      ###############################################################################################################################################

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:asadeg02_gxy9598#mergeOwnerAgeWithWealth', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})        
        mergeOwnerAgeWithWealth = doc.activity('log:uuid'+ str(uuid.uuid4()), startTime, endTime, 
                                        {'prov:label':'Merges Persoanl Info of buidings Owner with Porp Info', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith( mergeOwnerAgeWithWealth, this_script) 
        
        resource_voters_info = doc.entity('dat:asadeg02_gxy9598#voters_info', {prov.model.PROV_LABEL:'South Boston Voters Info', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_properties_per_voter = doc.entity('dat:asadeg02_gxy9598#properties_per_voter', {prov.model.PROV_LABEL:'ProPerties Per Voter', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(mergeOwnerAgeWithWealth, resource_voters_info, startTime)
        doc.usage(mergeOwnerAgeWithWealth, resource_properties_per_voter, startTime)        

        age_numProperties_phone_ocupation_addr = doc.entity('dat:asadeg02_gxy9598#age_numProperties_phone_ocupation_addr', 
                                       {prov.model.PROV_LABEL:'Personal Info like Age and Occupation Of owners Merged with Thier Porperties Info', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(age_numProperties_phone_ocupation_addr, this_script)
        doc.wasGeneratedBy(age_numProperties_phone_ocupation_addr, mergeOwnerAgeWithWealth, endTime)
        doc.wasDerivedFrom(age_numProperties_phone_ocupation_addr, resource_voters_info, mergeOwnerAgeWithWealth, mergeOwnerAgeWithWealth, mergeOwnerAgeWithWealth)
        doc.wasDerivedFrom(age_numProperties_phone_ocupation_addr, resource_properties_per_voter, mergeOwnerAgeWithWealth, mergeOwnerAgeWithWealth, mergeOwnerAgeWithWealth)
         
        
        return doc