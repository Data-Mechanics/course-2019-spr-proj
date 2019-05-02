import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class schools(dml.Algorithm):
    contributor = 'aqu1'
    reads = []
    writes = ['aqu1.schools_data']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        # Dataset 1: Colleges in Boston
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.csv'
        college = pd.read_csv(url)
        # Clean data to only include traditional 4-year colleges and 2-year community colleges
        college['City'] = college['City'].replace(['North Dorchester', 'South Dorchester'], 'Dorchester')
        college['City'] = college['City'].replace('Fenway/Kenmore', 'Fenway')
        college['City'] = college['City'].replace('Financial District', 'Downtown')
        college = college[college['NumStudents13'] != 0]
        college = college[college['Latitude'] != 0]
        college = college[college['City'] != 'Chestnut Hill']
        college = college[college['Name'] != 'Massachusetts General Hospital Dietetic Internship']
        college['City'] = college['City'].mask(college['Name'] == 'Suffolk University', 'Downtown')
        college['City'] = college['City'].mask(college['Name'] == 'Everest Institute-Brighton', 'Brighton')
        college['City'] = college['City'].mask(college['Name'] == 'Saint John\'s Seminary', 'Brighton')
        college['City'] = college['City'].mask(college['Name'] == 'Harvard Business School', 'Allston')
        college['City'] = college['City'].replace('Chinatown', 'Downtown')
        colleges = pd.concat([college.Latitude, college.Longitude, college.City], axis = 1) # select columns

        # Dataset 2: Public Schools in Boston
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0.csv'
        school = pd.read_csv(url)
        school = school.rename(columns = {'CITY': 'City'})
        #print(school.City.unique())
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Boston HS Bldg', 'Chinatown')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Eliot Bldg', 'North End')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Abraham Lincoln Building', 'Back Bay')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Snowden Hs Bldg', 'Back Bay')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Mckinley Mackey Bldg', 'South End')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Hurley Bldg', 'South End')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Blackstone Bldg', 'South End')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Boston Latin Academy Bldg', 'Roxbury')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Boston Latin School Bldg', 'Fenway')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Health Careers Academy', 'Fenway')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Mckinley Peterborough Bldg', 'Fenway')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Mckinley St Mary\'S St Bldg', 'Fenway')
        school['City'] = school['City'].mask(school['BLDG_NAME'] == 'Church Street Bldg', 'Back Bay')
        school['City'] = school['City'].replace('Chinatown', 'Downtown')
        # Merge latitudes, longitudes, and neighborhoods of all colleges and public schools in Boston
        school = school[school['SCH_TYPE'] != 'ELC']
        schools = pd.concat([school.Y, school.X, school.City], axis = 1) # select columns 
        schools.columns = ['Latitude', 'Longitude', 'City']
        all_schools = colleges.append(schools) # aggregate data 
        all_schools = pd.DataFrame(all_schools)
        all_schools = json.loads(all_schools.to_json(orient = 'records'))
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aqu1', 'aqu1')
        
        repo.dropCollection("schools_data")
        repo.createCollection("schools_data")
        repo['aqu1.schools_data'].insert_many(all_schools)
        
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
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/') # Boston Open Data 
        
        this_script = doc.agent('alg:aqu1#schools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # Schools Report 
        resource_schools = doc.entity('bod:1d9509a8b2fd485d9ad471ba2fdb1f90_0.csv', {'prov:label':'Boston Public Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_colleges = doc.entity('bod:cbf14bb032ef4bd38e20429f71acb61a_2.csv', {'prov:label':'Boston Colleges', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_schools, this_script)
        doc.usage(get_schools, resource_schools, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_schools, resource_colleges, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        schools = doc.entity('dat:aqu1#schools_data', {prov.model.PROV_LABEL:'Boston Schools', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource_schools, get_schools, get_schools, get_schools)
        doc.wasDerivedFrom(schools, resource_colleges, get_schools, get_schools, get_schools)
        
        repo.logout()

        return doc

'''   
schools.execute()
doc = schools.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''