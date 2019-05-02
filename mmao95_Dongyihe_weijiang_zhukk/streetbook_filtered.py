import pandas as pd
import requests
import json
import dml
import prov.model
import datetime
import uuid
import csv
from io import StringIO
import json
import pymongo
import numpy as np


class streetbook_filtered(dml.Algorithm):
    contributor = 'mmao95_Dongyihe_weijiang_zhukk'
    reads = [contributor + '.cau_landmark_merge', contributor + '.public_libraries',
             contributor + '.filtered_famous_people_streets', contributor + '.boston_traffic']
    writes = [contributor + '.streetbook_filtered',
              contributor + '.streetbook_alternate']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        contributor = 'mmao95_Dongyihe_weijiang_zhukk'
        reads = [contributor + '.cau_landmark_merge', contributor + '.public_libraries',
                 contributor + '.filtered_famous_people_streets', contributor + '.boston_traffic']
        writes = [contributor + '.streetbook_filtered',
                  contributor + '.streetbook_alternate']

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        CAU_landmark_merge_list = list(repo[reads[0]].find())
        CAU_landmark_merge_df = pd.DataFrame(CAU_landmark_merge_list)
        CAU_landmark_merge_list = np.array(CAU_landmark_merge_df).tolist()

        Public_libraries_list = list(repo[reads[1]].find())
        Public_libraries_df = pd.DataFrame(Public_libraries_list)
        Public_libraries_list = np.array(Public_libraries_df).tolist()

        Streetbook_list = list(repo[reads[2]].find())
        Streetbook_df = pd.DataFrame(Streetbook_list)
        Streetbook_list = np.array(Streetbook_df).tolist()

        # process CAU_landmark dataset
        CAU_landmark = [(adress, name) for (
            adress, name, neighbourhood, id) in CAU_landmark_merge_list]
        CAU_landmark = [(adress.split(",")[0], name)
                        for (adress, name) in CAU_landmark]

        # process public library dataset
        Public_libraries = [adress for (
            adress, branchName, city, latitude, longitude, numbers, zipcode, id) in Public_libraries_list]

        # process street book dataset
        Streetbook = [(fullName, streetName, zipcode) for (
            fullName, gender, gender2, nameLength, rank, streetName, zipcode, id) in Streetbook_list]
        Streetbook = [(fullName, streetName.split(' ')[0], zipcode)
                      for (fullName, streetName, zipcode) in Streetbook]

        Boston_traffic_list = list(repo[reads[3]].find())
        Boston_traffic_df = pd.DataFrame(Boston_traffic_list)
        Boston_traffic_list = np.array(Boston_traffic_df).tolist()

        # process boston traffic dataset
        Boston_traffic = [(streetName, number)
                          for (streetName, number, id) in Boston_traffic_list]

        # filter CAU_landmark
        for i in range(len(Streetbook) - 1, -1, -1):
            for j in range(0, len(CAU_landmark)):
                if ((Streetbook[i][1] in CAU_landmark[j][0]) or (Streetbook[i][1] in CAU_landmark[j][1])):
                    del Streetbook[i]

        # filter public libraries
        for i in range(len(Streetbook) - 1, -1, -1):
            for j in range(0, len(Public_libraries)):
                if (Streetbook[i][1] in Public_libraries[j]):
                    del Streetbook[i]

        # filter public library names
        Public_library_names = pd.read_csv(
            "http://datamechanics.io/data/public_library_%20names.csv").values.tolist()

        for i in range(len(Streetbook) - 1, -1, -1):
            for j in range(0, len(Public_library_names)):
                if (Streetbook[i][1] in Public_library_names[j]):
                    del Streetbook[i]

        for i in range(len(Streetbook) - 1, -1, -1):
            for j in range(0, len(Public_library_names)):
                if (Streetbook[i][1] in Public_library_names[j]):
                    del Streetbook[i]

        #  filter reduant zipCode
        for i in range(len(Streetbook) - 1, -1, -1):
            if(Streetbook[i][2].find("-") != -1):
                del Streetbook[i]

        for i in range(len(Streetbook) - 1, -1, -1):
            if(Streetbook[i][0] == '' or Streetbook[i][1] == '' or Streetbook[i][2] == ''):
                del Streetbook[i]

        # filter boston traffic
        for i in range(len(Streetbook) - 1, -1, -1):
            for j in range(0, len(Boston_traffic)):
                if(Streetbook[i][1] in Boston_traffic[j][0]):
                    del Streetbook[i]

        Streetbook_update = []
        for i in range(0, len(Streetbook)):
            if(Streetbook[i][1] not in Streetbook_update):
                Streetbook_update += [Streetbook[i][1]]

        # print(Streetbook)
        Streetbook_num = []
        for i in range(0, len(Streetbook_update)):
            index = 0
            for j in range(0, len(Streetbook)):
                if(Streetbook_update[i] == Streetbook[j][1]):
                    index = index + 1
            Streetbook_num += [(Streetbook_update[i], index)]

        Streetbook_num = sorted(Streetbook_num, key=lambda x: x[1])
        Streetbook_num = [(streetName, number) for (
            streetName, number) in Streetbook_num if number > 1.0]

        columnName = ['FullName', 'StreetName', 'Zipcode']
        df = pd.DataFrame(columns=columnName, data=Streetbook)
        data = json.loads(df.to_json(orient="records"))

        columnName = ['StreetName', 'RedundantTime']
        df = pd.DataFrame(columns=columnName, data=Streetbook_num)
        data1 = json.loads(df.to_json(orient="records"))

        repo.dropCollection('streetbook_filtered')
        repo.createCollection('streetbook_filtered')
        repo[writes[0]].insert_many(data)

        repo.dropCollection('streetbook_alternate')
        repo.createCollection('streetbook_alternate')
        repo[writes[1]].insert_many(data1)

        repo[writes[0]].metadata({'complete': True})
        print(repo[writes[0]].metadata())
        [record for record in repo[writes[0]].find()]

        repo[writes[1]].metadata({'complete': True})
        print(repo[writes[1]].metadata())
        [record for record in repo[writes[1]].find()]

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        contributor = 'mmao95_Dongyihe_weijiang_zhukk'
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://www.50states.com/bio/mass.htm')

        this_script = doc.agent('alg:'+contributor+'#streetbook_filtered', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        res_fp = doc.entity('bdp:fp', {'prov:label':'Public Libraries', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        res_sb = doc.entity('bdp:sb', {'prov:label':'Boston Traffic', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        filter_names = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(filter_names, this_script)
        doc.usage(filter_names, res_fp, startTime, None,
            {prov.model.PROV_TYPE: 'ont:Computation',
            'ont:Computation':'Selection, Differentiate'
            }
        )
        doc.usage(filter_names, res_sb, startTime, None,
            {prov.model.PROV_TYPE: 'ont:Computation',
            'ont:Computation':'Selection, Differentiate'
            }
        )
        result = doc.entity('dat:'+contributor+'#streetbook_filtered', {prov.model.PROV_LABEL:'Streetbook filtered', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(result, this_script)
        doc.wasGeneratedBy(result, filter_names, endTime)
        doc.wasDerivedFrom(result, res_fp, filter_names, filter_names, filter_names)
        doc.wasDerivedFrom(result, res_sb, filter_names, filter_names, filter_names)
        repo.logout()
                  
        return doc

## eof
