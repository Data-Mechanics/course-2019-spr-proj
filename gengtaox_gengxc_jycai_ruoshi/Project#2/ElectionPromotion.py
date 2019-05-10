import dml
import prov.model
import datetime
import uuid
import json
import xlrd
from scipy.optimize import linprog
import numpy as np


class ElectionPromotion(dml.Algorithm):
    contributor = 'gengtaox_gengxc_jycai_ruoshi'
    reads = []
    writes = ['gengtaox_gengxc_jycai_ruoshi.ElectionPromotion']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gengtaox_gengxc_jycai_ruoshi', 'gengtaox_gengxc_jycai_ruoshi')

        data = xlrd.open_workbook("gengtaox_gengxc_jycai_ruoshi/registered_voters_xtabs.xlsx")

        table = data.sheets()[0]

        c = []

        population = [table.col_values(61)]

        for i in range(3, 12):
            c.append(-1 * population[0][i])

        A_ub = [[-1, -1, -1, 0, 0, 0, 0, 0, 0], [0, 0, 0, -1, -1, -1, 0, 0, 0], [0, 0, 0, 0, 0, 0, -1, -1, -1],
                [1, 1, 1, 1, 1, 1, 1, 1, 1]]
        B_ub = [-1, -1, -1, 4]
        A_eq = [[0, 0, 1, 1, 0, 0, 0, 0, 0]]
        B_eq = [1]

        r = linprog(c, A_ub, B_ub, A_eq, B_eq,
                    bounds=((0, 1), (0, 1), (0, 1), (0, 1), (0, 1), (0, 1), (0, 1), (0, 1), (0, 1)))
        selected = []
        for i in range(0,len(r['x'])):
            selected.append({
                'Congress District %d selected'%(i+1): r['x'][i],
            })
        selected.append({
            'Covered population': -1*r['fun']
        })

        repo.dropCollection("ElectionPromotion")
        repo.createCollection("ElectionPromotion")

        repo['gengtaox_gengxc_jycai_ruoshi.ElectionPromotion'].insert_many(selected)
        repo['gengtaox_gengxc_jycai_ruoshi.ElectionPromotion'].metadata({'complete': True})
        print(repo['gengtaox_gengxc_jycai_ruoshi.ElectionPromotion'].metadata())

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

        doc.add_namespace('ont', 'http://datamechanics.io/ontology')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('xtab', 'http://datamechanics.io/voter/')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:gengtaox_gengxc_jycai_ruoshi#ElectionPromotion',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_web = doc.entity('xtab:non', 
                                {'prov:label': 'Election Promotion',
                                                prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension': 'xlsx'})

        get_web = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_web, this_script)

        doc.usage(get_web, resource_web, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:DataSet'
                   })

        ElectionPromotion = doc.entity('dat:gengtaox_gengxc_jycai_ruoshi#ElectionPromotion',
                           {prov.model.PROV_LABEL: 'Best election promotion solution', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(ElectionPromotion, this_script)

        doc.wasGeneratedBy(ElectionPromotion, get_web, endTime)

        doc.wasDerivedFrom(ElectionPromotion, resource_web, get_web, get_web, get_web)

        return doc
## eof
if __name__ == "__main__":
    ElectionPromotion.execute()
    doc = ElectionPromotion.provenance()
    print(doc.get_provn())
    print(json.dumps(json.loads(doc.serialize()), indent=4))

