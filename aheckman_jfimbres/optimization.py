import dml
import prov.model
import datetime
import uuid
import aheckman_jfimbres.Helpers.transformations as t
from sklearn.cluster import KMeans

class optimization(dml.Algorithm):
    contributor = 'aheckman_jfimbres'
    reads = ['aheckman_jfimbres.opinion_by_efficacy']
    writes = ['']
    @staticmethod
    def execute(trial = False):
        '''Run a statistical analysis gathering data comparing carbon efficacy to emissions per capita'''
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        ops_by_efficacy = repo.aheckman_jfimbres.opinion_by_efficacy.find({})
        ops = t.project(ops_by_efficacy, lambda x: x)
        ops = [list(x.values()) for x in ops]
        ops = ops[0][1:]

        #dem beliefs are ops[0][0][1]
        #rep beliefs are ops[0][1]

        q1 = []
        q2 = []
        q3 = []
        q4 = []
        q5 = []
        q6 = []
        q7 = []
        q8 = []
        q9 = []
        q10 = []
        q11 = []

        for i in range(len(ops)):
            q1 += ([ops[i][0][1][2], ops[i][0][1][3]], [ops[i][1][2], ops[i][1][3]])
            q2 += ([ops[i][0][1][4], ops[i][0][1][5]], [ops[i][1][4], ops[i][1][5]])
            q3 += ([ops[i][0][1][6], ops[i][0][1][7]], [ops[i][1][6], ops[i][1][7]])
            q4 += ([ops[i][0][1][8], ops[i][0][1][9]], [ops[i][1][8], ops[i][1][9]])
            q5 += ([ops[i][0][1][10], ops[i][0][1][11]], [ops[i][1][10], ops[i][1][11]])
            q6 += ([ops[i][0][1][12], ops[i][0][1][13]], [ops[i][1][12], ops[i][1][13]])
            q7 += ([ops[i][0][1][14], ops[i][0][1][15]], [ops[i][1][14], ops[i][1][15]])
            q8 += ([ops[i][0][1][16], ops[i][0][1][17]], [ops[i][1][16], ops[i][1][17]])
            q9 += ([ops[i][0][1][18], ops[i][0][1][19]], [ops[i][1][18], ops[i][1][19]])
            q10 += ([ops[i][0][1][20], ops[i][0][1][21]], [ops[i][1][20], ops[i][1][21]])
            q11 += ([ops[i][0][1][22], ops[i][0][1][23]], [ops[i][1][22], ops[i][1][23]])

        k1 = KMeans(n_clusters=2, random_state=0).fit(q1)
        k2 = KMeans(n_clusters=2, random_state=0).fit(q2)
        k3 = KMeans(n_clusters=2, random_state=0).fit(q3)
        k4 = KMeans(n_clusters=2, random_state=0).fit(q4)
        k5 = KMeans(n_clusters=2, random_state=0).fit(q5)
        k6 = KMeans(n_clusters=2, random_state=0).fit(q6)
        k7 = KMeans(n_clusters=2, random_state=0).fit(q7)
        k8 = KMeans(n_clusters=2, random_state=0).fit(q8)
        k9 = KMeans(n_clusters=2, random_state=0).fit(q9)
        k10 = KMeans(n_clusters=2, random_state=0).fit(q10)
        k11 = KMeans(n_clusters=2, random_state=0).fit(q11)

        inertias = [k1.inertia_, k2.inertia_, k3.inertia_, k4.inertia_, k5.inertia_, k6.inertia_, k7.inertia_,
                    k8.inertia_, k9.inertia_, k10.inertia_, k11.inertia_]

        inertias = {"disagreement on each question": inertias}

        repo.dropCollection("k_means")
        repo.createCollection("k_means")
        repo['aheckman_jfimbres.k_means'].insert(inertias)


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:aheckman_jfimbres#optimization',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        find_k_means = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        opinion_by_efficacy = doc.entity('dat:aheckman_jfimbres#opinion_by_efficacy',
                                  {prov.model.PROV_LABEL: 'Opinion by Efficacy',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAssociatedWith(find_k_means, this_script)

        doc.usage(find_k_means, opinion_by_efficacy, startTime, None)

        k_means = doc.entity('dat:aheckman_jfimbres#k_means',
                                          {prov.model.PROV_LABEL: 'Responses to important questions as k-means to represent disagreement',
                                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(k_means, this_script)
        doc.wasGeneratedBy(k_means, find_k_means, endTime)
        doc.wasDerivedFrom(k_means, opinion_by_efficacy, find_k_means)

        return doc