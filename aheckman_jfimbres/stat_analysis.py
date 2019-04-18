import dml
import prov.model
import datetime
import uuid
import aheckman_jfimbres.Helpers.transformations as t
import aheckman_jfimbres.Helpers.stats as s
import scipy.stats

class stat_analysis(dml.Algorithm):
    contributor = 'aheckman_jfimbres'
    reads = ['aheckman_jfimbres.carbon_efficacy', 'aheckman_jfimbres.emissions_per_capita']
    writes = ['aheckman_jfimbres.Stats']
    @staticmethod
    def execute(trial = False):
            '''Run a statistical analysis gathering data comparing carbon efficacy to emissions per capita'''
            startTime = datetime.datetime.now()

            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

            efficacy = repo.aheckman_jfimbres.carbon_efficacy.find({})
            epc = repo.aheckman_jfimbres.emissions_per_capita.find({})

            eff = t.project(efficacy, lambda x: x)
            epc = t.project(epc, lambda x: x)

            eff = [list(x.values()) for x in eff]
            epc = [list(x.values()) for x in epc]
            eff = eff[0][1:]
            epc = epc[0][1:]

            corr = scipy.stats.pearsonr(eff, epc)
            corr = {"Stats": corr}

            repo.dropCollection("Stats")
            repo.createCollection("Stats")
            repo['aheckman_jfimbres.Stats'].insert(corr)

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

        this_script = doc.agent('alg:aheckman_jfimbres#stat_analysis',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        find_stats = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        carbon_efficacy = doc.entity('dat:aheckman_jfimbres#carbon_efficacy',
                                  {prov.model.PROV_LABEL: 'Carbon Efficacy',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})
        epc = doc.entity('dat:aheckman_jfimbres#emissions_per_capita',
                            {prov.model.PROV_LABEL: 'Emissions per Capita',
                             prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAssociatedWith(find_stats, this_script)

        doc.usage(find_stats, carbon_efficacy, startTime, None)
        doc.usage(find_stats, epc, startTime, None)

        Stats = doc.entity('dat:aheckman_jfimbres#Stats',
                                          {prov.model.PROV_LABEL: 'Stats for analysis, namely correlation coefficient and p-values',
                                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(Stats, this_script)
        doc.wasGeneratedBy(Stats, find_stats, endTime)
        doc.wasDerivedFrom(Stats, carbon_efficacy, find_stats)
        doc.wasDerivedFrom(Stats, epc, find_stats)

        return doc