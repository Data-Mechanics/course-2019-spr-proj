import dml
import datetime
import json
import prov.model
import pprint
import scipy.stats
import uuid

class CorrelationCVS(dml.Algorithm):
    contributor = "henryhcy_jshen97_leochans_wangyp"
    reads = ['henryhcy_jshen97_leochans_wangyp.ratingEviction',
             'henryhcy_jshen97_leochans_wangyp.ratingCrime']
    writes = ['henryhcy_jshen97_leochans_wangyp.correlationCVS']

    @staticmethod
    def execute(trial=False):
        start_time = datetime.datetime.now()

        # set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('henryhcy_jshen97_leochans_wangyp', 'henryhcy_jshen97_leochans_wangyp')

        # create the result collections
        repo.dropCollection('correlationCVS')
        repo.createCollection('correlationCVS')

        # calculate the correlation and p-value between ratings and evictions
        # ratings are translated into 100 score bases and
        # those with less 100 cases are excluded as invaluable data points
        dataset_eviction = []
        for document in repo.henryhcy_jshen97_leochans_wangyp.ratingEviction.find({'rating_eviction.1': {'$gt': 100}}):

            dataset_eviction.append(document['rating_eviction'])

        x_evi = [i[0]*20 for i in dataset_eviction]
        y_evi = [i[1] for i in dataset_eviction]
        result_eviction = scipy.stats.pearsonr(x_evi, y_evi)
        d_evi = {
            "document_type": "rating_eviction_correlation",
            "corr": result_eviction[0],
            "p_value": result_eviction[1]
        }
        repo.henryhcy_jshen97_leochans_wangyp.correlationCVS.insert_one(d_evi)

        # calculate the correlation and p-value between ratings and crimes
        # ratings are translated into 100 score bases and
        # those with less 100 cases are excluded as invaluable data points
        dataset_crime = []
        for document in repo.henryhcy_jshen97_leochans_wangyp.ratingCrime.find({'rating_crime.1': {'$gt': 100}}):
            dataset_crime.append(document['rating_crime'])

        x_cri = [i[0]*20 for i in dataset_crime]
        y_cri = [i[1] for i in dataset_crime]
        result_crime = scipy.stats.pearsonr(x_cri, y_cri)
        d_cri = {
            "document_type": "rating_crime_correlation",
            "corr": result_crime[0],
            "p_value": result_crime[1]
        }
        repo.henryhcy_jshen97_leochans_wangyp.correlationCVS.insert_one(d_cri)


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
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:henryhcy_jshen97_leochans_wangyp#CorrelationCVS',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_ratingEvi = doc.entity('dat:rat_evi',
                                        {'prov:label': 'rating and eviction pair', prov.model.PROV_TYPE: 'ont:DataSet'})
        resource_ratingCri = doc.entity('dat:rat_cri',
                                        {'prov:label': 'rating and crime pair', prov.model.PROV_TYPE: 'ont:DataSet'})
        compute_corr = doc.activity('log:uuid' + str(uuid.uuid4()), start_time, end_time)

        doc.wasAssociatedWith(compute_corr, this_script)
        doc.usage(compute_corr, resource_ratingEvi, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(compute_corr, resource_ratingCri, start_time, None, {prov.model.PROV_TYPE: 'ont:Computation'})

        corr = doc.entity('dat:henryhcy_jshen97_leochans_wangyp#correlationCVS',
                          {prov.model.PROV_LABEL: 'Compute correlation of rating against E&C', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(corr, this_script)
        doc.wasGeneratedBy(corr, compute_corr, end_time)
        doc.wasDerivedFrom(corr, resource_ratingEvi, compute_corr, compute_corr, compute_corr)
        doc.wasDerivedFrom(corr, resource_ratingCri, compute_corr, compute_corr, compute_corr)

        repo.logout()

        return doc


# debug


CorrelationCVS.execute()
'''
doc = CorrelationCVS.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''