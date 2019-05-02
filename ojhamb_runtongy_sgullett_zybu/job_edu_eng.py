import dml
import prov.model
import datetime
import uuid
from math import sqrt


class job_edu_eng(dml.Algorithm):
    contributor = 'ojhamb_runtongy_sgullett_zybu'
    reads = ['ojhamb_runtongy_sgullett_zybu.linkedin']
    writes = ['ojhamb_runtongy_sgullett_zybu.job_edu_eng']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ojhamb_runtongy_sgullett_zybu', 'ojhamb_runtongy_sgullett_zybu')

        repo.dropCollection("job_edu_eng")
        repo.createCollection("job_edu_eng")

        # Generate a three dimensional data based on our dataset

        data_list = []

        i = 0
        titles = "Bachelor", "BSC", "Master"
        for ppl in repo.ojhamb_runtongy_sgullett_zybu.linkedin.find():
            job_index = 0
            edu_index = 0
            eng_index = 0
            if ppl["Organization End 1"] == "PRESENT":
                job_index += 1
            if type(ppl["Education Degree 1"]) == str and any(title in ppl["Education Degree 1"] for title in titles):
                edu_index += 1
            elif type(ppl["Education Degree 1"]) == str and any(title in ppl["Education Degree 2"] for title in titles):
                edu_index += 1
            elif type(ppl["Education Degree 1"]) == str and any(title in ppl["Education Degree 3"] for title in titles):
                edu_index += 1
            if ppl["English"] == 1:
                eng_index += 1
            data_list.append((job_index, edu_index, eng_index))
            i += 1

        print(data_list)

        job = [jobs for (jobs, edus, engs) in data_list]
        edu = [edus for (jobs, edus, engs) in data_list]
        eng = [engs for (jobs, edus, engs) in data_list]

        def avg(x):  # Average
            return sum(x) / len(x)

        def stddev(x):  # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

        def cov(x, y):  # Covariance.
            return sum([(xi - avg(x)) * (yi - avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

        def corr(x, y):  # Correlation coefficient.
            if stddev(x) * stddev(y) != 0:
                return cov(x, y) / (stddev(x) * stddev(y))

        corr_job_edu_eng = []

        corr_job_edu_eng.append(
            {"Correlation Coefficient between Working Status and Educational Level": corr(job, edu)})
        corr_job_edu_eng.append(
            {"Correlation Coefficient between Working Status and English Capability": corr(job, eng)})
        corr_job_edu_eng.append(
            {"Correlation Coefficient between Educational Level and English Capability": corr(edu, eng)})

        repo['ojhamb_runtongy_sgullett_zybu.job_edu_eng'].insert_many(corr_job_edu_eng)
        repo['ojhamb_runtongy_sgullett_zybu.job_edu_eng'].metadata({'complete': True})
#        print(repo['ojhamb_runtongy_sgullett_zybu.job_edu_eng'].metadata())

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

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ojhamb_runtongy_sgullett_zybu', 'ojhamb_runtongy_sgullett_zybu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        this_script = doc.agent('alg:ojhamb_runtongy_sgullett_zybu#job_edu_eng',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        linkedin = doc.entity('dat:ojhamb_runtongy_sgullett_zybu#linkedin',
                              {'prov:label': 'Linkedin Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_coefficient = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_coefficient, this_script)

        doc.usage(get_coefficient, linkedin, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        job_edu_eng = doc.entity('dat:ojhamb_runtongy_sgullett_zybu#job_edu_eng',
                          {prov.model.PROV_LABEL: 'Correlation Coefficient between Attributes', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(job_edu_eng, this_script)
        doc.wasGeneratedBy(job_edu_eng, get_coefficient, endTime)
        doc.wasDerivedFrom(job_edu_eng, linkedin, get_coefficient, get_coefficient, get_coefficient)

        repo.logout()

        return doc


'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
edu_work.execute()
doc = edu_work.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof