import dml
import prov.model
import datetime
import uuid


class JobAnalysis(dml.Algorithm):
    contributor = 'ojhamb_runtongy_sgullett_zybu'
    reads = ['ojhamb_runtongy_sgullett_zybu.linkedin']
    writes = ['ojhamb_runtongy_sgullett_zybu.JobAnalysis', 'ojhamb_runtongy_sgullett_zybu.JobAnalysis2',
              'ojhamb_runtongy_sgullett_zybu.JobAnalysis3']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ojhamb_runtongy_sgullett_zybu', 'ojhamb_runtongy_sgullett_zybu')

        repo.dropCollection("JobAnalysis")
        repo.dropCollection("JobAnalysis2")
        repo.dropCollection("JobAnalysis3")
        repo.createCollection("JobAnalysis")
        repo.createCollection("JobAnalysis2")
        repo.createCollection("JobAnalysis3")

        # Do selection to get only the data related to people who are currently working
        employed = [x for x in repo.ojhamb_runtongy_sgullett_zybu.linkedin.find()
                    if x["Organization End 1"] == "PRESENT"]
        '''
                titles = "Developer", "Engineer"
                engineers = []

                # Select only data related to Engineers/Developers
                for ppl in employed:
                    if any(title in ppl["Title"] for title in titles):
                        engineers.append(ppl)
                    elif any(title in ppl["Organization Title 1"] for title in titles):
                        engineers.append(ppl)
                    elif any(title in ppl["Organization Title 2"] for title in titles):
                        engineers.append(ppl)
                    elif any(title in ppl["Organization Title 3"] for title in titles):
                        engineers.append(ppl)
        '''
        # get the skill list
        skills = {}
        universities = {}
        degrees = {"Bachelor" : 0, "Master" : 0, "Phd" : 0, "Other" : 0}

        for ppl in employed:
            skill_str = ppl["Skills"]
            i = 0
            j = 0
            switch = 0
            while i < len(skill_str)-3:
                if switch == 1:
                    if skill_str[j:i-2] in skills:
                        skills[skill_str[j:i-2]] += 1
                    else:
                        skills[skill_str[j:i-2]] = 1
                    switch -= 1
                elif skill_str[i] == ":":
                    switch += 1
                elif skill_str[i] == ",":
                    j = i + 2
                i += 1

        for ppl in employed:
            uni_str = ppl["Education 1"]
            if uni_str in universities:
                universities[uni_str] += 1
            else:
                universities[uni_str] = 1

        degree1 = "Bachelor", "BSC", "B.Sc", "BS"
        degree2 = "Master", "MS"
        degree3 = "Phd", "Doctor"
        for ppl in employed:
            degree_str = ppl["Education Degree 1"]
            if type(degree_str) == str and any(degree in degree_str for degree in degree1):
                degrees["Bachelor"] += 1
            elif type(degree_str) == str and any(degree in degree_str for degree in degree2):
                degrees["Master"] += 1
            elif type(degree_str) == str and any(degree in degree_str for degree in degree3):
                degrees["Phd"] += 1
            else:
                degrees["Other"] += 1

#        print(skills);

        # get the most necessary skill
        nes_skills = sorted(skills, key=skills.get, reverse=True)[:10]
        top_uni = sorted(universities, key=universities.get, reverse=True)[:5]

        skill_list = []
        uni_list = []
        degree_list = []

        for x in nes_skills:
            skill_list.append({x:skills[x]})

        for x in top_uni:
            uni_list.append({x:universities[x]})

        degree_list.append(degrees)


        repo['ojhamb_runtongy_sgullett_zybu.JobAnalysis'].insert_many(skill_list)
        repo['ojhamb_runtongy_sgullett_zybu.JobAnalysis2'].insert_many(uni_list)
        repo['ojhamb_runtongy_sgullett_zybu.JobAnalysis3'].insert_many(degree_list)
        repo['ojhamb_runtongy_sgullett_zybu.JobAnalysis'].metadata({'complete': True})
        repo['ojhamb_runtongy_sgullett_zybu.JobAnalysis2'].metadata({'complete': True})
        repo['ojhamb_runtongy_sgullett_zybu.JobAnalysis3'].metadata({'complete': True})
#        print(repo['ojhamb_runtongy_sgullett_zybu.JobAnalysis'].metadata())

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

        this_script = doc.agent('alg:ojhamb_runtongy_sgullett_zybu#JobAnalysis',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        linkedin = doc.entity('dat:ojhamb_runtongy_sgullett_zybu#linkedin',
                              {'prov:label': 'Linkedin Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        do_JobAnalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(do_JobAnalysis, this_script)

        doc.usage(do_JobAnalysis, linkedin, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        JobAnalysis = doc.entity('dat:ojhamb_runtongy_sgullett_zybu#JobAnalysis',
                          {prov.model.PROV_LABEL: 'Necessary skills for Developers', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(JobAnalysis, this_script)
        doc.wasGeneratedBy(JobAnalysis, do_JobAnalysis, endTime)
        doc.wasDerivedFrom(JobAnalysis, linkedin, do_JobAnalysis, do_JobAnalysis, do_JobAnalysis)

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