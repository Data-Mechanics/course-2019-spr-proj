import xlrd
import dml
import prov.model
import datetime
import uuid


class Ward_data(dml.Algorithm):
    contributor = 'gengtaox_gengxc_jycai_ruoshi'
    reads = []
    writes = ['gengtaox_gengxc_jycai_ruoshi.Ward_data', ]

    # Helper functions
    @staticmethod
    def convert_age_to_group(age):
        if isinstance(age, int) or isinstance(age, float):
            if 18 <= age <= 24:
                return "18-24"
            elif 25 <= age <= 34:
                return "25-34"
            elif 35 <= age <= 49:
                return "35-49"
            elif 50 <= age <= 64:
                return "50-64"
            elif age >= 65:
                return "65+"
            else:
                print(age)
                raise Exception("Incorrect Age")
        elif isinstance(age, str):
            if age == "18 to 24":
                return "18-24"
            elif age == "25 to 34":
                return "25-34"
            elif age == "35 to 49":
                return "35-49"
            elif age == "50 to 64":
                return "50-64"
            elif age == "65+":
                return "65+"
            else:
                print(age)
                raise Exception("Incorrect Age")
        else:
            raise Exception("Incorrect Age")

    @staticmethod
    def convert_gender_to_group(gender):
        if gender == "M":
            return "Male"
        elif gender == "F":
            return "Female"
        else:
            raise Exception("Incorrect gender")

    @staticmethod
    def get_ward_id(name):
        return int(name.split(" ")[1][1:])


    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gengtaox_gengxc_jycai_ruoshi', 'gengtaox_gengxc_jycai_ruoshi')

        # Process VAN File
        stat_for_van_file = {}
        for i in range(1, 23):
            stat_for_van_file[i] = {
                'Age': {
                    '18-24': 0,
                    '25-34': 0,
                    '35-49': 0,
                    '50-64': 0,
                    '65+': 0
                },
                'Gender': {
                    'Male': 0,
                    'Female': 0
                }
            }

        # col-F(5) sex, col-N(13) Age, col-Q(16) precinct (Boston W14 P14)
        data = xlrd.open_workbook("gengtaox_gengxc_jycai_ruoshi/VAN File - All Wards SPARK USE.xlsx")
        table = data.sheets()[0]

        for row in range(1, table.nrows):
            wid = Ward_data.get_ward_id(table.cell(row, 16).value)
            age_group = Ward_data.convert_age_to_group(table.cell(row, 13).value)

            gender_field = table.cell(row, 5).value
            if gender_field == "U":
                continue
            gender_group = Ward_data.convert_gender_to_group(gender_field)

            stat_for_van_file[wid]["Age"][age_group] += 1
            stat_for_van_file[wid]["Gender"][gender_group] += 1

        print(stat_for_van_file)


        # Process reg files
        stat_for_eligible = {}
        stat_for_registered = {}
        for i in range(1, 23):
            stat_for_eligible[i] = {
                'Age': {
                    '18-24': 0,
                    '25-34': 0,
                    '35-49': 0,
                    '50-64': 0,
                    '65+': 0
                },
                'Gender': {
                    'Male': 0,
                    'Female': 0
                }
            }

            stat_for_registered[i] = {
                'Age': {
                    '18-24': 0,
                    '25-34': 0,
                    '35-49': 0,
                    '50-64': 0,
                    '65+': 0
                },
                'Gender': {
                    'Male': 0,
                    'Female': 0
                }
            }

        # row N-R S(Unknown)
        data = xlrd.open_workbook("gengtaox_gengxc_jycai_ruoshi/registered_voters_xtabs.xlsx")
        table = data.sheets()[3]

        for row in range(180, 435):
            for col in range(13, 18):
                age_group = Ward_data.convert_age_to_group(table.cell(2, col).value)
                wid = Ward_data.get_ward_id(table.cell(row, 0).value)

                stat_for_eligible[wid]["Age"][age_group] += int(table.cell(row, col).value)
                stat_for_registered[wid]["Age"][age_group] += int(table.cell(row, col).value)

        print(stat_for_registered)

        # Process non-reg files

        # row N-R S(Unknown)
        data = xlrd.open_workbook("gengtaox_gengxc_jycai_ruoshi/non_registered_xtabs.xlsx")
        table = data.sheets()[3]

        for row in range(180, 435):
            for col in range(13, 18):
                age_group = Ward_data.convert_age_to_group(table.cell(2, col).value)
                wid = Ward_data.get_ward_id(table.cell(row, 0).value)

                stat_for_eligible[wid]["Age"][age_group] += int(table.cell(row, col).value)

        print(stat_for_eligible)

        # process election files
        stat_for_election = {}

        # row 2, 5, 8, 11, 14
        data = xlrd.open_workbook("gengtaox_gengxc_jycai_ruoshi/Historical Votes Cast by Boston Ward.xlsx")
        table = data.sheets()[0]

        for row in range(1, 14, 3):
            year = int(table.cell(row, 0).value.split(" ")[0])
            ward_election = {}
            for col in range(1, 44, 2):
                wid = int(table.cell(0, col).value)

                ward_election[wid] = {
                    "1st": {
                        "Name": table.cell(row, col + 1).value,
                        "Votes": table.cell(row, col).value
                    },
                    "2nd": {
                        "Name": table.cell(row + 1, col + 1).value,
                        "Votes": table.cell(row + 1, col).value
                    }
                }

            stat_for_election[year] = ward_election

        print(stat_for_election)

        # Agg date by year

        result = []
        for ward_id in range(1, 23):
            result.append({
                "Ward": ward_id,
                "Voter": {
                    "Eligible": stat_for_eligible[ward_id],
                    "Registered": stat_for_registered[ward_id],
                    "Voted": stat_for_van_file[ward_id]
                },
                "Election": stat_for_election[2017][ward_id]
            })

        repo.dropCollection("Ward_data")
        repo.createCollection("Ward_data")

        repo['gengtaox_gengxc_jycai_ruoshi.Ward_data'].insert_many(result)
        repo['gengtaox_gengxc_jycai_ruoshi.Ward_data'].metadata({'complete': True})
        print(repo['gengtaox_gengxc_jycai_ruoshi.Ward_data'].metadata())

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

        this_script = doc.agent('alg:gengtaox_gengxc_jycai_ruoshi#Ward_data',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_historical = doc.entity('xtab:historical', {'prov:label': 'Historical Votes Cast', prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension': 'xlsx'})
        resource_nonReg = doc.entity('xtab:nonReg', {'prov:label': 'Non registered', prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension': 'xlsx'})
        resource_reg = doc.entity('xtab:reg', {'prov:label': 'Registered', prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension': 'xlsx'})
        resource_van = doc.entity('xtab:van', {'prov:label': 'Van File', prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension': 'xlsx'})

        get_historical = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_nonReg = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_reg = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_van = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_historical, this_script)
        doc.wasAssociatedWith(get_nonReg, this_script)
        doc.wasAssociatedWith(get_reg, this_script)
        doc.wasAssociatedWith(get_van, this_script)

        doc.usage(get_historical, resource_historical, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(get_nonReg, resource_nonReg, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(get_reg, resource_reg, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(get_van, resource_van, startTime, None, {prov.model.PROV_TYPE: 'ont:DataSet'})

        wardData = doc.entity('dat:gengtaox_gengxc_jycai_ruoshi#Ward_data',
                           {prov.model.PROV_LABEL: 'The voter and election statistic data of 22 wards in boston.',
                            prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(wardData, this_script)

        doc.wasGeneratedBy(wardData, get_historical, endTime)
        doc.wasGeneratedBy(wardData, get_nonReg, endTime)
        doc.wasGeneratedBy(wardData, get_reg, endTime)
        doc.wasGeneratedBy(wardData, get_van, endTime)

        doc.wasDerivedFrom(wardData, resource_historical, get_historical, get_historical, get_historical)
        doc.wasDerivedFrom(wardData, resource_nonReg, get_nonReg, get_nonReg, get_nonReg)
        doc.wasDerivedFrom(wardData, resource_reg, get_reg, get_reg, get_reg)
        doc.wasDerivedFrom(wardData, resource_van, get_van, get_van, get_van)

        return doc


## eof





















