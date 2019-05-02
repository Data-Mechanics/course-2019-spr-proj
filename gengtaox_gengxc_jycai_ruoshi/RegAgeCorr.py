from openpyxl import load_workbook
import scipy.stats
import dml
import prov.model
import datetime
import uuid


class RegAgeCorr(dml.Algorithm):
    contributor = 'gengtaox_gengxc_jycai_ruoshi'
    reads = []
    writes = ['gengtaox_gengxc_jycai_ruoshi.RegAgeCorr', ]

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gengtaox_gengxc_jycai_ruoshi', 'gengtaox_gengxc_jycai_ruoshi')

        wb = load_workbook(filename='gengtaox_gengxc_jycai_ruoshi/registered_voters_xtabs.xlsx')
        sheet = wb['Registered Voters by Precinct']

        age_group_column = {
            "18-24": ("B", "H", "N", "T", "Z", "AF", "AL"),
            "25-34": ("C", "I", "O", "U", "AA", "AG", "AM"),
            "35-49": ("D", "J", "P", "V", "AB", "AH", "AN"),
            "50-64": ("E", "K", "Q", "W", "AC", "AI", "AO"),
            "65+": ("F", "L", "R", "X", "AD", "AJ", "AP")
        }

        if trial:
            endline = 200
        else:
            endline = 436

        ward_statistic = []
        for i in range(181, endline):
            ward_id = int(sheet["A{}".format(i)].value.split()[1][1:])
            if len(ward_statistic) < ward_id:
                # add a new ward list
                stat = {
                    "18-24": 0,
                    "25-34": 0,
                    "35-49": 0,
                    "50-64": 0,
                    "65+": 0
                }

                for group_columns in age_group_column:
                    for column in age_group_column[group_columns]:
                        stat[group_columns] += int(sheet["{}{}".format(column, i)].value)

                ward_statistic.append(stat)
            else:
                for group_columns in age_group_column:
                    for column in age_group_column[group_columns]:
                        ward_statistic[ward_id - 1][group_columns] += int(sheet["{}{}".format(column, i)].value)

        for ward in ward_statistic:
            ward["total"] = sum(ward.values())

        output = []
        for group in age_group_column:
            curr_group_list = [ward[group] for ward in ward_statistic]
            total_list = [ward['total'] for ward in ward_statistic]
            corr = scipy.stats.pearsonr(curr_group_list, total_list)
            output.append({
                'group': group,
                'corr': corr[0],
                'p_val': corr[1]
            })
            print("{}: {}".format(group, corr))

        repo.dropCollection("RegAgeCorr")
        repo.createCollection("RegAgeCorr")

        repo['gengtaox_gengxc_jycai_ruoshi.RegAgeCorr'].insert_many(output)
        repo['gengtaox_gengxc_jycai_ruoshi.RegAgeCorr'].metadata({'complete': True})
        print(repo['gengtaox_gengxc_jycai_ruoshi.RegAgeCorr'].metadata())

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

        this_script = doc.agent('alg:gengtaox_gengxc_jycai_ruoshi#RegAgeCorr',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_excel_reg = doc.entity('xtab:non',
                                      {'prov:label': 'registered ward', prov.model.PROV_TYPE: 'ont:DataSet',
                                       'ont:Extension': 'xlsx'})

        get_excel_reg = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_excel_reg, this_script)

        doc.usage(get_excel_reg, resource_excel_reg, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:DataSet'
                   })

        NonRegAgeCorr = doc.entity('dat:gengtaox_gengxc_jycai_ruoshi#RegAgeCorr',
                           {prov.model.PROV_LABEL: 'The correlation coefficient for different age group for reg voter',
                            prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(NonRegAgeCorr, this_script)

        doc.wasGeneratedBy(NonRegAgeCorr, get_excel_reg, endTime)

        doc.wasDerivedFrom(NonRegAgeCorr, resource_excel_reg, get_excel_reg, get_excel_reg, get_excel_reg)

        return doc


## eof





















