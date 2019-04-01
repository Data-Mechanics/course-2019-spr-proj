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


class filter_Streetbook_version_1(dml.Algorithm):
    contributor = 'mmao95_Dongyihe_weijiang_zhukk'
    reads = [contributor + '.cau_landmark_merge', contributor +
             '.public_libraries', contributor + '.street_book']
    writes = [contributor + '.filter_Streetbook_version_1']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        contributor = 'mmao95_Dongyihe_weijiang_zhukk'
        reads = [contributor + '.cau_landmark_merge', contributor +
                 '.public_libraries', contributor + '.street_book']
        writes = [contributor + '.filter_Streetbook_version_1']

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
        Public_library_names = ["Boston Public Library", "Honan-Allston Branch of the Boston Public Library", "Brighton Branch of the Boston Public Library",
                                "Chinatown Branch Boston Public Library", "South End Branch of the Boston Public Library", "West End Branch of the Boston Public Library",
                                "North End Branch of the Boston Public Library", "Connolly Branch of the Boston Public Library", "Faneuil Branch of the Boston Public Library",
                                "Mugar Memorial Library", "Parker Hill Branch of the Boston Public Library", "Dudley Branch of the Boston Public Library", "East Boston Branch of the Boston Public Library",
                                "South Boston Branch Boston Public Library", "Codman Square Branch of the Boston Public Library", "Frederick S. Pardee Management Library",
                                "Jamaica Plain Branch of the Boston Public Library", "Egleston Square Branch of the Boston Public Library", "Charlestown Branch of the Boston Public Library",
                                "Grove Hall Branch of the Boston Public Library", "West Roxbury Branch of the Boston Public Library", "Roslindale Branch of the Boston Public Library",
                                "Uphams Corner Branch of the Boston Public Library", "Boston College Theology and Ministry Library", "Music Library Boston University", "Stone Science Library",
                                "Adams Street Branch of the Boston Public Library", "Fields Corner Branch Boston Public Library", "Hyde Park Branch of the Boston Public Library",
                                "Albert Alphin Music Library", "Mattapan Branch of the Boston Public Library", "Lower Mills Branch of the Boston Public Library", "State Library of Massachusetts",
                                "Boston University Visual Arts Resource Library", "Insurance Library Association-Boston", "Fineman & Pappas Law Libraries", "African Studies Library",
                                "Science and Engineering Library", "Pickering Educational Resources Library"]
        Old_names = [
            "Adjutant-Generals Library",
            "Almshouse Library",
            "American Academy of Arts and Sciences Library",
            "American Baptist Union Library",
            "American Board of Commissioners for Foreign Missions Library",
            "American Institute of Instruction Library",
            "American Peace Society Library",
            "American Statistical Association Library",
            "American Unitarian Association",
            "Mary Ashley, no.124 Charles St.",
            "Asylum and Farm School Library",
            "Backups Circulating Library",
            "Luke Bakers circulating library, no.69 Court St.",
            "Berkeley Circulating Library",
            "Bigelow School Library",
            "Bixbys Circulating Library;2 L.W. Bixby, Washington St.6",
            "William Pinson Blake and Lemuel Blake, circulating library at the Boston Book-Store, no.1, Cornhill7",
            "Board of Trade Library12",
            "Boston and Albany Railroad Library (est.1868)8",
            "Boston Art Club Library2",
            "Boston Athenaeum",
            "Boston Circulating Library, no.3 School St.; E. Penniman Jr.;9 no.5 Cornhill-Square10",
            "Boston City Hospital Library2",
            "Boston College Library2",
            "Boston Library Society2",
            "Boston Lunatic Hospital Library2",
            "Boston Medical Library (1805-1826)11",
            "Boston Medical Library (est. 1875)",
            "Boston Public Library",
            "Boston Society for Medical Improvement1",
            "Boston Society of Natural History Library12",
            "Boston Society of the New Jerusalem Church Library2",
            "Boston Theological Library211",
            "Boston Turnverein Library2",
            "Boston University",
            "Boston University Library2",
            "Boston University School of Medicine Library2",
            "Boston University Theological Library2",
            "Boston Young Mens Christian Union Library,2 no.20 Boylston6",
            "Bowditch Library1",
            "Bowdoin Literary Association1",
            "Boylston Library12",
            "Brattle Square Church Library2",
            "Broadway Circulating Library2",
            "Bromfield Street Church Library2",
            "Eliza Brown, circulating library13",
            "Bulfinch Place Chapel Library2",
            "Burnham & Bros., no.60 Cornhill;14 Thomas Burnham, Perry Burnham12",
            "Kezia Butler, no.82 Newbury Street1215",
            "Campbells Circulating Library2",
            "Carney Hospital Library2",
            "Carters Circulating Library212",
            "Callenders Library, School St.; also known as the Shakespeare Library; Charles Callender, H.G. Callender31216",
            "Charlestown High School Library2",
            "Christ Church Library2",
            "Christian Unity Library2",
            "Church Home for Orphans Library2",
            "Church of the Advent Library2",
            "City Point Circulating Library2",
            "Clarendon Library, Clarendon St.6",
            "W.B. Clarkes circulating library12",
            "Columbian Circulating Library, no.43 Cornhill",
            "Columbian Social Library (est.1813), Boylston Hall1718",
            "Comers Commercial College1",
            "Congregational Library & Archives,2 corner of Beacon and Somerset16",
            "Consumptives Home Library2",
            "Deaf Mute Library Association2",
            "Democratic reading room, corner Congress St. and Congress Sq.3",
            "Dorchester Athenaeum Library2",
            "Dramatic Fund Association1",
            "J.H. Duclos & Bro., no.57 Warren St.6",
            "East Boston Library Association1",
            "Ministerial Library of Eliot Church2",
            "Caroline Fanning19",
            "Farrers Circulating Library2",
            "First Christian Church Library2",
            "First Universalist Church Library2",
            "Frederick Fletcher, no.55 Meridian, East Boston20",
            "Franklin Circulating Library, no.69 Court St.21",
            "Franklin Typographical Society Library12",
            "Gate of Heaven Church Library2",
            "Library of the General Court1",
            "General Theological Library (est.1860);212 no.12 West St.6",
            "Gills Circulating Library2",
            "Good Samaritan Church Library2",
            "Grand Lodge of Masons Library2",
            "Grant & Brown, no.873 Washington St.6",
            "Guild Library of Church of the Advent2",
            "Hallidays Circulating Library, West St.25",
            "Hancock Library, 42 Hancock; A. Boyden22",
            "Handel and Haydn Library1",
            "Harvard Chapel Library2",
            "Harvard Musical Library2",
            "Medical College of Harvard University Library2",
            "C.W. Holbrooks circulating library; no.88 Dover6",
            "Holy Trinity Church Library2",
            "Home for Aged Women Library2",
            "House of Correction Library2",
            "House of Industry Library2",
            "House of Reformation Library2",
            "Jamaica Plain Circulating Library2",
            "Joy Street Baptist Church Library2",
            "Keatings Circulating Library;2 no.1027 Washington St.6",
            "Kings Chapel Library1",
            "Ladies Circulating Library, Washington St.; N. Nutting, proprietor19",
            "Lawrence Association Library2",
            "R.L. Learneds circulating library, Tremont St.5",
            "Lincoln School Library2",
            "Lindsays Circulating Library;2 George W. Lindsey, Washington St.20",
            "Liscombs Circulating Library2",
            "Lorings Circulating Library;2 Lorings Select Library, Washington St.;20 A.K. Loring12",
            "A.F. Lows circulating library, Meridian St.5",
            "Lowes Circulating Library2",
            "Marine Board of Underwriters Library2",
            "Mariners Exchange reading room, no.1 Lewis20",
            "Mariners House, North Square23",
            "Thomas Marshs circulating library, Beach St.5",
            "Massachusetts College of Pharmacy1",
            "Massachusetts Historical Society",
            "Massachusetts Horticultural Society Library2",
            "Massachusetts Hospital1",
            "Massachusetts New Church Union Library2",
            "Massachusetts School for Idiotic and Feeble-minded Youth1",
            "Massachusetts Society for Promotion of Agriculture1",
            "Massachusetts State Library",
            "Massachusetts State Prison Library2",
            "Massachusetts Teachers Association1",
            "Massachusetts Total Abstinence Society Library2",
            "Mayhew and Bakers Juvenile Circulating Library, no.208 Washington St.12",
            "McGraths Circulating Library2",
            "Mechanic Apprentices Library Association2",
            "J.O. Mendums circulating library, Tremont St.5",
            "Mercantile Library Association Library2",
            "Merchants Exchange reading room, Merchants Exchange building, State St.;24 basement, Old State House",
            "Merrills Circulating Library;2 C.H. Merrill, no.1575 Washington St.6",
            "Methodist Episcopal Church Library2",
            "Catherine Moores circulating library, no.436 Washington St.314",
            "Mount Vernon Church Library2",
            "Mount Vernon School for Young Ladies1",
            "Munroe & Francis, juvenile library, no.4 Cornhill25",
            "Musical Society Library12",
            "New England Conservatory of Music Library2",
            "New England Female Medical College1",
            "New England Historic Genealogical Society2",
            "New England Hospital Library2",
            "New England Methodist Historical Society1",
            "Norcross School Library2",
            "North End Circulating Library, no.123 Hanover St.; Thomas Hiller Jr.326",
            "Notre Dame Academy Library2",
            "Odd Fellows Library2",
            "Old Colony Chapel Library2",
            "Osgoods Circulating Library2",
            "Paines Circulating Library2",
            "Samuel H. Parkers circulating library",
            "H.B. Payne & Co.s circulating library5",
            "Elizabeth Peabodys foreign circulating library, no.13 West St.3",
            "William Pelhams circulating library, no.59 Cornhill",
            "Penitent Female Refuge Society Library2",
            "F.W. Perkins circulating library5",
            "Library of Perkins Institution for the Blind12",
            "Pioneer Circulating Library2",
            "Prince Library27",
            "Quinns Circulating Library2",
            "Lydia Reed19",
            "Republican Institution1",
            "Republican Reading Room, Bromfield St.20",
            "E.R. Rich & Son; no.477 West Broadway6",
            "Roxbury Athenaeum Library2",
            "Roxbury High School Library2",
            "Sages Circulating Library;2 William Sage, no.371 Tremont6",
            "Sailors Home Library2",
            "School of Technology Library2",
            "Second Methodist Church Library2",
            "W.F. & M.H. Shattuck, no.106 Main6",
            "Shawmut Avenue Baptist Church Library2",
            "Shawmut Mission Library2",
            "Social Law Library2",
            "Society to Encourage Studies at Home",
            "South End Circulating Library2",
            "Mary Spragues circulating library, no.9 Milk St.1228",
            "St. Francis de Sales Church Library2",
            "St. Joseph Circulating Library2",
            "St. Marys Young Mens Sodality Library2",
            "St. Stephens Church Library2",
            "State Agricultural Library2",
            "Stoughton Street Church Library2",
            "Suffolk Circulating Library; corner of Court and Brattle St.; N.S. Simpkins, J. Simpkins29",
            "Sumner library, no.6 Winthrop block, East Boston20",
            "Teuthorns Circulating Library;2 Julius Teuthorn, no.10 Beach6",
            "George Ticknors private library, no.8 Park St.",
            "Toll-Gate Circulating Library,2 no.665 East Broadway56",
            "D.A. Tompkins, no.127 Hanover St.14",
            "Treadwell Library, Massachusetts General Hospital2",
            "Union Circulating Library, no.4 Cornhill, corner of Water St.;30 William Blagrove",
            "Union Mission Church Library2",
            "Unitarian Association Library2",
            "S.R. Urbino, foreign circulating library",
            "Village Church Library2",
            "Vine Street Congregational Church Library2",
            "Walkers Circulating Library2",
            "J.B. Walker, no.1392 Tremont6",
            "Thomas O. Walker, no.68 Cornhill3512",
            "Warren Street Chapel Library2",
            "Washington Circulating Library, no.38 Newbury St.31",
            "Washington Circulating Library, no.11 School St.32",
            "Washingtonian Home Library2",
            "West Boston Library, Cambridge St.12",
            "West Church Library2",
            "West Roxbury Free library, Centre St.6",
            "West Roxbury High School Library2",
            "Whig reading room, no.144 Washington St.3",
            "Winkley & Boyds Central library20",
            "Workingmens Reading Room",
            "Boston Young Mens Christian Association Library (YMCA),2 Tremont Temple120",
            "Young Mens Christian Union1",
            "Young Womens Christian Association (YWCA), no.68 Warrenton6",
            "Young Mens Working Association Library2",
            "Zion Church Library2"]

        Public_library_names = Public_library_names + Old_names

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

        columnName = ['FullName', 'StreetName', 'Zipcode']
        df = pd.DataFrame(columns=columnName, data=Streetbook)
        data = json.loads(df.to_json(orient="records"))

        repo.dropCollection('filter_Streetbook_version_1')
        repo.createCollection('filter_Streetbook_version_1')
        repo[writes[0]].insert_many(data)

        repo[writes[0]].metadata({'complete': True})
        print(repo[writes[0]].metadata())
        [record for record in repo[writes[0]].find()]

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

        contributor = 'mmao95_Dongyihe_weijiang_zhukk'
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        # The scripts are in <folder>#<filename> format.
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        # The event log.
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://www.50states.com/bio/mass.htm')

        this_script = doc.agent('alg:' + contributor + '#filter_Streetbook_version_1', {
                                prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': '311, Service Requests',
                                                prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        get_names = doc.activity(
            'log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_names, this_script)
        doc.usage(get_names, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Computation': 'Data cleaning'
                   }
                  )

        fp = doc.entity('dat:' + contributor + '#filter_Streetbook_version_1', {
                        prov.model.PROV_LABEL: 'filter_Streetbook_version_1', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(fp, this_script)
        doc.wasGeneratedBy(fp, get_names, endTime)
        doc.wasDerivedFrom(fp, resource, get_names, get_names, get_names)

        repo.logout()

        return doc

# eof
