import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
import pymongo
from bson.objectid import ObjectId
import mapdata as md
import csv

class example(dml.Algorithm):
    contributor = 'ido_jconstan_jeansolo_suitcase'
    reads = []
    writes = ['ido_jconstan_jeansolo_suitcase.registered_students',
              'ido_jconstan_jeansolo_suitcase.property_data',
              'ido_jconstan_jeansolo_suitcase.gas_emissions',
              'ido_jconstan_jeansolo_suitcase.zones',
              'ido_jconstan_jeansolo_suitcase.traffic_count',
              'ido_jconstan_jeansolo_suitcase.asap_student_address',
              'ido_jconstan_jeansolo_suitcase.student_address'
              ]

    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ido_jconstan_jeansolo_suitcase', 'ido_jconstan_jeansolo_suitcase')
        
        #DATA SET 1 [Bu Transportation Study]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/bu_transportation_study.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("registered_students")
        repo.createCollection("registered_students")
        repo['ido_jconstan_jeansolo_suitcase.registered_students'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.registered_students'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.registered_students'].metadata())

        #DATA SET 2 [Spark Property Data]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/property_data.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r1 = json.loads(response)
        s1 = json.dumps(r1, sort_keys=True, indent=2)
        repo.dropCollection("property_data")
        repo.createCollection("property_data")
        repo['ido_jconstan_jeansolo_suitcase.property_data'].insert_many(r1)
        repo['ido_jconstan_jeansolo_suitcase.property_data'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.property_data'].metadata())
		
        #DATA SET 3[Greenhouse Emissions]
        url = 'https://drive.google.com/uc?export=download&id=1OaOvImEZLgxcmg1FmcqP4gSsABOKQu7P'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r2 = json.loads(response)
        s2 = json.dumps(r2, sort_keys=True, indent=2)
        repo.dropCollection("greenhouse_emissions")
        repo.createCollection("greenhouse_emissions") 
        repo['ido_jconstan_jeansolo_suitcase.greenhouse_emissions'].insert(r)
        repo['ido_jconstan_jeansolo_suitcase.greenhouse_emissions'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.greenhouse_emissions'].metadata())   
        
        #DATA SET 4[Boston work zones]
        url = 'https://drive.google.com/uc?export=download&id=1LhG0cxZgHCU2fqDNaGLdQeBZo9z7gJTj'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r3 = json.loads(response)
        s3 = json.dumps(r3, sort_keys=True, indent=2)
        repo.dropCollection("zones")
        repo.createCollection("zones")
        repo['ido_jconstan_jeansolo_suitcase.zones'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.zones'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.zones'].metadata())
        
        #DATA SET 5 [Traffic Count Locations]
        url = 'https://opendata.arcgis.com/datasets/53cd17c661da464c807dfa6ae0563470_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r4 = json.loads(response)
        s4 = json.dumps(r4, sort_keys=True, indent=2)
        repo.dropCollection("traffic_count")
        repo.createCollection("traffic_count")
        repo['ido_jconstan_jeansolo_suitcase.traffic_count'].insert(r)
        repo['ido_jconstan_jeansolo_suitcase.traffic_count'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.traffic_count'].metadata())
        
        #DATA SET 6 [ASAP Student Address]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/ASAP_Student_Addresses.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("asap_student_address")
        repo.createCollection("asap_student_address")
        repo['ido_jconstan_jeansolo_suitcase.asap_student_address'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.asap_student_address'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.asap_student_address'].metadata())

        #DATA SET 7 [Student Address]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/Enrolled_Student_Addresses_Assigned_Schools.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("student_address")
        repo.createCollection("student_address")
        repo['ido_jconstan_jeansolo_suitcase.student_address'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.student_address'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.student_address'].metadata())

        ################################################################################################
        # Data manipulation 
        ################################################################################################
        
        # DATA SET 1 [Bu Transportation Study]
        # Students who take the bus in Natick
        # r1 = {'Last Name', 'School', 'Grade', 'Address', 'Bus Number', 'Pickup Stop', 'Pay for Bus'}
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/bu_transportation_study.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r1 = json.loads(response)
        r1Addy = 'Address'
        r1BusStop = 'Pickup Stop'
        r1School = 'School'
        r1 = addressNormalizer(r1Addy, r1)

        # DATA SET 2 [Spark Property Data]
        # Property values of ALL the homes in Natick
        # r2 = {'Parent ID', 'Address Number', 'Address Street', 'CLS Code', 'Assessed Land', 'Assessed BLDG', 'Assessed Total', 
        #       'Owner 1', 'Owner 2', 'Address 1', 'Address 2', 'City', 'State', 'Zip1', 'Zip2'}
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/property_data.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r2 = json.loads(response)
        r2Addy = 'Address 1'
        r2TotalValue = 'Assessed Total'
        r2City = 'City'
        r2State = 'State'
        r2 = addressNormalizer(r2Addy, r2)

        # DATA SET 6 [ASAP Student Address]
        # Students who do not take the bus
        # r6 = {'Site Name', 'Street Address', 'Apt. No', 'City', 'State', 'Zip'}
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/ASAP_Student_Addresses.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r6 = json.loads(response)
        r6Addy = 'Street Address'
        r6 = addressNormalizer(r6Addy, r6)

        # DATA SET 7 [Student Address]
        # ALL students and the schools that they go to
        # r7 = {'Street Number + Address 1 + Address 2 + Apt', 'School Name'}
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/Enrolled_Student_Addresses_Assigned_Schools.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r7 = json.loads(response)
        r7Addy = 'Street Number + Address 1 + Address 2 + Apt'
        r7SchoolName = 'School Name'
        r7 = addressNormalizer(r7Addy, r7)

        # ('Address', 'NumChildren') ; number of children at each house 
        t1 = project(r7, lambda t: (t[r7Addy], 1) ) # list of addresses
        t2 = aggregate(t1, sum) # final

        # ('Address', 'School Name', 'Assessed Total')
        t3 = project(r2, lambda t: (t[r2Addy], t[r2TotalValue]) ) # ('Address', 'Assessed Total')
        t4 = project(r7, lambda t: (t[r7Addy], t[r7SchoolName]) ) # ('Address', 'School Name')
        t5 = project(select(product(t3, t4), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[1][1], t[0][1]) ) 
        
        # ('Address') ; students who do not take the bus
        t6 = project(r6, lambda t: (t[r6Addy]) )

        # ('Address', 'School Name', 'Assessed Total', 'Y') ; students who take the bus
        t7 = project(t5, lambda t: (t[0], t[1], t[2], 'Y') )
        t8 = select(t7, lambda t: (t[0] not in t6) )

        # ('Address', 'School Name', 'Assessed Total', 'N') ; students who do not take the bus
        t9 = project(t5, lambda t: (t[0], t[1], t[2], 'N') )
        t10 = select(t9, lambda t: (t[0] in t6) )

        # ('Address', 'School Name', 'Assessed Total', 'Y/N do they take the bus')
        t11 = union(t8, t10)


        # ('Address', 'City', 'State') of all addresses
        t12 = project(r2, lambda t: (t[r2Addy], t[r2City], t[r2State]))

        # ('Address', 'City', 'State', 'School Name', 'Assessed Total', 'Y/N do they take the bus')
        t13 = product(t11,t12)
        t14 = select(t13, lambda t: t[0][0] == t[1][0])
        
        #t13school = project(t14, lambda t: (t[0][0], t[1][1], t[1][2], t[0][3]))
        #print(t13school[0])
        t13 = project(t14, lambda t: (t[0][0], t[1][1], t[1][2], t[0][1]))


        t15 = []
        for i in range(0, len(t13)-1):
            #t15[i][0] = t13[i][0] + ", " + t13[i+1][1] + ", " + t13[i+1][2]
            t15.append(((t13[i][0] + ", " + t13[i+1][1] + ", " + t13[i+1][2]), (t13[i][3])))

        # ('Bus Stop'), append city state to bus stops
        t16 = project(r1, lambda t: (t[r1School], t[r1BusStop] + ', Natick, MA'))
       
        
        #separate student addresses by school attended - these will be the points in k-means
        POINTS_OG = []
        tNHS = project(select(t15, lambda t: t[1] == 'Natick High School'), lambda t: t[0])
        tMES = project(select(t15, lambda t: t[1] == 'Memorial Elementary School'), lambda t: t[0])
        tJFKMS = project(select(t15, lambda t: t[1] == 'J. F. Kennedy Middle School'), lambda t: t[0])
        tWMS = project(select(t15, lambda t: t[1] == 'Wilson Middle School'), lambda t: t[0])
        tBES = project(select(t15, lambda t: t[1] == 'Brown Elementary School'), lambda t: t[0])
        tBHES = project(select(t15, lambda t: t[1] == 'Bennett-Hemenway Elementary School'), lambda t: t[0])
        
        POINTS_OG.append(tNHS)
        POINTS_OG.append(tMES)
        POINTS_OG.append(tJFKMS)
        POINTS_OG.append(tWMS)
        POINTS_OG.append(tBES)
        POINTS_OG.append(tBHES)
        
        '''

            # Convert to lang/long for k-means        
            POINTS_NEW = POINTS_OG     
              
            for i in range(len(POINTS_OG)):
                lenPoints = len(POINTS_OG[i])
                print("lenPoints: ", lenPoints)
                for j in range(lenPoints):
                    print("point ", i, " ", j)
                    f = json.loads(json.dumps(md.toLatLong(POINTS_OG[i][j])))
                    #indexed to 0, but there is only a 0th element. it shouldn't be an array but go ogle made a decision
                    lat = f[0]['geometry']['location']['lat']
                    lng = f[0]['geometry']['location']['lng']
                    POINTS_NEW[i][j] = tuple([lat,lng])
        '''
                   
        temp = []
        tswitch = False
        with open('HomesLatLng.csv', mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            # final format: POINTS_NEW[i][j] = tuple([lat,lng])
            for row in csv_reader:
                if row and tswitch:
                    lat = row[0]
                    lng = row[1]
                    temp.append(tuple([float(lat), float(lng)]))
                elif not tswitch:
                    tswitch = True
        tswitch = False

        POINTS_NEW = []  
        count = 0
        for i in range(len(POINTS_OG)):   
            POINTS_NEW.append([])         
            lenStops = len(POINTS_OG[i])-5
            for j in range(lenStops):
                x, y = temp[count]
                if (type(x) == float):
                    POINTS_NEW[i].append(temp[count])
                    count += 1     

                 
        
        #the stops are the means for k-means - converted to sets and back to remove duplicates
        STOPS_OG = []
        tNHSStops = list(set(project(select(t16, lambda t: t[0] == 'NHS'), lambda t: t[1])))
        tMESStops = list(set(project(select(t16, lambda t: t[0] == 'MM'), lambda t: t[1])))
        tJFKMSStops = list(set(project(select(t16, lambda t: t[0] == 'KN'), lambda t: t[1])))
        tWMSStops = list(set(project(select(t16, lambda t: t[0] == 'WL'), lambda t: t[1])))
        tBESStops = list(set(project(select(t16, lambda t: t[0] == 'BR'), lambda t: t[1])))
        tBHESStops = list(set(project(select(t16, lambda t: t[0] == 'BH'), lambda t: t[1])))
        
        STOPS_OG.append(tNHSStops)
        STOPS_OG.append(tMESStops)
        STOPS_OG.append(tJFKMSStops)
        STOPS_OG.append(tWMSStops)
        STOPS_OG.append(tBESStops)
        STOPS_OG.append(tBHESStops)
        
        '''
        temp = []
        ogHolder = ""
        with open('StopsLatLng.csv', mode='w') as csv_file:
            fieldnames = ['lat', 'lng', 'og']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            writer.writeheader()

            STOPS_NEW = STOPS_OG      
            for i in range(len(STOPS_OG)):
                lenStops = len(STOPS_OG[i])
                print("lenStops: ", lenStops)
                for j in range(lenStops):
                    
                    ogHolder = STOPS_OG[i][j]
                    f = json.loads(json.dumps(md.toLatLong(STOPS_OG[i][j])))
                    #indexed to 0, but there is only a 0th element. it shouldn't be an array but go ogle made a decision
                    if f:
                        lat = f[0]['geometry']['location']['lat']
                        lng = f[0]['geometry']['location']['lng']
                        STOPS_NEW[i][j] = tuple([lat,lng])
                        print("stop ", i, " ", j, ": ", STOPS_NEW[i][j])
                    else :
                        f = json.loads(json.dumps(md.toLatLong(STOPS_OG[i][j])))
                        lat = f[0]['geometry']['location']['lat']
                        lng = f[0]['geometry']['location']['lng']
                        STOPS_NEW[i][j] = tuple([lat,lng])
                        print("Error! ", "stop ", i, " ", j, ": ", STOPS_NEW[i][j])
                    writer.writerow({'lat': lat, 'lng': lng, 'og': og})
        '''
        #read the lat/lng from the csv file
        temp = []
        tswitch = False
        with open('StopsLatLng.csv', mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            # final format: STOPS_NEW[i][j] = tuple([lat,lng])
            for row in csv_reader:
                if row and tswitch:
                    lat = row[0]
                    lng = row[1]
                    temp.append(tuple([float(lat), float(lng)]))
                elif not tswitch:
                    tswitch = True

        #make a new structure, & eliminate elements that are not floats (where the geocode api failed to convert)
        STOPS_NEW = []
        count = 0
        for i in range(len(STOPS_OG)):   
            STOPS_NEW.append([])         
            lenStops = len(STOPS_OG[i])-5

            for j in range(lenStops):
                x,y = temp[count]
                if (type(x) == float):
                    STOPS_NEW[i].append(temp[count])
                    count += 1

        #print("\nSTOPS_NEW: ", STOPS_NEW, "\n")
        #implementation of k-means, with md.time as the distance function
        #todo: set a departure time in md.time
        #print('\n\nSTOPS NEW: ',STOPS_NEW ,'\n\n')
        #testLen = 5
        #done for every school separately
        #for x in range(len(STOPS_OG)):
        #STOPS_NEW = 
        #POINTS_NEW = 
        
        print("Before loop")
        for x in range(len(STOPS_NEW)):
            print("STOPS_NEW[",x,"]")
            strFileName = 'k_means_school_' + str(x) + '.csv'
            MEANS = []
            POINTSC = []
            OLD = []
            
            
            for i in range(len(STOPS_NEW[x])):
                MEANS.append(STOPS_NEW[x][i])
            for i in range(len(POINTS_NEW[x])):
                POINTSC.append(POINTS_NEW[x][i])

            
            with open(strFileName, mode='w') as csv_file:
                fieldnames = ['new_stop']
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()

                while sorted(OLD) != sorted(MEANS):
                    print("iteration")
                    
                    OLD = MEANS
                    
                    MPD = [(m, p, dist(m,p)) for (m, p) in product(MEANS, POINTSC)]
                    PDs = [(p, d) for (m, p, d) in MPD]
                    
                    PD = aggregate(PDs, min)
                    MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
                    
                    MT = aggregate(MP, plus)
                    
                    M1 = [(m, 1) for (m, _) in MP]
                    MC = aggregate(M1, sum)
                    
                    MEANS = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]

                # write MEANS to file
                for i in MEANS:
                    writer.writerow({'new_stop': i})

                print("\n\nOLD = MEANS WOOHOO\n\n")
        
        
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
    

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ido_jconstan_jeansolo_suitcase', 'ido_jconstan_jeansolo_suitcase')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('dbg', 'https://data.boston.gov/dataset/greenhouse-gas-emissions/resource/')
        doc.add_namespace('dbg2', 'https://data.boston.gov/dataset/public-works-active-work-zones/resource/')
        doc.add_namespace('oda', 'https://opendata.arcgis.com/datasets/')
        # CHANGES ONLY MADE BELOW THIS COMMENT

        this_script = doc.agent('alg:ido_jconstan_jeansolo_suitcase#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_registeredStudents = doc.entity('dat:registered_students', {'prov:label':'BU Transportation Study', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_propertyData = doc.entity('dat:property_data', {'prov:label':'Property Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_gasEmissions = doc.entity('dbg:bd8dd4bb-867e-4ca2-b6c7-6c3bd9e6c290', {'prov:label':'Gas Emissions', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_workZones = doc.entity('dbg2:36fcf981-e414-4891-93ea-f5905cec46fc', {'prov:label':'Work Zones', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_trafficCount = doc.entity('oda:53cd17c661da464c807dfa6ae0563470_0', {'prov:label':'Traffic Count', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})


        resource_asapStudentAddress = doc.entity('dat:asap_student_address', {'prov:label':'ASAP Student Address', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_studentAddress = doc.entity('dat:student_address', {'prov:label':'Student Address', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})




        get_registered_students = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_property_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_gas_emissions = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_work_zones = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_traffic_count = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_asap_student_address = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_student_address = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_registered_students, this_script)
        doc.wasAssociatedWith(get_property_data, this_script)
        doc.wasAssociatedWith(get_gas_emissions, this_script)
        doc.wasAssociatedWith(get_work_zones, this_script)
        doc.wasAssociatedWith(get_traffic_count, this_script)

        doc.wasAssociatedWith(get_asap_student_address, this_script)
        doc.wasAssociatedWith(get_student_address, this_script)

        doc.usage(get_registered_students, resource_registeredStudents, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )


        doc.usage(get_property_data, resource_propertyData, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )
        doc.usage(get_gas_emissions, resource_gasEmissions, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )
        doc.usage(get_work_zones, resource_workZones, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )
        doc.usage(get_traffic_count, resource_trafficCount, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        doc.usage(get_asap_student_address, resource_asapStudentAddress, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        doc.usage(get_student_address, resource_studentAddress, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )



        registered_students = doc.entity('dat:ido_jconstan_jeansolo_suitcase#registered_students', {prov.model.PROV_LABEL:'BU Transportation Study', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(registered_students, this_script)
        doc.wasGeneratedBy(registered_students, get_registered_students, endTime)
        doc.wasDerivedFrom(registered_students, resource_registeredStudents, get_registered_students, get_registered_students, get_registered_students)

        property_data = doc.entity('dat:ido_jconstan_jeansolo_suitcase#property_data', {prov.model.PROV_LABEL:'Property Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(property_data, this_script)
        doc.wasGeneratedBy(property_data, get_property_data, endTime)
        doc.wasDerivedFrom(property_data, resource_propertyData, get_property_data, get_property_data, get_property_data)

        gas_emissions = doc.entity('dat:ido_jconstan_jeansolo_suitcase#gas_emissions', {prov.model.PROV_LABEL:'Greenhouse Gas Emissions', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(gas_emissions, this_script)
        doc.wasGeneratedBy(gas_emissions, get_gas_emissions, endTime)
        doc.wasDerivedFrom(gas_emissions, resource_gasEmissions, get_gas_emissions, get_gas_emissions, get_gas_emissions)

        work_zones = doc.entity('dat:ido_jconstan_jeansolo_suitcase#work_zones', {prov.model.PROV_LABEL:'Active Work Zones', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(work_zones, this_script)
        doc.wasGeneratedBy(work_zones, get_work_zones, endTime)
        doc.wasDerivedFrom(work_zones, resource_workZones, get_work_zones, get_work_zones, get_work_zones)

        traffic_count = doc.entity('dat:ido_jconstan_jeansolo_suitcase#traffic_count', {prov.model.PROV_LABEL:'Traffic Count', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(traffic_count, this_script)
        doc.wasGeneratedBy(traffic_count, get_traffic_count, endTime)
        doc.wasDerivedFrom(traffic_count, resource_trafficCount, get_traffic_count, get_traffic_count, get_traffic_count)



        asap_student_address = doc.entity('dat:ido_jconstan_jeansolo_suitcase#asap_student_address', {prov.model.PROV_LABEL:'ASAP Student Address', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(asap_student_address, this_script)
        doc.wasGeneratedBy(asap_student_address, get_asap_student_address, endTime)
        doc.wasDerivedFrom(asap_student_address, resource_asapStudentAddress, get_asap_student_address, get_asap_student_address, get_asap_student_address)

        student_address = doc.entity('dat:ido_jconstan_jeansolo_suitcase#student_address', {prov.model.PROV_LABEL:'Student Address', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(student_address, this_script)
        doc.wasGeneratedBy(student_address, get_student_address, endTime)
        doc.wasDerivedFrom(student_address, resource_studentAddress, get_student_address, get_student_address, get_student_address)

        repo.logout()
                  
        return doc

def addressNormalizer(key, rx):
    for i in rx:
        if(type(i[key]) == str):
            i[key] = i[key].upper()

            if "STREET" in i[key]:
                i[key] = i[key].replace("STREET", "ST")
            if "PLACE" in i[key]:
                i[key] = i[key].replace("PLACE", "PL")
            if "TERRACE" in i[key]:
                i[key] = i[key].replace("TERRACE", "TER")
            if "AVENUE" in i[key]:
                i[key] = i[key].replace("AVENUE", "AVE")
            if "CIR" in i[key] and "CIRCLE" not in i[key]:
                i[key] = i[key].replace("CIR", "CIRCLE")
            if "COURT" in i[key]:
                i[key] = i[key].replace("COURT", "CT")
            if "LANE" in i[key]:
                i[key] = i[key].replace("LANE", "LN")
            if "ROAD" in i[key]:
                i[key] = i[key].replace("ROAD", "RD")
            if "PARK" in i[key]:
                i[key] = i[key].replace("PARK", "PK")

    return rx

def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]
    
def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)
    
def scale(p,c):
    (x,y) = p
    return (x/c, y/c)
    
def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def isClose(MEANS, OLD):
    if len(MEANS)!=len(OLD):
        return False
    for x in range(len(MEANS)):
        a = MEANS[x]
        b = OLD[x]
        res = math.isclose(a[0],b[0],abs_tol=.0000001) #check lat/long to see if this is a good tolerance
        res2 = math.isclose(a[1],b[1],abs_tol=.0000001)
        if not res or not res2:
            return False
    return True
            
'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''