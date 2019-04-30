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
from random import shuffle
from math import sqrt
import gmplot


class example(dml.Algorithm):
    contributor = 'ido_jconstan_jeansolo_suitcase'
    reads = []
    writes = ['ido_jconstan_jeansolo_suitcase.registered_students',
              'ido_jconstan_jeansolo_suitcase.property_data',
              'ido_jconstan_jeansolo_suitcase.gas_emissions',
              'ido_jconstan_jeansolo_suitcase.zones',
              'ido_jconstan_jeansolo_suitcase.traffic_count',
              'ido_jconstan_jeansolo_suitcase.asap_student_address',
              'ido_jconstan_jeansolo_suitcase.student_address',
              'ido_jconstan_jeansolo_suitcase.HomesLatLng',
              'ido_jconstan_jeansolo_suitcase.StopsLatLng'
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

        #DATA SET 8 [HomesLatLng]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/HomesLatLng.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("HomesLatLng")
        repo.createCollection("HomesLatLng")
        repo['ido_jconstan_jeansolo_suitcase.HomesLatLng'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.HomesLatLng'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.HomesLatLng'].metadata())

        #DATA SET 9 [StopsLatLng]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/StopsLatLng.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("StopsLatLng")
        repo.createCollection("StopsLatLng")
        repo['ido_jconstan_jeansolo_suitcase.StopsLatLng'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.StopsLatLng'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.StopsLatLng'].metadata())

        # DATA SET 9 [StopsLatLng]
        # Bus Stops Latitude and Longitude
        # r9 = {'lat', 'long, 'og'}
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/StopsLatLng.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r9 = json.loads(response)

        #DATA SET 10 [k-means_0]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_0.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r10 = json.loads(response)
        s = json.dumps(r10, sort_keys=True, indent=2)
        repo.dropCollection("k_means_0")
        repo.createCollection("k_means_0")
        repo['ido_jconstan_jeansolo_suitcase.k_means_0'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.k_means_0'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.k_means_0'].metadata())

        #DATA SET 11 [k-means_1]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_1.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r11 = json.loads(response)
        s = json.dumps(r11, sort_keys=True, indent=2)
        repo.dropCollection("k_means_1")
        repo.createCollection("k_means_1")
        repo['ido_jconstan_jeansolo_suitcase.k_means_1'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.k_means_1'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.k_means_1'].metadata())

        #DATA SET 12 [k-means_2]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_2.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r12 = json.loads(response)
        s = json.dumps(r12, sort_keys=True, indent=2)
        repo.dropCollection("k_means_2")
        repo.createCollection("k_means_2")
        repo['ido_jconstan_jeansolo_suitcase.k_means_2'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.k_means_2'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.k_means_2'].metadata())

        #DATA SET 13 [k-means_3]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_3.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r13 = json.loads(response)
        s = json.dumps(r13, sort_keys=True, indent=2)
        repo.dropCollection("k_means_3")
        repo.createCollection("k_means_3")
        repo['ido_jconstan_jeansolo_suitcase.k_means_3'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.k_means_3'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.k_means_3'].metadata())

        #DATA SET 14 [k-means_4]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_4.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r14 = json.loads(response)
        s = json.dumps(r14, sort_keys=True, indent=2)
        repo.dropCollection("k_means_4")
        repo.createCollection("k_means_4")
        repo['ido_jconstan_jeansolo_suitcase.k_means_4'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.k_means_4'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.k_means_4'].metadata())

        #DATA SET 15 [k-means_5]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_5.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r15 = json.loads(response)
        s = json.dumps(r15, sort_keys=True, indent=2)
        repo.dropCollection("k_means_5")
        repo.createCollection("k_means_5")
        repo['ido_jconstan_jeansolo_suitcase.k_means_5'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.k_means_5'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.k_means_5'].metadata())

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

        # DATA SET 8 [HomesLatLng]
        # Homes Latitude and Longitude Data
        # r8 = {'lat', 'long, 'og'}
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/HomesLatLng.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r8 = json.loads(response)
        

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
        t8 = select(t7, lambda t: (t[0] not in t6 ) )

        '''
        # make tuples for agreements for household value under 400k and do take the bus
        catU_Y = select(t8, lambda t: (t[2] < 400000))
        #print('CATU_Y', catU_Y)
        proj_catU_Y = project(catU_Y,lambda t:(0,1))
        print('Under_Yes', proj_catU_Y)

        # make tuples for agreements for household value under 400k and do take the bus
        catO_Y = select(t8, lambda t: (t[2] >= 400000))
        #print('CATO_Y', catO_Y)
        proj_catO_Y = project(catO_Y,lambda t:(1,1))
        print('Over_Yes', proj_catO_Y)
        '''

        # cat2 = select(t8, lambda t: (t[2] >= 500000 and t[2] < 600000))
        # #print('CAT2', cat2)
        # proj_cat2 = project(cat2,lambda t:('500k-600k',1))
        # num_cat2 = aggregate(proj_cat2,sum)
        # print('NUM_CAT2',num_cat2)

        # cat3 = select(t8, lambda t: (t[2] >= 600000 and t[2] < 700000))
        # #print('CAT3', cat3)
        # proj_cat3 = project(cat3,lambda t:('600k-700k',1))
        # num_cat3 = aggregate(proj_cat3,sum)
        # print('NUM_CAT3',num_cat3)

        # cat4 = select(t8, lambda t: (t[2] >= 700000 and t[2] < 800000))
        # #print('CAT4', cat4)
        # proj_cat4 = project(cat4,lambda t:('700k-800k',1))
        # num_cat4 = aggregate(proj_cat4,sum)
        # print('NUM_CAT4',num_cat4)

        # cat5 = select(t8, lambda t: (t[2] >= 800000 and t[2] < 900000))
        # #print('CAT5', cat5)
        # proj_cat5 = project(cat5,lambda t:('800k-900k',1))
        # num_cat5 = aggregate(proj_cat5,sum)
        # print('NUM_CAT5',num_cat5)

        # cat6 = select(t8, lambda t: (t[2] >= 900000 and t[2] < 1000000))
        # #print('CAT6', cat6)
        # proj_cat6 = project(cat6,lambda t:('900k-1M',1))
        # num_cat6 = aggregate(proj_cat6,sum)
        # print('NUM_CAT6',num_cat6)

        # cat7 = select(t8, lambda t: (t[2] >= 1000000))
        # #print('CAT7', cat7)
        # proj_cat7 = project(cat7,lambda t:('Over 1M',1))
        # num_cat7 = aggregate(proj_cat7,sum)
        # print('NUM_CAT7',num_cat7)

        # ('Address', 'School Name', 'Assessed Total', 'N') ; students who do not take the bus
        t9 = project(t5, lambda t: (t[0], t[1], t[2], 'N') )
        t10 = select(t9, lambda t: (t[0] in t6) )
        '''

        # make tuples for agreements for household value under 400k and don't take the bus
        catU_N = select(t10, lambda t: (t[2] < 400000))
        #print('CATU_N', catU_N)
        proj_catU_N = project(catU_N,lambda t:(0,0))
        print('Under_No', proj_catU_N)

        # make tuples for agreements for household value over 400k and don't take the bus
        catO_N = select(t10, lambda t: (t[2] >= 400000))
        #print('CATO_N', catO_N)
        proj_catO_N = project(catO_N,lambda t:(1,0))
        print('Over_No', proj_catO_N)

        # combine into one list
        final_proj = proj_catO_N + proj_catO_Y + proj_catU_N + proj_catU_Y

        # calculate proportion agreement
        result = proportionAgreement(final_proj)
        print('RESULT', result)
        '''

        # cat11 = select(t10, lambda t: (t[2] < 500000))
        # #print('CAT11', cat11)
        # proj_cat11 = project(cat11,lambda t:('less than 500k',1))
        # num_cat11 = aggregate(proj_cat11,sum)
        # print('NUM_CAT11',num_cat11)

        # cat12 = select(t10, lambda t: (t[2] >= 500000 and t[2] < 600000))
        # #print('CAT12', cat12)
        # proj_cat12 = project(cat12,lambda t:('500k-600k',1))
        # num_cat12 = aggregate(proj_cat12,sum)
        # print('NUM_CAT12',num_cat12)

        # cat13 = select(t10, lambda t: (t[2] >= 600000 and t[2] < 700000))
        # #print('CAT3', cat3)
        # proj_cat13 = project(cat13,lambda t:('600k-700k',1))
        # num_cat13 = aggregate(proj_cat13,sum)
        # print('NUM_CAT13',num_cat13)

        # cat14 = select(t10, lambda t: (t[2] >= 700000 and t[2] < 800000))
        # #print('CAT4', cat4)
        # proj_cat14 = project(cat14,lambda t:('700k-800k',1))
        # num_cat14 = aggregate(proj_cat14,sum)
        # print('NUM_CAT14',num_cat14)

        # cat15 = select(t10, lambda t: (t[2] >= 800000 and t[2] < 900000))
        # #print('CAT5', cat15)
        # proj_cat15 = project(cat15,lambda t:('800k-900k',1))
        # num_cat15 = aggregate(proj_cat15,sum)
        # print('NUM_CAT15',num_cat15)

        # cat16 = select(t10, lambda t: (t[2] >= 900000 and t[2] < 1000000))
        # #print('CAT16', cat16)
        # proj_cat16 = project(cat16,lambda t:('900k-1M',1))
        # num_cat16 = aggregate(proj_cat6,sum)
        # print('NUM_CAT16',num_cat16)

        # cat17 = select(t10, lambda t: (t[2] >= 1000000))
        # #print('CAT17', cat17)
        # proj_cat17 = project(cat17,lambda t:('Over 1M',1))
        # num_cat17 = aggregate(proj_cat17,sum)
        # print('NUM_CAT17',num_cat17)

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

        # ('lat', 'lng', 'og') of homes r8
        t17 = project(r8, lambda t: (t['lat'], t['long']))
        # ('lat', 'lng', 'og') of stops r9
        t18 = project(r9, lambda t: (t['lat'], t['long']))

        k0 = project(r10, lambda t: (t['new_stop']))
        k1 = project(r11, lambda t: (t['new_stop']))
        k2 = project(r12, lambda t: (t['new_stop']))
        k3 = project(r13, lambda t: (t['new_stop']))
        k4 = project(r14, lambda t: (t['new_stop']))
        k5 = project(r15, lambda t: (t['new_stop']))


        # '(42.55555, 42.5555)'
        temp = 0
        for i in k0:
            list_x = []
            list_y = []
            
            #for j in range(len(i)):
            j=0
            if i[j] == '(':
                j+=1
                list_x.append(i[j])
                j+=1

            while i[j] != ',':
                list_x.append(i[j])
                j+=1

            if i[j] == ',':
                j+=2

            while i[j] != ')':
                list_y.append(i[j])
                j+=1
            str_x = "".join(list_x)
            str_y = "".join(list_y)
            float_x = float(str_x)
            float_y = float(str_y)
            i = (float_x, float_y)

            k0[temp] = i
            temp += 1


        temp = 0
        for i in k1:
            list_x = []
            list_y = []

            #for j in range(len(i)):
            j=0
            if i[j] == '(':
                j+=1
                list_x.append(i[j])
                j+=1

            while i[j] != ',':
                list_x.append(i[j])
                j+=1

            if i[j] == ',':
                j+=2

            while i[j] != ')':
                list_y.append(i[j])
                j+=1
            str_x = "".join(list_x)
            str_y = "".join(list_y)
            float_x = float(str_x)
            float_y = float(str_y)
            i = (float_x, float_y)

            k1[temp] = i
            temp += 1

        temp = 0
        for i in k2:
            list_x = []
            list_y = []
            
            #for j in range(len(i)):
            j=0
            if i[j] == '(':
                j+=1
                list_x.append(i[j])
                j+=1

            while i[j] != ',':
                list_x.append(i[j])
                j+=1

            if i[j] == ',':
                j+=2

            while i[j] != ')':
                list_y.append(i[j])
                j+=1
            str_x = "".join(list_x)
            str_y = "".join(list_y)
            float_x = float(str_x)
            float_y = float(str_y)
            i = (float_x, float_y)

            k2[temp] = i
            temp += 1

        temp = 0
        for i in k3:
            list_x = []
            list_y = []
            
            #for j in range(len(i)):
            j=0
            if i[j] == '(':
                j+=1
                list_x.append(i[j])
                j+=1

            while i[j] != ',':
                list_x.append(i[j])
                j+=1

            if i[j] == ',':
                j+=2

            while i[j] != ')':
                list_y.append(i[j])
                j+=1
            str_x = "".join(list_x)
            str_y = "".join(list_y)
            float_x = float(str_x)
            float_y = float(str_y)
            i = (float_x, float_y)

            k3[temp] = i
            temp += 1

        temp = 0
        for i in k4:
            list_x = []
            list_y = []
            
            #for j in range(len(i)):
            j=0
            if i[j] == '(':
                j+=1
                list_x.append(i[j])
                j+=1

            while i[j] != ',':
                list_x.append(i[j])
                j+=1

            if i[j] == ',':
                j+=2

            while i[j] != ')':
                list_y.append(i[j])
                j+=1
            str_x = "".join(list_x)
            str_y = "".join(list_y)
            float_x = float(str_x)
            float_y = float(str_y)
            i = (float_x, float_y)

            k4[temp] = i
            temp += 1

        temp = 0
        for i in k5:
            list_x = []
            list_y = []
            
            #for j in range(len(i)):
            j=0
            if i[j] == '(':
                j+=1
                list_x.append(i[j])
                j+=1 

            while i[j] != ',':
                list_x.append(i[j])
                j+=1

            if i[j] == ',':
                j+=2

            while i[j] != ')':
                list_y.append(i[j])
                j+=1
            str_x = "".join(list_x)
            str_y = "".join(list_y)
            float_x = float(str_x)
            float_y = float(str_y)
            i = (float_x, float_y)

            k5[temp] = i
            temp += 1
        
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
        
        temp = t17

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
        
        temp = t18
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



        # # read new stops from csv
        # NEW_STOPS = []
        # strName = ''
        # count = 0
        # limit = 230080
        # for i in range(5):
            # strName = 'k_means_school_' + str(i) + '.csv'
            # NEW_STOPS.append([])
            # with open(strName, mode='r') as csv_file:
                # csv_reader = csv.reader(csv_file, delimiter=',')
                # for row in csv_reader:
                    # count += 1
                    # if row: 
                        # stop = row
                        # #print("i: ", i)
                        # NEW_STOPS[i].append(row)
                    
                    # if count >= limit:
                        # break
            

        # # parse points
        # for x in range(len(NEW_STOPS)):
            # #for i in range(len(NEW_STOPS[x])):
            # NEW_STOPS[x] = pointParser(NEW_STOPS[x])
        
        # NEW_STOPS.pop(0)


        # print("Before loop")
        # for x in range(len(STOPS_NEW)):
            # print("STOPS_NEW[",x,"]")
            # strFileName = 'k_means_school_' + str(x) + '.csv'
            # MEANS = []
            # POINTSC = []
            # OLD = []
            
            
            # for i in range(len(STOPS_NEW[x])):
                # MEANS.append(STOPS_NEW[x][i])
            # for i in range(len(POINTS_NEW[x])):
                # POINTSC.append(POINTS_NEW[x][i])

            # M_OLD = MEANS # old bus stops
            # print('OLD MEANS',M_OLD)
            # P = POINTSC # student addresses
            # MPD_OLD = [(m, p, dist(m,p)) for (m, p) in product(M_OLD, P)]
            # PDs_OLD = [(p, dist(m,p)) for (m, p, d) in MPD_OLD]
            # PD_OLD = aggregate(PDs_OLD, min)
            # MP_OLD = [(d) for ((m,p,d), (p2,d2)) in product(MPD_OLD, PD_OLD) if p==p2 and d==d2]
            # count_OLD = 0
            # for d in MP_OLD:
                # count_OLD += d 
            # average_OLD = count_OLD/len(POINTSC)
            # print('old avg', average_OLD)

            # MEANS = []
            # for i in range(len(NEW_STOPS[x])):
                # MEANS.append(NEW_STOPS[x][i])
            # M_NEW = MEANS # new bus stops
            # print('NEW MEANS',M_NEW)
            # MPD_NEW = [(m, p, dist(m,p)) for (m, p) in product(M_NEW, P)]
            # PDs_NEW = [(p, dist(m,p)) for (m, p, d) in MPD_NEW]
            # PD_NEW = aggregate(PDs_NEW, min)
            # MP_NEW = [(d) for ((m,p,d), (p2,d2)) in product(MPD_NEW, PD_NEW) if p==p2 and d==d2]
            # count_NEW = 0
            # for d in MP_NEW:
                # count_NEW += d 
            # average_NEW = count_NEW/len(POINTSC)
            # print('new avg', average_NEW)

            
            # with open(strFileName, mode='w') as csv_file:
                # fieldnames = ['new_stop']
                # writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                # writer.writeheader()

                # while sorted(OLD) != sorted(MEANS):
                    # print("iteration")
                    
                    # OLD = MEANS
                    
                    # MPD = [(m, p, dist(m,p)) for (m, p) in product(MEANS, POINTSC)]
                    # PDs = [(p, d) for (m, p, d) in MPD]
                    
                    # PD = aggregate(PDs, min)
                    # MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
                    
                    # MT = aggregate(MP, plus)
                    
                    # M1 = [(m, 1) for (m, _) in MP]
                    # MC = aggregate(M1, sum)
                    
                    # MEANS = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]

                # # write MEANS to file
                # for i in MEANS:
                    # writer.writerow({'new_stop': i})

               #print("\n\nOLD = MEANS WOOHOO\n\n")
        
        '''
                  
                  
        #parse r9 and plot the points on a map
        latitude_list = [] 
        longitude_list = [] 
        
        for (x,y) in t18:
            latitude_list.append(x)
            longitude_list.append(y)
        
          
        gmapBefore = gmplot.GoogleMapPlotter(42.283772, 
                                        -71.347290, 13) 
          
        # scatter points on the google map 
        gmapBefore.scatter( latitude_list, longitude_list, '# 0000FF', 
                                      size = 40, marker = False ) 
          
        # Draw a line in 
        # between given coordinates 
          
        gmapBefore.draw( "D:\\_Documents\\cs504\\Project2\\course-2019-spr-proj\\mapBefore.html" ) 


        latitude_list_k0 = []
        longitude_list_k0 = []
        for (x,y) in k0:
            latitude_list_k0.append(x)
            longitude_list_k0.append(y)

        latitude_list_k1 = []
        longitude_list_k1 = []
        for (x,y) in k1:
            latitude_list_k1.append(x)
            longitude_list_k1.append(y)

        latitude_list_k2 = []
        longitude_list_k2 = []
        for (x,y) in k2:
            latitude_list_k2.append(x)
            longitude_list_k2.append(y)

        latitude_list_k3 = []
        longitude_list_k3 = []
        for (x,y) in k3:
            latitude_list_k3.append(x)
            longitude_list_k3.append(y)

        latitude_list_k4 = []
        longitude_list_k4 = []
        for (x,y) in k4:
            latitude_list_k4.append(x)
            longitude_list_k4.append(y)

        latitude_list_k5 = []
        longitude_list_k5 = []
        for (x,y) in k5:
            latitude_list_k5.append(x)
            longitude_list_k5.append(y)


        gmapAfter = gmplot.GoogleMapPlotter(42.283772, 
                                        -71.347290, 13) 
        
        after_size = 40

        # scatter points on the google map 
        gmapAfter.scatter( latitude_list_k0, longitude_list_k0, '# FF0000', 
                                      size = after_size, marker = False ) 
        # scatter points on the google map 
        gmapAfter.scatter( latitude_list_k1, longitude_list_k1, '# FF0000', 
                                      size = after_size, marker = False ) 
        # scatter points on the google map 
        gmapAfter.scatter( latitude_list_k2, longitude_list_k2, '# FF0000', 
                                      size = after_size, marker = False ) 
        # scatter points on the google map 
        gmapAfter.scatter( latitude_list_k3, longitude_list_k3, '# FF0000', 
                                      size = after_size, marker = False ) 
        # scatter points on the google map 
        gmapAfter.scatter( latitude_list_k4, longitude_list_k4, '# FF0000', 
                                      size = after_size, marker = False ) 
        # scatter points on the google map 
        gmapAfter.scatter( latitude_list_k5, longitude_list_k5, '# FF0000', 
                                      size = after_size, marker = False ) 

        gmapAfter.draw( "D:\\_Documents\\cs504\\Project2\\course-2019-spr-proj\\mapAfter.html" ) 
        
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

        resource_HomesLatLng = doc.entity('dat:HomesLatLng', {'prov:label':'Homes Latitude and Longitude', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_StopsLatLng = doc.entity('dat:StopsLatLng', {'prov:label':'Stops Latitude and Longitude', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})


        get_registered_students = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_property_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_gas_emissions = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_work_zones = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_traffic_count = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_asap_student_address = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_student_address = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_HomesLatLng = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_StopsLatLng = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_k_means_0 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_k_means_1 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_k_means_2 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_k_means_3 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_k_means_4 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_k_means_5 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_registered_students, this_script)
        doc.wasAssociatedWith(get_property_data, this_script)
        doc.wasAssociatedWith(get_gas_emissions, this_script)
        doc.wasAssociatedWith(get_work_zones, this_script)
        doc.wasAssociatedWith(get_traffic_count, this_script)

        doc.wasAssociatedWith(get_asap_student_address, this_script)
        doc.wasAssociatedWith(get_student_address, this_script)
        doc.wasAssociatedWith(get_HomesLatLng, this_script)
        doc.wasAssociatedWith(get_StopsLatLng, this_script)

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

        doc.usage(get_HomesLatLng, resource_HomesLatLng, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )
        doc.usage(get_StopsLatLng, resource_StopsLatLng, startTime, None,
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


        Homes_Lat_Lng = doc.entity('dat:ido_jconstan_jeansolo_suitcase#HomesLatLng', {prov.model.PROV_LABEL:'Homes Latitude and Longitude', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Homes_Lat_Lng, this_script)
        doc.wasGeneratedBy(student_address, get_HomesLatLng, endTime)
        doc.wasDerivedFrom(Homes_Lat_Lng, resource_HomesLatLng, get_HomesLatLng, get_HomesLatLng, get_HomesLatLng)

        Stops_Lat_Lng = doc.entity('dat:ido_jconstan_jeansolo_suitcase#StopsLatLng', {prov.model.PROV_LABEL:'Stops Latitude and Longitude', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Stops_Lat_Lng, this_script)
        doc.wasGeneratedBy(Stops_Lat_Lng, get_StopsLatLng, endTime)
        doc.wasDerivedFrom(Stops_Lat_Lng, resource_StopsLatLng, get_StopsLatLng, get_StopsLatLng, get_StopsLatLng)

        k_means_0 = doc.entity('dat:ido_jconstan_jeansolo_suitcase#k-means_0', {prov.model.PROV_LABEL:'k-means stop 0', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_means_0, this_script)
        doc.wasGeneratedBy(k_means_0, get_k_means_0, endTime)
        doc.wasDerivedFrom(k_means_0, resource_k_means_0, get_k_means_0, get_k_means_0, get_k_means_0)

        k_means_1 = doc.entity('dat:ido_jconstan_jeansolo_suitcase#k-means_1', {prov.model.PROV_LABEL:'k-means stop 1', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_means_1, this_script)
        doc.wasGeneratedBy(k_means_1, get_k_means_1, endTime)
        doc.wasDerivedFrom(k_means_1, resource_k_means_1, get_k_means_1, get_k_means_1, get_k_means_1)

        k_means_2 = doc.entity('dat:ido_jconstan_jeansolo_suitcase#k-means_2', {prov.model.PROV_LABEL:'k-means stop 2', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_means_2, this_script)
        doc.wasGeneratedBy(k_means_2, get_k_means_2, endTime)
        doc.wasDerivedFrom(k_means_2, resource_k_means_2, get_k_means_2, get_k_means_2, get_k_means_2)

        k_means_3 = doc.entity('dat:ido_jconstan_jeansolo_suitcase#k-means_3', {prov.model.PROV_LABEL:'k-means stop 3', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_means_3, this_script)
        doc.wasGeneratedBy(k_means_3, get_k_means_3, endTime)
        doc.wasDerivedFrom(k_means_3, resource_k_means_3, get_k_means_3, get_k_means_3, get_k_means_3)

        k_means_4 = doc.entity('dat:ido_jconstan_jeansolo_suitcase#k-means_4', {prov.model.PROV_LABEL:'k-means stop 4', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_means_4, this_script)
        doc.wasGeneratedBy(k_means_4, get_k_means_4, endTime)
        doc.wasDerivedFrom(k_means_4, resource_k_means_4, get_k_means_4, get_k_means_4, get_k_means_4)

        k_means_5 = doc.entity('dat:ido_jconstan_jeansolo_suitcase#k-means_5', {prov.model.PROV_LABEL:'k-means stop 5', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_means_5, this_script)
        doc.wasGeneratedBy(k_means_5, get_k_means_5, endTime)
        doc.wasDerivedFrom(k_means_5, resource_k_means_5, get_k_means_5, get_k_means_5, get_k_means_5)


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

def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

def p(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 2000):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))

def proportionAgreement(x):
    count = 0
    total = 0
    for i,j in x: 
        if i == j:
            count += 1
        total += 1
    return count/total 

def pointParser(point_list):
    point_list_float = []

    for i in point_list:
        lst_lat = []
        lst_lng = []
        str_lat = ''
        str_lng = ''
        flt_lat = 0.0
        flt_lng = 0.0

        # create a tuple from this
    
        charIdx = 0
        # lat value
        if i[0][charIdx] == '(':
            charIdx += 1
            lst_lat.append(i[0][charIdx])

            # get vals until we hit a '.'
            charIdx += 1
            while i[0][charIdx] != '.':
                lst_lat.append(i[0][charIdx])
                charIdx += 1
            # append the '.'
            lst_lat.append(i[0][charIdx])

            # get vals until we hit a ','
            charIdx += 1
            while i[0][charIdx] != ',':
                lst_lat.append(i[0][charIdx])
                charIdx += 1
            # skip ',_'
            charIdx += 2

            # lng value
            # get vals until we hit a '.'
            while i[0][charIdx] != '.':
                lst_lng.append(i[0][charIdx])
                charIdx += 1
            # append the '.'
            lst_lng.append(i[0][charIdx])

            # get vals until we hit a ')'
            charIdx += 1
            while i[0][charIdx] != ')':
                lst_lng.append(i[0][charIdx])
                charIdx += 1
        
            # done parsing
            str_lat = ''.join(lst_lat)
            str_lng = ''.join(lst_lng)
            flt_lat = float(str_lat)
            flt_lng = float (str_lng)
            point_list_float.append((flt_lat, flt_lng))


    return point_list_float
'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''