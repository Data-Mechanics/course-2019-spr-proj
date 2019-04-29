from flask import render_template, redirect, request, flash
from FlaskApp import app, db, repo
from FlaskApp.models import User
from FlaskApp.forms import RateForm
from bson.json_util import dumps
from urllib.request import urlopen
from mapping import Mapping
import dml
import datetime
import geopy.distance
import json
import prov.model
import pprint
import random
import uuid
from FlaskApp.forms import RateForm, Task2From
import geopy.distance
import itertools
import math
import z3


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html', title='Project3-Homepage')


@app.route('/task1')
def task1():        
    Mapping()
    return render_template('task1.html')


    
#     map_data = repo.henryhcy_jshen97_leochans_wangyp.neighborhoods.find_one()
#     map_data = dumps(map_data)
#     map_data_ = dumps(data)
#     print(data)
#     geojson_stores = {
#     "type": "FeatureCollection",
#     "features": [
#     {
#         "type": "Feature",
#         "geometry" : {
#             "type": "Point",
#             "coordinates": [item['location']["lng"], item['location']["lat"]],
#             },
#         "properties" : {
#             'name': item["name"]
#         }
#      } for item in repo.henryhcy_jshen97_leochans_wangyp.cvsWalgreen.find()]
# }
    
#     stores = dumps(geojson_stores)
#     return render_template('task1.html', map_data = map_data_,store_data=stores, title='Quantify Rivalry')


@app.route('/task2', methods=['GET', 'POST'])
def task2():
    form = Task2From()

    if request.method == 'GET':
        return render_template('task2.html', title='A Casual Exploration', form=form)
    elif request.method == 'POST':
        if form.validate_on_submit():
            #first selection--------------------------------------------------------------------------------------------
            place_id1 = form.Stores.data
            if place_id1 != 'None':
                evi_cases1 = 0
                cri_cases1 = 0
                rating1 = 0.0
                lat1 = 0.0
                lng1 = 0.0
                for document in repo.henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS.find({'place_id': place_id1}):
                    evi_cases1 = document['eviction_case']
                    cri_cases1 = document['crime_case']
                    rating1 = document['rating']
                    lat1 = document['location']['lat']
                    lng1 = document['location']['lng']

                vicinity1 = ''
                for document in repo.henryhcy_jshen97_leochans_wangyp.cvs.find({'place_id': place_id1}):
                    vicinity1 = document['vicinity']

                flash("Result1--Store1: {}; Rating: {}; Evictions: {}; Larcenies: {}.".format(vicinity1, rating1, evi_cases1, cri_cases1), 'error')

            #second selection-------------------------------------------------------------------------------------------
            place_id2 = form.Compare.data
            if (place_id2 != 'None' and place_id1!= 'None'):
                if (place_id2 == place_id1):
                    flash("Result2--Please select a different store.")
                else:
                    evi_cases2 = 0
                    cri_cases2 = 0
                    rating2 = 0.0
                    lat2 = 0.0
                    lng2 = 0.0
                    for document in repo.henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS.find({'place_id': place_id2}):
                        evi_cases2 = document['eviction_case']
                        cri_cases2 = document['crime_case']
                        rating2 = document['rating']
                        lat2 = document['location']['lat']
                        lng2 = document['location']['lng']

                    coord1 = (lat1, lng1)
                    coord2 = (lat2, lng2)
                    distance =geopy.distance.distance(coord1, coord2)

                    evi_case_diff = (evi_cases1 - evi_cases2)
                    cri_case_diff = (cri_cases1 - cri_cases2)

                    vicinity2 = ''
                    for document in repo.henryhcy_jshen97_leochans_wangyp.cvs.find({'place_id': place_id2}):
                        vicinity2 = document['vicinity']

                    flash("Result2--Store2: {}; Rating {}; DistanceTo: {}; Store1: {}; Evictions diff: {} Larcenies diff {}.".format(vicinity2, rating2, distance, vicinity1, evi_case_diff, cri_case_diff))

            #third selection--------------------------------------------------------------------------------------------
            place_id3 = form.Total.data
            if (place_id3 != "None" and place_id1 != 'None' and place_id2 != 'None'):
                if (place_id3 == place_id1):
                    flash("Result3--You select a the same store as store1. Please Select a different one.")
                elif (place_id3 == place_id2):
                    flash("Result3--You select a the same store as store2. Please Select a different one.")
                else:
                    corr_evi = 0.0
                    corr_cri = 0.0
                    for document in repo.henryhcy_jshen97_leochans_wangyp.correlationCVS.find():
                        if document['document_type'] == 'rating_eviction_correlation':
                            corr_evi = document['corr']
                        elif document['document_type'] == 'rating_crime_correlation':
                            corr_cri = document['corr']
                    c = corr_cri/corr_evi

                    input_list = [place_id1, place_id2, place_id3]
                    rating_list = []
                    coord_list = []
                    total_stab = 0.0
                    for document in repo.henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS.find({'place_id': {'$in': input_list}}):
                        rating_list.append(document['rating'])
                        total_stab += math.pow(document['crime_case'], c)/(document['eviction_case'])
                        coord_list.append( (document['location']['lat'], document['location']['lng']) )
                    dist01 = geopy.distance.distance(coord_list[0], coord_list[1]).km
                    dist02 = geopy.distance.distance(coord_list[0], coord_list[2]).km
                    dist12 = geopy.distance.distance(coord_list[1], coord_list[2]).km
                    total_access = dist01 + dist02 + dist12

                    vicinity3 = ''
                    for document in repo.henryhcy_jshen97_leochans_wangyp.cvs.find({'place_id': place_id3}):
                        vicinity3 = document['vicinity']

                    flash("Result3--Store1: {}; Store2: {}; Store3: {}; Total Stability S: {}; Total Accessibilty A = {}; Respective Ratings: {}, {}, {}.".format(vicinity1, vicinity2, vicinity3, total_stab, total_access, rating_list[0], rating_list[1], rating_list[2]))

            #fourth, fifth, and sixth selection--------------------------------------------------------------------------------------------
            K = int(form.K.data)
            S = form.S.data
            A = form.A.data

            corr_evi = 0.0
            corr_cri = 0.0
            for document in repo.henryhcy_jshen97_leochans_wangyp.correlationCVS.find():
                if document['document_type'] == 'rating_eviction_correlation':
                    corr_evi = document['corr']
                elif document['document_type'] == 'rating_crime_correlation':
                    corr_cri = document['corr']
            c = corr_cri / corr_evi

            pid_list = []
            geo_list = []
            stab_list = []
            for document in repo.henryhcy_jshen97_leochans_wangyp.countEvictionCrimeCVS.find():
                pid = document['place_id']
                location_coordinate = (document['location']['lat'], document['location']['lng'])
                stability = (math.pow(document['crime_case'], c) / document['eviction_case']) * 1000

                pid_list.append(pid)
                geo_list.append(location_coordinate)
                stab_list.append(stability)
            addr_list = []
            for document in repo.henryhcy_jshen97_leochans_wangyp.cvs.find({'place_id': {'$in': pid_list}}):
                addr_list.append(document['vicinity'])

            print(max(stab_list), min(stab_list))

            solver = z3.Solver()
            X = [z3.Real('x{}'.format(i)) for i in range(len(pid_list))]

            # constraints:
            # (1) choose exactly K stores
            for i in X:
                solver.add(z3.Or(i == 0, i == 1))
            solver.add(sum(X) == K)
            # (2) the total stability of K stores must greater than or equal to S
            solver.add(sum([X[i]*stab_list[i] for i in range(len(pid_list))]) >= S)
            # (3) the total accessibility of k stores must greater than or equal to A
            two_subset = list(itertools.combinations([i for i in range(len(pid_list))], 2))
            solver.add(sum([X[i[0]] * X[i[1]] * (geopy.distance.distance(geo_list[i[0]], geo_list[i[1]]).km) for i in two_subset]) >= A)

            # get the solution and return their address
            solution_list = []
            if (solver.check() == z3.sat):
                m = solver.model()
                for i in range(len(pid_list)):
                    name = 'x{}'.format(i)
                    if (m[z3.Real(name)] == 1):
                        solution_list.append(addr_list[i])
            else:
                solution_list.append('not solution found')
            flash("The solution for K-salesmen is: {}".format(str(solution_list)))

            return redirect('/task2')
        else:
            flash("We are sorry. Something went wrong.")
            return render_template('task2.html', title='A Casual Exploration', form=form)


@app.route('/task2/map')
def task2map():
    return render_template('task2map.html', title='Task2 Map')


@app.route('/report')
def report():
    return render_template('report.html', title='Final Report')


@app.route('/feedback', methods=['GET', 'POST'])
def feedback():
    form = RateForm()

    if request.method == 'GET':
        return render_template('feedback.html', title='Project3-Feedback', form=form)
    elif request.method == 'POST':
        if form.validate_on_submit():
            user = User(name=form.Name.data, ratings=form.Ratings.data, comments=form.Comments.data)
            db.session.add(user)
            db.session.commit()
            flash("Thank you for your time!")
            return redirect('/index')
        else:
            flash("Name field is required.")
            return render_template('feedback.html', title='Project3-Feedback', form=form)
