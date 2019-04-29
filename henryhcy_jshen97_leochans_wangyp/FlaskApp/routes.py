from flask import render_template, redirect, request, flash
from FlaskApp import app, db
from FlaskApp.models import User
from FlaskApp.forms import RateForm
from FlaskApp import repo,data
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


@app.route('/task2')
def task2():
    return render_template('task2.html', title='A Casual Exploration')


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
