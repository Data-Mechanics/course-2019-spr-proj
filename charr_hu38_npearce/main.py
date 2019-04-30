##############################################################################################
## 
## main.py
##
## Script for running a single project's data and provenance workflows and output to Flask App
##
##	 Web:	  datamechanics.org
##	 Version: 0.0.1
##
##

import flask
from flask import Flask, Response, request, render_template, redirect, url_for
from datetime import date

from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from mpl_toolkits.basemap import Basemap

import io
import sys
import os
import importlib
import json
import argparse
import prov.model
import dml
import numpy as np

app = Flask(__name__)
app.secret_key = 'super secret string'	# Change this!

def execute(max_num=10):
	# Extract the algorithm classes from the modules in the
	# subdirectory specified on the command line.
		
	path="Algorithms"
	algorithms = []
	for r,d,f in os.walk(path):
		for file in f:
			if r.find(os.sep) == -1 and file.split(".")[-1] == "py" and not file.split(".")[0] == "main":
				name_module = ".".join(file.split(".")[0:-1])
				module = importlib.import_module(path + "." + name_module)
				algorithms.append(module.__dict__[name_module])

	# Create an ordering of the algorithms based on the data
	# sets that they read and write.
	datasets = set()
	ordered = []
	while len(algorithms) > 0:
		for i in range(0,len(algorithms)):
			print(algorithms[i])
			if set(algorithms[i].reads).issubset(datasets):
				datasets = datasets | set(algorithms[i].writes)
				ordered.append(algorithms[i])
				del algorithms[i]
				break
	# Execute the algorithms in order.
	provenance = prov.model.ProvDocument()
	for algorithm in ordered:
		algorithm.execute(trial=False,max_num=max_num)
		provenance = algorithm.provenance(provenance)

	# Display a provenance record of the overall execution process.
	print(provenance.get_provn())

	# Render the provenance document as an interactive graph.
	prov_json = json.loads(provenance.serialize())
	import protoql
	agents = [[a] for a in prov_json['agent']]
	entities = [[e] for e in prov_json['entity']]
	activities = [[v] for v in prov_json['activity']]
	wasAssociatedWith = [(v['prov:activity'], v['prov:agent'], 'wasAssociatedWith') for v in prov_json['wasAssociatedWith'].values()]
	wasAttributedTo = [(v['prov:entity'], v['prov:agent'], 'wasAttributedTo') for v in prov_json['wasAttributedTo'].values()]
	wasDerivedFrom = [(v['prov:usedEntity'], v['prov:generatedEntity'], 'wasDerivedFrom') for v in prov_json['wasDerivedFrom'].values()]
	wasGeneratedBy = [(v['prov:entity'], v['prov:activity'], 'wasGeneratedBy') for v in prov_json['wasGeneratedBy'].values()]
	used = [(v['prov:activity'], v['prov:entity'], 'used') for v in prov_json['used'].values()]
	open('provenance.html', 'w').write(protoql.html("graph(" + str(entities + agents + activities) + ", " + str(wasAssociatedWith + wasAttributedTo + wasDerivedFrom + wasGeneratedBy + used) + ")"))


	
#default page  
@app.route("/", methods=['GET'])
def homepage():
	return render_template('hello.html', message="Welcome!	Click execute to run the program.")

#execute page
@app.route("/execute/", methods=['GET','POST'])
def ex_page():
	#Render input page
	if request.method == 'GET':
		return render_template('input.html') 
		
	#Recieve Info from Input
	try:	
		budget=request.form.get('budget')
		city=request.form.get('city')
		size=request.form.get('size')
	except:
		print("couldn't find all tokens") #this prints to shell, end users will not see this (all print statements go to shell)
		return flask.redirect(flask.url_for('ex_page'))
		
	#Figure out max station amount
	switcher = {	#Construction Costs obtained from https://engineering.wustl.edu/current-students/student-services/ecc/documents/heda.pdf
				"11": 40000,
				"15": 48000,
				"19": 58000
				}
	max_num=int(budget)//(switcher.get(str(size)))
		
	#Run Algorithm
	execute(max_num=max_num)
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('charr_hu38_npearce', 'charr_hu38_npearce')
	
	#Find data for correct city
	kmeans = str(list(repo.charr_hu38_npearce.Kmeans.find())[int(city)]["locs"])
	cities=["Boston", "Washington", "New York", "Chicago", "San Francisco"]
	return render_template('response.html', max_num=max_num, city=cities[int(city)], locs=kmeans)

#route visualizations
@app.route('/fig.png', methods=['GET'])
def fig_png():
    city = int(request.args.get('city'))
    max_num = int(request.args.get('max_num'))
    fig = create_figure(city, max_num)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

#render visualizations
def create_figure(city, max_num):
    #bounding box coordinates by city
    bbox = [(-71.19126004,42.22765398,-70.8044881,42.39697748),
            (-77.11976633,38.79163024,-76.90936606,38.99585237),
            (-74.25909008,40.47739894,-73.70018092,40.91617849),
            (-87.94010101,41.64391896,-87.5239841,42.02302188),
            (-122.599628661,37.64031423,-122.28178006,37.92984427)
            ]
    l, d, r, u = bbox[city]
    #connect to mongo
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('charr_hu38_npearce', 'charr_hu38_npearce')
    #access new station locations
    lon_new = []
    lat_new = []
    new_locs = list(repo.charr_hu38_npearce.Kmeans.find())[city]["locs"]
    for loc in new_locs:
        lon_new.append(loc[0])
        lat_new.append(loc[1])
    #access old station locations
    if city == 0:
        old_locs = list(repo.charr_hu38_npearce.boston_s.find())
    elif city == 1:
        old_locs = list(repo.charr_hu38_npearce.washington_s.find())
    elif city == 2:
        old_locs = list(repo.charr_hu38_npearce.newyork_s.find())
    elif city == 3:
        old_locs = list(repo.charr_hu38_npearce.chicago_s.find())
    else:
        old_locs = list(repo.charr_hu38_npearce.sanfran_s.find())
    lon_old = []
    lat_old = []
    for loc in old_locs:
        lon_old.append(loc["lon"])
        lat_old.append(loc["lat"])
    #access population
    population = list(repo.charr_hu38_npearce.unionpopbike.find())[city]["population"]
    #access regression info
    regress = list(repo.charr_hu38_npearce.optstationnum.find())
    coef = regress[0]["coef"]
    x1 = []
    y1 = []
    for city in regress:
        x1.append(city["x"])
        y1.appen(city["y"])
    #instantiate figure and add map
    fig = Figure()
    ax = fig.add_subplot(211)
    m = Basemap(projection='merc',llcrnrlat=d,urcrnrlat=u,llcrnrlon=l,urcrnrlon=r,\
                resolution='f')
    m.fillcontinents()
    m.scatter(lon_old,lat_old,latlon=True,c='#000000',marker='o')
    m.scatter(lon_new,lat_new,latlon=True,c='#ff0000',marker='o')
    ax.title("Existing Station Locations (black) and Suggested Station Locations (red)")
    #add graph
    ax = fig.add_subplot(212)
    bound = (len(lon_old) + max_num) / population
    x2 = np.linspace(0,(1.2*bound))
    y2 = coef*x2
    y3 = np.linspace(0,(1.2*(bound*coef)))
    x3 = np.full_like(y3,bound)
    ax.scatter(x1,y1,c='#000000',marker='o')
    ax.plot(x2,y2,'b-')
    ax.plot(x3,y3,'r--')
    ax.xlabel("Per Capita Bike Station Amount (# of bike stations/city population)")
    ax.ylabel("Per Capita Bike Use in Sept 2018 (total bike use in minutes/city population)")
    ax.title("Bounded Linear Regression Demonstrating Optimality of Maximizing Number of Bike Stations")
    return fig
    


if __name__ == "__main__":
	#this is invoked when in the shell	you run 
	#$ python app.py 
	app.run(port=5000, debug=True)