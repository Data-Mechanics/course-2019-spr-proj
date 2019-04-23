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

import sys
import os
import importlib
import json
import argparse
import prov.model
import dml

app = Flask(__name__)
app.secret_key = 'super secret string'	# Change this!

def execute():
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
		algorithm.execute(trial=False)
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
	return render_template('hello.html', message="Welcome!  Click execute to run the program.")
	
@app.route("/execute", methods=['GET'])
def ex_page():
	execute()
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('charr_hu38_npearce', 'charr_hu38_npearce')
	kmeans = str(list(repo.charr_hu38_npearce.Kmeans.find())[0])
	return render_template('hello.html', message=kmeans)
	

if __name__ == "__main__":
	#this is invoked when in the shell	you run 
	#$ python app.py 
	app.run(port=5000, debug=True)