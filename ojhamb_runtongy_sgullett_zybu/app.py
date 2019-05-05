import flask
from flask import Flask, Response, request, render_template, redirect, url_for, jsonify
from flask_pymongo import PyMongo
import json
import sys
sys.path.append('..')
import dml
import urllib.request

app = Flask(__name__)
app.config['MONGO_DBNAME']='repo'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/repo'
mongo = PyMongo(app)

#default page
@app.route("/")
def hello():
    return render_template('hello.html')

#LinkedIn Skills Data page
@app.route("/skills")
def skills():
    return render_template('skills.html')

#LinkedIn univerisities Data page
@app.route("/universities")
def univerisities():
    return render_template('universities.html')

#LinkedIn degrees Data page
@app.route("/degrees")
def degrees():
    return render_template('degrees.html')

linkedin_data_new = []

#LinkedinPage
@app.route("/linkedin", methods=['GET'])
def get_linkedin():
    profiles = mongo.db.ojhamb_runtongy_sgullett_zybu.linkedin
    linkedin_data = []
    for s in profiles.find():
        linkedin_data.append({"Full name":s["Full name"], "Organization 1":s["Organization 1"], "Organization Title 1":s["Organization Title 1"],
                       "Organization Start 1":s["Organization Start 1"], "Organization End 1":s["Organization End 1"],
                       "Education 1":s["Education 1"], "Education Degree 1":s["Education Degree 1"], "Skills":s["Skills"],
                       "English":s["English"]})
    whole_list = linkedin_data_new+linkedin_data
    return jsonify(whole_list)

#Insert LinkedIn Data page
@app.route("/linkedin", methods=['POST'])
def get_data():
    new_name = request.form["client-name"]
    new_eduins = request.form["client-uni"]
    new_degree = request.form["client-degree"]
    new_comp = request.form["client-comp"]
    new_title = request.form["client-title"]
    new_eng = request.form.get('client-eng', type=int)
    new_skill = request.form["client-skill"]
    new_data = {"Full name":new_name, "Organization 1":new_comp, "Organization Title 1":new_title,
    "Organization Start 1":'', "Organization End 1":'', "Education 1":new_eduins, "Education Degree 1":new_degree,
                "Skills":new_skill,"English":new_eng}
    linkedin_data_new.append(new_data)
    return jsonify(new_data)

#Get a list of top skills from MongoDB
@app.route("/getskills", methods=['GET'])
def get_top_skills():
    skills = mongo.db.ojhamb_runtongy_sgullett_zybu.JobAnalysis
    output = []
    for s in skills.find():
        keys = list(s.keys())
        key = keys[1]
        output.append({key:s[key]})
    return jsonify(output)

#Get a list of popular universities from MongoDB
@app.route("/getuniversities", methods=['GET'])
def get_unis():
    unis = mongo.db.ojhamb_runtongy_sgullett_zybu.JobAnalysis2
    output = []
    for s in unis.find():
        keys = list(s.keys())
        key = keys[1]
        output.append({key:s[key]})
    return jsonify(output)

#Get a list of degrees from MongoDB
@app.route("/getdegrees", methods=['GET'])
def get_degrees():
    degrees = mongo.db.ojhamb_runtongy_sgullett_zybu.JobAnalysis3
    output = []
    for s in degrees.find():
        keys = list(s.keys())
        for x in keys[1:]:
            output.append({x:s[x]})
    return jsonify(output)

@app.route("/insert")
def inserts():
    return render_template('insert.html')



if __name__ == "__main__":
    app.run(port=5000, debug=True)