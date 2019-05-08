import flask
from flask import Flask, Response, request, render_template, redirect, url_for
import dml
from plotter import plot as plt
from plotter import plot2 as plt2
# from flaskext.mysql import MySQL
# #import flask.ext.login as flask_login
# import flask_login
# #for image uploading
# from werkzeug import secure_filename
# import os, base64
# import collections

# mysql = MySQL()
app = Flask(__name__)
app.secret_key = '0123456789'  # Change this!
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

@app.route('/', methods=['GET'])
def hello():
	if flask.request.method == 'GET':
	    return render_template('hello.html')

@app.route('/q1', methods=['GET'])
def q1():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q1')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/q2', methods=['GET'])
def q2():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q2')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/q3', methods=['GET'])
def q3():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q3')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/q4', methods=['GET'])
def q4():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q4')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/q5', methods=['GET'])
def q5():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q5')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/q6', methods=['GET'])
def q6():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q6')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/q7', methods=['GET'])
def q7():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q7')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/q8', methods=['GET'])
def q8():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q8')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/q9', methods=['GET'])
def q9():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q9')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/q10', methods=['GET'])
def q10():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q10')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/q11', methods=['GET'])
def q11():
    if flask.request.method == 'GET':
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

        questions = repo.aheckman_jfimbres.questions.find({})
        questions = questions[0]
        q = questions.get('q11')
        plt(q)

        return render_template('hello.html',message='image')

@app.route('/relScatter', methods=['GET','POST'])
def find_state():
    state = request.form.get("state")

    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('aheckman_jfimbres', 'aheckman_jfimbres')

    epc = repo.aheckman_jfimbres.emissions_per_capita.find({})
    epc = epc[0]
    eff = repo.aheckman_jfimbres.carbon_efficacy.find({})
    eff = eff[0]

    coord = [eff.get(state), epc.get(state)]

    plt2(coord, eff.values(), epc.values())

    return render_template('hello.html',message2='image')


if __name__ == "__main__":
	#this is invoked when in the shell  you run 
	#$ python app.py 
	app.run(port=2000, debug=True)