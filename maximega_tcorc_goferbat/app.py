import flask
import requests
import json
from flask import Flask, request, render_template, redirect, url_for, session

from kmeans_app import run


app = Flask(__name__)

@app.route('/')
def profile():
    return render_template('home.html')

@app.route('/result', methods=['GET'])
def result():
    zones = request.args['zones']
    mx = request.args['max']
    mn = request.args['min']
    factor = request.args['factor']
    if zones == '':
        zones = 5
    if mx == '':
        mx = 20
    if mn == '':
        mn = 1
    if factor == '':
        factor = 1.4
    data = run(int(zones), float(mx), float(mn), float(factor))

    return render_template('result.html', data = data)

if __name__ == "__main__":
    # this is invoked when in the shell  you run
    # $ python app.py
    app.run(port=8080, debug=True)
