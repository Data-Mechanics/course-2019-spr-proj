from flask import Flask, render_template, request

import minStations

app = Flask(__name__)

@app.route('/')
def hello():
    return render_template('index.html')

@app.route('/map', methods=["GET", "POST"])
def map():
  return render_template('map.html')

@app.route('/analysis', methods=["GET", "POST"])
def analyze():
  return render_template('analysis.html')