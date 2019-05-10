from flask import Flask, render_template, request

import minStations

app = Flask(__name__)

@app.route('/')
def hello():
    return render_template('index.html')

@app.route('/map', methods=["GET", "POST"])
def map():
  foo = minStations.minStations()

  if request.method == "POST":
  	bar = foo.execute(trial=True, numberOfPoints=int(request.form['number']))
  else:
  	bar = foo.execute(trial=True, numberOfPoints=30)
  print("hello")
  print(bar)
  return render_template('map.html', bar=bar, apple="word")

@app.route('/analysis', methods=["GET", "POST"])
def analyze():
  return render_template('analysis.html')
