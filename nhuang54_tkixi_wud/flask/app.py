from flask import Flask, render_template, request

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

@app.route('/visualize', methods=['POST'])
def visualize():
	month1 = request.form['month1']
	month2 = request.form['month2']
	month3 = request.form['month3']
	month4 = request.form['month4']
	month5 = request.form['month5']
	month6 = request.form['month6']
	month7 = request.form['month7']
	month8 = request.form['month8']
	month9 = request.form['month9']
	month10 = request.form['month10']
	month11 = request.form['month11']
	month12 = request.form['month12']

	

	## pull all the collisions of the particular months (lat,lng in list)
	## pass that lat lng to the template
	## template would then load all the lat lng to the map

	return render_template('analysis.html', plots)