from flask import Flask, render_template
from flask import send_from_directory

app = Flask(__name__, static_url_path='')


@app.route("/", methods=['GET'])
def welcome():
	return render_template('welcome.html')

@app.route("/chart", methods=['GET'])
def get_chart():
	return render_template('chart.html')

@app.route("/score", methods=['GET'])
def get_avgScore():
	return render_template('score.html')

@app.route("/question", methods=['GET'])
def get_questionRatio():
	return render_template('question.html') 

@app.route("/img/<path:file>")
def get_res(file):
	return send_from_directory('img', file)

if __name__ == '__main__':
    app.run(port=5200, debug=True)