from flask import Flask, render_template

app = Flask(__name__)

@@app.route('/', methods = 'GET')
def welcome():
    return render_template('welcome.html')

@app.route("/score", methods=['GET'])
def get_avgScore():
	return render_template('score.html')

@app.route("/question", methods=['GET'])
def get_questionRatio():
	return render_template('question.html')

if __name__ == '__main__':
    app.run(port=5100, debug=True)