import json
from flask import Flask, request, render_template

app = Flask(__name__)

@app.route("/")
def fun(name=None):
    return render_template('index.html',name=name)

@app.route("/accidents/")
def fun1(name=None):
    return render_template('indexAccident.html', name=name)

@app.route("/crimes/")
def fun2(name=None):
    return render_template('indexCrime.html', name=name)


if __name__ == '__main__':
    print('This runs on http://127.0.0.1:5000/')
    app.run(debug=False)
