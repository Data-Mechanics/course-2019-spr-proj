from flask import Flask, url_for
from flask_cors import *
import json

app = Flask(__name__)
CORS(app, resources=r'/*')


@app.route('/')
def index():
    with open('./static/index.html', encoding='utf-8') as html:
        return html.read()


@app.route('/js/<name>')
def get_js(name=None):
    with open('./static/js/'+name, encoding='utf-8') as js:
        return js.read()


@app.route('/css/<name>')
def get_css(name=None):
    with open('./static/css/'+name, encoding='utf-8') as css:
        return css.read()


@app.route('/words')
def words():
    with open('./data/words.json') as words_file:
        return words_file.read()




if __name__ == '__main__':
    app.run()
