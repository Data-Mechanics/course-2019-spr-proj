from flask import Flask, url_for
from flask_cors import *
import json

app = Flask(__name__)
CORS(app, resources=r'/*')


def reduce(f, R):
    keys = {k for (k, v) in R}
    return [f(k1, [v for (k2, v) in R if k1 == k2]) for k1 in keys]


def union(R, S):
    return R + S


def difference(R, S):
    return [t for t in R if t not in S]


def intersect(R, S):
    return [t for t in R if t in S]


def project(R, p):
    return [p(t) for t in R]


def select(R, s):
    return [t for t in R if s(t)]


def product(R, S):
    return [(t, u) for t in R for u in S]


def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k, v) in R if k == key])) for key in keys]

def sum(vs):
    s = 0
    for v in vs:
        s += v
    return s

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


@app.route('/zipcode')
def words():
    with open('./data/zipcode.json', encoding='utf-8') as words_file:
        words = json.load(words_file)
        result = {}

        for key in words:
            word_list = list(map(lambda w : (w, 1), words[key].split(' ')))
            result[key] = reduce(lambda k, vs: [k, sum(vs)], word_list)

        return json.dumps(result)



if __name__ == '__main__':
    app.run()
