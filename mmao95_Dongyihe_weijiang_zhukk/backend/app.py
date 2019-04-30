from flask import Flask, url_for, send_file
# from flask_cors import *
import json

app = Flask(__name__)
# CORS(app, resources=r'/*')


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
    return send_file('./static/index.html')


@app.route('/data/<file>')
def get_data(file):
    return send_file('./data/' + file)


@app.route('/js/<name>')
def get_js(name=None):
    return send_file('./static/js/'+name)


@app.route('/css/<name>')
def get_css(name=None):
    return send_file('./static/css/'+name)


def str2list(s, bar=1, sep=' '):
    word_list = list(map(lambda w: (w, 1), s.split(sep)))
    word_list = reduce(lambda k, vs: [k, sum(vs)], word_list)
    return list(filter(lambda p: p[1] >= bar, word_list))


def sortdict(src, des):
    for key in sorted(src):
        des[key] = src[key]
    return des


@app.route('/neighbourhood')
def neighbourhood():
    with open('./data/neighbourhood.json', encoding='utf-8') as words_file:
        words = json.load(words_file)
        tmp = {}
        total = ''
        for key in words:
            tmp[key] = str2list(words[key])
            total += words[key]
        result = {'Entire Boston': str2list(total, 2)}
        return json.dumps(sortdict(tmp, result))


@app.route('/cluster')
def cluster():
    with open('./data/cluster.json', encoding='utf-8') as words_file:
        clusters = json.load(words_file)
        result = {}
        for c in clusters:
            result['Cluster ' + str(c['Cluster'])] = str2list(c['Names'], sep=',')
        return json.dumps(result)

if __name__ == '__main__':
    app.run()
