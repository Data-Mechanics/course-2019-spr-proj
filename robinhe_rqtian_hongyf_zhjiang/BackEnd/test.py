from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/')
def root():
    t = {
        'a': 1,
        'b': 2,
        'c': [3, 4, 5]
    }
    t2 ={'_id': ObjectId('5cc8d25c89dc480da7d6c7a2'), 'number': 1403053, 'date': '11/25/01', 'severity': 'Non-fatal injury', 'light': 'Dark - lighted roadway', 'road': 'Wet', 'weather': 'Rain', 'lat': 42.442067749916035, 'lon': -71.0143856960369}
    return jsonify(t)

if __name__ == '__main__':
    app.debug = True
    app.run()