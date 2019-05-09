from flask import Flask, jsonify, abort, make_response, request, send_file, render_template
from k_mean_compute import kMeanCompute

app = Flask(__name__)
kmeanService = None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/app/api/v0.1/compute/<int:k_val>', methods=['GET'])
def compute_kval_k(k_val):
    kmean_filename = kmeanService.compute_kmean(k_val)
    return send_file(kmean_filename)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Page not found!'}), 404)

if __name__ == '__main__':
    kmeanService = kMeanCompute()
    app.run(debug=False)