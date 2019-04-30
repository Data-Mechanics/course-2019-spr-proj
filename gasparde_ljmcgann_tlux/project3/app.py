from bson.json_util import dumps
import dml
from flask import Flask, request, render_template
from kmeans import compute_kmeans
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

contributor = "gasparde_ljmcgann_tlux"
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(contributor, contributor)

################################################################################


neighborhoods = dumps(repo[contributor + ".Neighborhoods"].find())
censusshape = dumps(repo[contributor + ".CensusTractShape"].find())


@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
def hello():
    if request.method == 'POST':
        print("hello a post happened")
        req_data = request.get_json()
        print(req_data)
        name = request.form["neighborhood"]
        kmeans = int(request.form['kmeans'])
        weight = int(request.form["weight"])
        kmeans = dumps(compute_kmeans(name, kmeans, weight))
        response = dumps(kmeans),
        return render_template('index.html', neighborhoods=neighborhoods, kmeans=kmeans, name=str(name))

    return render_template("index.html", neighborhoods=neighborhoods)


if __name__ == "__main__":
    app.run(port=5000, debug=True)
