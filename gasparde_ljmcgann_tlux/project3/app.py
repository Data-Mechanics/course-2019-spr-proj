from bson.json_util import dumps
import dml
from flask import Flask, request, render_template
from kmeans import compute_kmeans


app = Flask(__name__)

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
        name = str(request.form["neighborhood"])
        kmeans = int(request.form['kmeans'])
        weight = int(request.form["weight"])
        kmeans = dumps(compute_kmeans(name, kmeans, weight))
        print(kmeans)
        return render_template('index.html', neighborhoods=neighborhoods, kmeans=kmeans, name123=name)

    return render_template("index.html", neighborhoods=neighborhoods)


if __name__ == "__main__":
    app.run(port=5000, debug=True)
