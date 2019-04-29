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


@app.route('/')
@app.route('/index')
def hello():
    if request.args.get('kmeans') != None and request.args.get("neighborhood") != None:
        kmeans = compute_kmeans(request.args.get("neighborhood"), int(request.args.get('kmeans')), int(request.args.get("weight")))
        print(kmeans)
        ## return render_template("index.html", neighborhoods=neighborhoods, kmeans=kmeans, censusshape=censusshape)
        return dumps(kmeans);
    return render_template("index.html", neighborhoods=neighborhoods, censusshape=censusshape)


if __name__ == "__main__":
    app.run(port=5000, debug=True)
