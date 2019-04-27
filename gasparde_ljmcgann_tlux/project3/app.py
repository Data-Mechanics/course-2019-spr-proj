from bson.json_util import dumps
import dml
from flask import Flask, request, render_template


app = Flask(__name__)

contributor = "gasparde_ljmcgann_tlux"
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(contributor, contributor)

################################################################################


neighborhoods = dumps(repo[contributor + ".Neighborhoods"].find())

@app.route('/index')
def hello():
    return render_template("index.html", neighborhoods=neighborhoods)

if __name__ == "__main__":
    app.run(port=5000, debug=True)
