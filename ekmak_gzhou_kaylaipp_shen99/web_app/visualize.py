import json
import jsonschema
from flask import Flask
from flask.ext.httpauth import HTTPBasicAuth

app = Flask(__name__)
auth = HTTPBasicAuth()





if __name__ == '__main__':
    app.run(debug=True)

    
