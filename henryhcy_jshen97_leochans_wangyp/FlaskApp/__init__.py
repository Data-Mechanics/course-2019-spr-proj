import dml

from flask import Flask
from FlaskApp.config import Config
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from bson.json_util import dumps
import dml
import datetime
import geopy.distance
import json
import prov.model
import pprint
import random
import uuid

app = Flask(__name__)
app.config.from_object(Config)
db = SQLAlchemy(app)
migrate = Migrate(app, db)

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(Config.MONGO_USERNAME, Config.MONGO_PASSWORD)

from FlaskApp import routes, models
