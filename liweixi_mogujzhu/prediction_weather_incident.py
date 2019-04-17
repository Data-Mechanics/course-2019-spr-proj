import urllib.request
import json
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import numpy
from sklearn.preprocessing import KBinsDiscretizer, MinMaxScaler
from sklearn.utils import shuffle
from sklearn import linear_model
from sklearn import svm
from sklearn import ensemble
# This script use the data of daily weather (temperature, rain, wind etc.) and machine learning methods to predict
# the risk of daily fire incident
class prediction_weather_incident(dml.Algorithm):
    contributor = 'liweixi_mogujzhu'
    reads = ['liweixi_mogujzhu.weather_fire_incident_transformation']
    writes = ['liweixi_mogujzhu.prediction_weather_incident']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liweixi_mogujzhu', 'liweixi_mogujzhu')
        repo.dropCollection("prediction_weather_incident")
        repo.createCollection("prediction_weather_incident")
        # Create the training data and target
        data_name = 'liweixi_mogujzhu.weather_fire_incident_transformation'
        data = pd.DataFrame(list(repo[data_name].find()))
        print(data.shape)
        # If trial mode, use half of the data for training
        if trial:
            data = data[:data.shape[0]//2]
        data['LSCORE'] = data['NINCIDENT']
        data['TDIFF'] = data["TMAX"]-data["TMIN"]
        X = data[["TAVG","TDIFF","PRCP","SNOW","AWND"]]
        y = data["LSCORE"].astype(float)
        # Scale the data to range [0,1]
        min_max_scaler = MinMaxScaler()
        x_scaled = numpy.array(min_max_scaler.fit_transform(X.values))
        y_scaled_value = numpy.array(min_max_scaler.fit_transform(y.values.reshape(-1,1)))
        kbd = KBinsDiscretizer(n_bins=3,encode='ordinal',strategy='quantile')
        y_scaled = kbd.fit_transform(y_scaled_value)
        # Shuffle the data and create the training set and testing set
        X_shuffled, y_shuffled, y_shuffled_scaled_value = shuffle(x_scaled,y_scaled, y_scaled_value)
        X_train = X_shuffled[:int(X.shape[0]*0.8)]
        y_train = y_shuffled[:int(X.shape[0]*0.8)].ravel()
        y_train_value = y_shuffled_scaled_value[:int(X.shape[0]*0.8)].ravel()
        X_test = X_shuffled[int(X.shape[0]*0.8):]
        y_test = y_shuffled[int(X.shape[0]*0.8):].ravel()
        y_test_value = y_shuffled_scaled_value[int(X.shape[0]*0.8):].ravel()
        # Ser up the classifiers. We use 7 different classifier in this case
        classifiers = [
            linear_model.SGDClassifier(),
            linear_model.LogisticRegression(),
            svm.SVC(),
            ensemble.AdaBoostClassifier(),
            ensemble.BaggingClassifier(),
            ensemble.RandomForestClassifier(),
            ensemble.GradientBoostingClassifier()
            ]
        for item in classifiers:
            print(item)
            clf = item
            clf.fit(X_train, y_train)
            print("Training accuracy:",clf.score(X_train, y_train),"Base: 0.33")
            print("Testing accuracy:",clf.score(X_test,y_test),"Base: 0.33")


        insert_data = pd.DataFrame()
        model = svm.SVC(probability=True)
        model.fit(X_train, y_train)
        print("Final Classifer", model)
        pred = model.predict_proba(X_test)
        pred_label = model.predict(X_test)
        print("Accuracy",model.score(X_test, y_test))
        insert_data["LOW_PROB"]=pred[:,0]
        insert_data["MID_PROB"]=pred[:,1]
        insert_data["HIGH_PROB"]=pred[:,2]
        insert_data["PRED_LABEL"]=pd.DataFrame(pred_label).replace(0.0, "LOW").replace(1.0,"MID").replace(2.0,"HIGH")
        insert_data["TRUE_LABEL"]=pd.DataFrame(y_test).replace(0.0, "LOW").replace(1.0,"MID").replace(2.0,"HIGH")
        insert_data["TRUE_VALUE"]=y_test_value
        print(insert_data)
        repo['liweixi_mogujzhu.prediction_weather_incident'].insert_many(insert_data.to_dict('records'))
        repo['liweixi_mogujzhu.prediction_weather_incident'].metadata({'complete': True})
        print(repo['liweixi_mogujzhu.prediction_weather_incident'].metadata())
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liweixi_mogujzhu', 'liweixi_mogujzhu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.


        this_script = doc.agent('alg:liweixi_mogujzhu#prediction_weather_incident',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:liweixi_mogujzhu#weather_fire_incident_transformation',
                              {'prov:label': 'Boston Weather and Fire Incident', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_prediction_weather_incident = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_prediction_weather_incident, this_script)

        prediction_weather_incident = doc.entity('dat:liweixi_mogujzhu#prediction_weather_incident',
                          {prov.model.PROV_LABEL: 'Prediction Boston Weather and Fire Incident', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(prediction_weather_incident, this_script)
        doc.wasGeneratedBy(prediction_weather_incident, get_prediction_weather_incident, endTime)
        doc.wasDerivedFrom(prediction_weather_incident, resource,
                           get_prediction_weather_incident, get_prediction_weather_incident,
                           get_prediction_weather_incident)

        repo.logout()

        return doc



# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
if __name__ == "__main__":
    prediction_weather_incident.execute()
    # doc = prediction_weather_incident.provenance()
    # print(doc.get_provn())
    # print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof