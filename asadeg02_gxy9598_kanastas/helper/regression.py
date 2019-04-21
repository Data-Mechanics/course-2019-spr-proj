import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
import time
from sklearn import linear_model
from sklearn import preprocessing
import numpy as np
import matplotlib.pyplot as plt


class regression():     
    

    @staticmethod
    def getRegressionResults(x_train, y_train, img_name, fig_number):

        start_time = time.time()        

        x_train = np.array(x_train)
        y_train = np.array(y_train)

        min_max_scaler = preprocessing.MinMaxScaler()

        #Normalizing training data using min max scaler 
        x_train = min_max_scaler.fit_transform(x_train.reshape(-1,1))
        y_train = min_max_scaler.fit_transform(y_train.reshape(-1,1))        

        regr = linear_model.LinearRegression()
        regr.fit(x_train,y_train)

        results = {'coefficients': regr.coef_[0][0], 
                   'mean_square_errors': np.mean((regr.predict(x_train) - y_train) ** 2),
                   'score': regr.score(x_train, y_train)}    

        
        elapsed_time = time.time() - start_time
        

        #Generate a pdf file for regression plot
        plt.figure(fig_number)
        plt.scatter(x_train, y_train,color='red')
        plt.plot(x_train ,regr.predict(x_train), color='blue',linewidth=3)
        plt.xticks(())
        plt.yticks(())
        plt.savefig(img_name + '.pdf')
        
        return results  
    
