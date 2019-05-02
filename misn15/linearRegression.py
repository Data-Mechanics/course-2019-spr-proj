import dml
import prov.model
import datetime
import uuid
import pandas as pd
from sklearn.metrics import mean_squared_error
import numpy as np
import statsmodels.api as sm

class linearRegression(dml.Algorithm):
    contributor = 'misn15'
    reads = ['misn15.crime_health_waste_space']
    writes = ['misn15.reg_results', 'misn15.log_results']

    @staticmethod
    def execute(trial = False):
        '''gradient descent and linear regression'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        # Gradient Descent
        def cost(theta, X, y):
            m = len(y)
            y_hat = X.dot(theta)
            cost = (1 / 2 * m) * np.sum(np.square(y_hat - y))
            return cost

        def gradient_descent(X, y, theta, lr, n_iter):
            m = len(y)
            cost_history = np.zeros(n_iter)
            theta_history = np.zeros((n_iter, theta.shape[0]))
            for i in range(n_iter):
                y_hat = np.dot(X, theta)
                theta -= (1 / m) * lr * (X.T.dot((y_hat - y)))
                theta_history[i] = theta.T
                cost_history[i] = cost(theta, X, y)
            return theta, cost_history, theta_history

        def predict(X, theta):
            return np.dot(X, theta)

        # Define parameters for gradient descent
        lr = 0.000000001
        n_iter = 100

        # randomly initialize theta
        theta = np.random.randn(4, 1)

        # get data into proper format
        crime_health = list(repo['misn15.crime_health_waste_space'].find())
        crime_health = pd.DataFrame(crime_health)
        health_filter = crime_health[['open space', 'waste', 'income', 'crime', 'cancer occurrences', 'asthma occurrences',
                                     'COPD occurrences', 'total occurrences']]
        final_matrix = np.matrix(health_filter, dtype='float64')

        # choose regressors and dependent variable
        X = final_matrix[:,0:3]
        ones = np.ones([X.shape[0], 1])
        X = np.concatenate((ones, X), axis=1)
        y = final_matrix[:, -1]
        y.shape = (len(final_matrix), 1)

        theta, cost_history, theta_history = gradient_descent(X, y, theta, lr, n_iter)

        # MSE using gradient descent; it is much greater than simple linear regression
        y_hat = predict(X, theta)
        mse = mean_squared_error(y, y_hat)

        #print('Theta0: {:0.3f}\nTheta1: {:0.3f}'.format(theta[0][0], theta[1][0]))
        #print('MSE: {:0.3f}'.format(mse))

        # compare to regression results from stats library
        X = sm.add_constant(X)
        reg = sm.OLS(y, X).fit(cov_type='HC1')
        predictions = reg.predict(X)
        mse_2 = mean_squared_error(y, predictions)

        # get results
        coef = np.array(reg.params).tolist()
        std_err = np.array(reg.bse).tolist()
        p_values = np.array(reg.pvalues).tolist()
        t_stat = np.array(reg.tvalues).tolist()
        results = {'reg_1': {'coefficients': coef, 'standard error': std_err, 'p-values': p_values, 't-statistics': t_stat}}
        #print(reg.summary())

        # regress crime on open spaces, waste, and income
        X = final_matrix[:, 0:2]
        ones = np.ones([X.shape[0], 1])
        X = np.concatenate((ones, X), axis=1)
        y = final_matrix[:, 3]
        y.shape = (len(final_matrix), 1)
        X = sm.add_constant(X)

        # get output of regression
        reg = sm.OLS(y, X).fit(cov_type='HC1')
        coef = np.array(reg.params).tolist()
        std_err = np.array(reg.bse).tolist()
        p_values = np.array(reg.pvalues).tolist()
        t_stat = np.array(reg.tvalues).tolist()
        results2 = {'reg_2': {'coefficients': coef, 'standard error': std_err, 'p-values': p_values, 't-statistics': t_stat}}

        repo.dropCollection("misn15.reg_results")
        repo.createCollection("misn15.reg_results")
        repo['misn15.reg_results'].insert_one(results)
        repo['misn15.reg_results'].insert_one(results2)
        repo['misn15.reg_results'].metadata({'complete':True})
        print(repo['misn15.reg_results'].metadata())

        # Logistic Regression
        # get data
        crime_health = crime_health[['open space', 'waste', 'income', 'crime', 'probability of disease']]
        final_matrix = np.matrix(crime_health, dtype='float64')
        y = final_matrix[:, -1]

        # construct model
        X = sm.add_constant(X)
        logreg = sm.Logit(y, X).fit()
        #logreg.summary()

        # get odds ratio
        coef = np.array(np.exp(logreg.params)).tolist()
        conf = np.exp(logreg.conf_int())
        std_err = np.array(logreg.bse).tolist()
        p_values = np.array(logreg.pvalues).tolist()
        t_stat = np.array(logreg.tvalues).tolist()

        results = {'coefficients': coef, 'standard error': std_err, 'p-values': p_values, 't-statistics': t_stat}

        repo.dropCollection("misn15.log_results")
        repo.createCollection("misn15.log_results")
        repo['misn15.log_results'].insert_one(results)

        repo['misn15.log_results'].metadata({'complete':True})
        print(repo['misn15.log_results'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        this_script = doc.agent('alg:misn15#linearRegression', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:misn15#crime_health_waste_space', {'prov:label':'Boston Crime, Health, Waste and Open Space Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_reg = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_reg, this_script)
        doc.usage(get_reg, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                   }
                  )
        get_logreg = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_logreg, this_script)
        doc.usage(get_logreg, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'
                   }
                  )

        reg_results = doc.entity('dat:misn15#reg_results', {prov.model.PROV_LABEL:'Linear Regression Results', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(reg_results, this_script)
        doc.wasGeneratedBy(reg_results, get_reg, endTime)
        doc.wasDerivedFrom(reg_results, resource, get_reg, get_reg, get_reg)

        log_results = doc.entity('dat:misn15#log_results', {prov.model.PROV_LABEL: 'Logistic Regression Results', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(log_results, this_script)
        doc.wasGeneratedBy(log_results, get_logreg, endTime)
        doc.wasDerivedFrom(log_results, resource, get_logreg, get_logreg, get_logreg)

        return doc

linearRegression.execute()
##doc = linearRegression.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
