# Project 1 Description
In this project, we are collecting data from multiple sources which is need for 
THE ANALYZE BOSTON OPEN DATA CHALLENGE:  
https://data.boston.gov/pages/opendatachallenge  
We are focus on the Problem Track 4, which is Identifying Fire Risks.  
In the project, we collected the data of Boston fire alarm boxes, fire department and fire hydrants.
We did a transformation to map the three datast into one dataset which can
reflect the overall fire facilities condition of Boston.
We also collect the data of History of Boston Fire Incident from 2017-01-01 to 2018-10-31. 
And corresponding Boston Weather condition. We did a transformation to aggregate the daily weather and
 number of incidents to see if they have certain relations.  
Finally, we collected the data of Building and Property Violations in Boston to see if such violations can
largely improve the risks of fire incidents. 


# Project 2 Description
In this project, we are exploring the correlation between the daily weather and the probability of 
a fire incident.  
In Project 1, we have already collected the daily weather information (including 
temperature, wind speed, precipitation, snow etc.) and the number of fire incidents of each day.
In Project 2, we implemented several machine learning classifiers using scikit-learn, including Logistic 
Regression, Support Vector Machine and AdaBoosting Classifier. We consider this problem as an optimization 
problem such that we use the daily weather information to train the model parameter so that the model can 
best predict the daily fire incident risk probabilities.  
Finally on average the Support Vector Machine Classifier model got the best result, with 50% accuracy to predict
 3 classes (where baseline is 33%). We then output the parameter of the SVC models and the weight of each features 
 after training.
For the statistical analysis part, we compute corelation coefficient and p-value between weather features and 
fire incidents to find which weather features influence the fire incidents the most. Finally, we find although
every single feature has less influence on fire incidents, when they are combined together in our SVM machine 
learning model, they as a union show a great influence on fire incidents.


# Dataset
* Boston Fire Incident
https://data.boston.gov/dataset/fire-incident-reporting
* Boston Code Enforcement - Building and Property Violations:
https://data.boston.gov/dataset/code-enforcement-building-and-property-violations
* Boston Fire Alarm Boxes
http://bostonopendata-boston.opendata.arcgis.com/datasets/fire-alarm-boxes
* Boston Fire Department
http://bostonopendata-boston.opendata.arcgis.com/datasets/fire-departments
* Boston Fire Hydrants
http://bostonopendata-boston.opendata.arcgis.com/datasets/fire-hydrants
* Boston Weather
https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00014739/detail

# Package Dependency
Please use following commands to install packages
  ```
    pip install -r requirements.txt
  ```
* Pandas: Provide interface to load the data.
* Numpy: Provide interface to calculate on the data.
* Scikit-learn: Provide interface for data preprocessing and machine learning

# Data Document
## boston_fire_facility_transformation.py
* facility_type: The type of the facility.<br>
One of [fire_alarm_boxes, fire_hydrants, fire_department]
* coordinates: Coordinates of the facility. <br>
Shape: [latitude, longitude]

## weather_fire_incident_transformation.py
* DATE: The date
* TMAX: The maximum temperature
* TMIN: The minimum temperature
* TAVG: The average temperature
* AWND: Average daily wind speed
* PRCP: Precipitation
* SNOW: Snowfall
* SNWD: Snow depth
* NINCIDENT: Number of fire incident happened
* NLOSS: Number of fire incident which causes money loss

## fire_building_transformation.py
* Street Name: The name of the street which happens fire incident or building violation
* Fire Incident Number: The number of fire incidents that happen on a street
* Building Violation Number: The number of building violations that happen on a street

## prediction_weather_incident.py
### All data below are of the test set which is shuffled for testing purpose
* DATE: The date
* LOW_PROB: The probability of occurring low level fire incidents that our machine learning model predicts
* MID_PROB: The probability of occurring medium level fire incidents that our machine learning model predicts
* HIGH_PROB: The probability of occurring high level fire incidents that our machine learning model predicts
* TRUE_LABEL: The actual label of fire incident of that day
* PRED_LABEL: The predict label by the model
* TRUE_VALUE: The true normalized incident number of the day
 

## statistical_analysis_weather_incident.py
* FEATURE: The name of weather feature
* CORR: The corelation coefficient between feature and fire incidents
* PVALUE: The p-value of feature and fire incidents