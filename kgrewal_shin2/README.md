Kerin Grewal and Stephanie Shin 
github link: https://github.com/keringrewal/course-2019-spr-proj/tree/master/kgrewal_shin2


<b> INSTRUCTIONS: </b>

Run execute.py to run all data collection, transformations, and provenances, 
by running 'python3.7 execute.py kgrewal_shin2' in the root project folder. 

<h4><b>Must run app.py in Project3 folder to view our final webpage.</b></h4>
Make sure that you have ran execute.py as stated in previous steps, and that mongodb is running (which it should be when running execute). 
In order to run the web application, you need to retrieve a google maps api key and insert it into config.py. 
A key can be retrieved here: https://developers.google.com/maps/documentation/javascript/get-api-key
You may run the application by running 'flask run' in the kgrewal_shin2/Project3 folder. 

 
<b> LIBRARIES/dependencies: </b>
In data insertions and transformations: 
urllib.request, requests, json, dml, prov.model, datetime, uuid

For statistics.py: 
dml, prov.model, datetime, uuid, scipy.stats

For kmeans.py: 
dml, prov.model, datetime, uuid, scipy.cluster.vq

Project3: 
dml, flask, flask-googlemaps (https://github.com/rochacbruno/Flask-GoogleMaps), google maps api key from: https://developers.google.com/maps/documentation/javascript/get-api-key



<b> DATASETS USED: </b>

1. Boston Street Names 
This data was collected from last semester's project as a CSV and uploaded to datamechanics.io as a .json file. It is a list of all the street names, their genders, and their zipcodes.
This is the base of our information (all of the street names in Boston). 

2. Landmarks 
This data was collected from Boston Landmarks Commission (BLC) Landmarks- http://bostonopendata-boston.opendata.arcgis.com/datasets/7a7aca614ad740e99b060e0ee787a228_3. 
This gives us names of landmarks as well as their locations and neighborhoods. 

3. Neighborhoods 
This data was collected from https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/boston.geojson. 
It includes the geographic boundaries of each neighborhood in Boston. 

4. Public Schools 
This data was collected from https://data.boston.gov/dataset/public-schools/resource/6c48e501-3dba-44f3-912f-8a5f309d5df4. 
It includes the locations of public schools in Boston. 

5. Uber data 
This data was collected from Uber including all the information of Uber rides originating at the Boston Common. 
It includes where they ended their Uber rides and the mean time to get there. 

6. Neighborhood Zip Codes
From http://archive.boston.com/news/local/articles/2007/04/15/sixfigurezipcodes_city/ we took the table and changed
it into a CSV. This tells us which boston zip codes belong to which neighborhood.


<b> FILES: </b>

street_name_cleaning.py - ran once in order to create json

uber_data_cleaning.py - ran once in order to create json 

test.py - test file for our use previously 

transformations.py - transformation functions as outlined in class 

neighborhood_zipcodes.py - ran once in order to create json



<b> TRANSFORMATIONS: </b>

transformation1.py - finding the difference between street names, and streets with landmarks on them, selecting only those that are not female street names

transformation2.py - finding the streets that do not have public schools on them by scanning through the public school addresses and the streets

transformation3.py - locating areas of boston where ubers travel most frequently 

transformation4.py - combines the streets without landmarks and the streets without schools to find the unclaimed streets

transformation5.py - combines the unclaimed streets with the neighborhood zip codes to assign every street to a neighborhood based of it's zipcode 


<br></br>
<b>PROJECT 2: </b> 

kmeans.py - We performed kmeans clustering on the data to find the areas where unclaimed streets are common. We then did a count 
to find how many streets were exist in each cluster. We plan to map this data and use it to spread out the locations where we 
suggest street names should be changed.

statistics.py - We also took the street lengths and calculated the correlation coefficients to see if 
there is a connection between the lengths of our unclaimed streets. We decided that longer
roads are more likely to be "major roads" and this would tell us if we are selecting 
mostly major roads or mostly less-significant roads. We found no correlation between length and gender
and therefore we have chosen a balance between long and short streets which will mean the streets
we have narrowed our list down to will have plenty of significant suggestions.


<br></br>
<b> PROJECT 3: </b>
Our end result is an interactive web application where you are able to first select one of all the neighborhoods in Boston
and see a list of the streets that are good candidates for renaming within each neighborhood. Then, you are able to locate 
them and view them on the map to visualize and assess the possibility of this street being renamed. 
See instructions at the beginning of readme to run this application. 


