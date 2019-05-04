# Project 3


## Team Members:

Roberto Alcalde Diego<br/>
Darren Hoffmann-Marks<br/> 
Alyssa Gladding<br/>



## The report is still being written by Roberto

I've made a pull request so that we can submit the code relating to our project. When Roberto finishes the report we will submit another pull request containing the report as the Readme.

## How to run the code within our project

All the files that are not within the visualizations folder are scripts that need to be run using execute.py to set up the collections that our web application within the visualizations folder will use. So follow these steps:

1) run: python execute.py darren68_gladding_ralcalde (with trial mode on or off)
2) Go into the visualizations folder and run the command: python app.py

Those are all the commands needed to use our web visualization components properly.

Some things to note: Since we're working with a lot of data some of the scripts may take a very long time to finish execution. In the script ConvertToLatLng the mongo cursor will most likely timeout while working with the collection and raise an exception. To get around this, use "mongod --setParameter cursorTimeoutMillis=3600000" to prevent the cursor from timing out before the script finishes execution.



To run the scripts you will need the following libraries:
branca==0.3.1
certifi==2019.3.9
chardet==3.0.4
Click==7.0
DateTime==4.3
decorator==4.4.0
definitions==0.2.0
dml==0.0.16.0
Flask==1.0.2
folium==0.8.3
idna==2.8
isodate==0.6.0
itsdangerous==1.1.0
Jinja2==2.10.1
lxml==4.3.3
MarkupSafe==1.1.1
networkx==2.3
nltk==3.4.1
numpy==1.16.3
protoql==0.0.3.0
prov==1.5.3
pymongo==3.8.0
pyparsing==2.4.0
pyproj==2.1.3
python-dateutil==2.8.0
pytz==2019.1
PyYAML==5.1
rdflib==4.2.2
requests==2.21.0
six==1.12.0
urllib3==1.24.2
Werkzeug==0.15.2
zope.interface==4.6.0



