NOTE: Chart.js and flask need to be installed
project #3 is completely web visualization, a web app and an attempted web service with RESTful API. 
also included are a report and poster. All files are in the project_3 directory
Visualization 1: 
Name of file: webapp_scripts/templates/donut_trial.html
Tool used: Chart.js
Data used: Input data for project #1 from Weng Lab
Figure output: donut_bm5.png and donut_bm5_half.png

Visualization 2:
Name of file: webapp_scripts/templates/cluster_trial.html
Tool used: Chart.js
Data used: output data from project #1 clustering step
Figure output: clustering_chartjs.png

Visualization 3:
Name of file: webapp_scripts/templates/bubble_trial.html
Tool used: Chart.js
Data used: evaluation of results from ClusPro
Figure output: dockQvsclusrank_bubble.png

Web App: (maybe for bonus points?)
Name of files: flask_blog.py, webapp_sc/templates/layout.html,  webapp_scripts/templates/home.html,  webapp_scripts/templates/about.html, webapp_scripts/static/main.css
Tool used: FLASK
Data used: Visualizations 1, 2 and 3
Figure: webapp_screenshot.PNG

Attempted Web service with RESTful API
Visualization 3 with data from the webservice
Unable to fix bug to actually pull the data from the json file uploaded to the server via flask
Name of file: webapp_scripts/templates/bubble_ab_ag.html
Tool used: Flask and webapp_scripts
Data used: evaluation of results from CLusPro

How to run the program:
enter "flask run" in the webapp_scripts directory
use the port and http address that it gives + "/home" to reach the home page. 
Use the tabs on the side to view the different visuals and also the raw data used. 
