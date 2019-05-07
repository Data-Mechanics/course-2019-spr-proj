# course-2019-spr-proj README
Chengyu Deng (cdeng@bu.edu)
Project 3 Updated README file.
There are three updates for project 3

## Two new features for visualizations
Using [Plotly.js](https://plot.ly/javascript/) and [Flask](http://flask.pocoo.org/), I extended my project with two new features. Both of these two features are interactive web-based visualizations that can be displayed in a standard web browser. 

First, the processed datasets can be visualized based on charts and maps. The charts will indicate the most 5 popular incoming/outgoing stations. The maps will visualize all station docks based on the coordinates. Feel free to toggle around with the interactive maps. The sample screenshots of the charts and maps are available in the final report. 

The second feature visualizes the results of the algorithm and statistical analysis. A 3-D scatter graph and a 3-D surface graph will be available after the optimization algorithm and statistical analysis are finished. Feel free to toggle around with the interactive 3-D graphs. The sample screenshots of the graphs are available in the final report. 

Please **Notice** that the way to see those visualizations will be summarized in "How to run the project code" section. 

## Completion of final report
I finished the final report which describes everything of this project. The report is in pdf format and it is called `CS504_Final_Report.pdf`. 

## Completion of poster
I attended the poster session and talked with Professor Lapets about my project. The poster name is `CS504_Project_Poster.pdf`. 

## How to run the project code
### Dependencies
+ Python 3.6 or Python 3.7 
+ MongoDB
+ Flask: [link](http://flask.pocoo.org/)
+ `prov` package: [link](https://pypi.org/project/prov/)
+ `dml` package: [link](https://pypi.org/project/dml/)

### Normal mode
Follow the project 2 instruction to set up the MongoDB environment and run the provided code such as **reset.js** and **setup.js**. Then run the following terminal code:
```
python execute.py cdeng
```
After the optimization algorithm and the statistical analysis are finished. The flask will create a local address for the brower to show the visualization. Typically, you will see the following in your terminal:

```sh
Finish data provenance here...
 * Running on http://XXX.X.X.X:XXXX/ (Press CTRL+C to quit)
```
Then copy/paste the address (`http://XXX.X.X.X:XXXX/`) into a brower and then add:
+ `chart` to see the chart graph. (Full address is: `http://XXX.X.X.X:XXXX/chart`)
+ `maps` to see the interactive maps. (Full address is: `http://XXX.X.X.X:XXXX/maps`)
+ `policy` to see the interactive 3-D graphs. (Full address is: `http://XXX.X.X.X:XXXX/policy`)

After finishing reviewing visualizations, enter `CTRL+C` to continue the program. The program will keep going until the end of the program. 

Please notice that this program in normal mode will run about 4-5 hours. In the normal mode, all bike returns and rentals are unstable and the program will consider all the possible situations so that the program runs slow. 

To check the correctness of the code, there is a trial mode available. 
### Trial Mode
```
python execute.py cdeng --trial
```
In this mode, the program assumes that the incoming bikes and outgoing bikes are stable for each time. Although it is not natural in the real situation, in this way, the program will run fast so that the correctness of the code can be quickly inspected. The program will run about 2-3 minutes in trial mode (I realize it might not quick enough still, but this is the best I can do for this algorithm if we still want to fully run the algorithm). I provide a text file called ```Project3_sampleOutput.txt``` to show the trial mode result for any reference. 

The visualization showing process remains the same as the normal mode. 

**If there is any problem when running the program, please don't hesitate to contact me. My email is cdeng@bu.edu**

__________________________________________________________________________________________

Project 2 Updated README file. 
This project is finished by Chengyu Deng(U68004039, cdeng@bu.edu). There are no other collaborators. 

## General Introduction of the Project
I continued my original task from project 1 which is about bike sharing analysis in the Boston area. In this project, I used several datasets, an optimization method called **policy iteration algorithm**, and statistical analysis to solve the most two popular bike sharing stations bike allocation problem. 

## Datasets
I used three datasets in project 2, which are: 
- **Hubway_Stations_as_of_July_2017.csv**
This dataset indicates the total bike sharing locations mainly in Boston and Cambridge areas (some locations are in Somerville and Brookline). It contains important information such as the number of docks at that station, Latitude/Longitude data, street name, etc. 
- **201801_hubway_tripdata.csv**
This dataset maintains all the bike trip information in January 2018. It contains data such as trip length, start/end station, and its street name, user type, user gender, etc. 
- **201802_hubway_tripdata.csv**
This dataset contains all the bike trip information in February 2018. It has the same schema as the previous dataset. 

These datasets will be enough for us to find out the most popular bike stations from January 2018 to February 2018. In the ```Project2_load_data.py``` file, the program will load those three datasets into the MongoDB and then ```Project2_most_popular_stations.py``` file will use aggregation to find out the total number of trips for each station and then sort them in descending order. In this way, we can find the most two popular bike stations. Once we get the most two popular bike stations' names, numbers of docks, average incoming bike rates, average outgoing bike rates, we can continue to the optimization part of this project. 

## Optimization
### Assumptions
The optimization problem has the following assumptions. Since those two bike stations are really popular, we want to maximize the usage of bikes for customers in those two stations. Sometimes, a station may be too popular in a specific time (for example, rush hour) so that the bikes are gone too quickly. Also, a station also can be too popular so that there are lots of bike returns and docks fill up too quickly. So to solve this problem, we want to hire a team to move bikes from one station to the other. For moving each bike, there is a cost $$c$$ and we will get a reward $$r$$ when a user rents a bike in either of these two stations. Each time, the team only move at most $$n$$ bikes (I didn't find any dataset which provides these values so I determined those values by my own). The team will execute the moving process every two hours a day from 6 AM to 12 AM. The average incoming rate and outgoing rate are calculated from the datasets. Also, the customers return/rent bikes in a Poisson process. The reason I choose the Poisson process is that the Poisson process is one closest to the real world situation. 

### Policy Iteration Algorithm
We can formalize the bike allocation problem in the following way: for each time t, there are bikes getting rent and getting returned and we will get a reward for those renting bikes (total reward at time t is r times the number of bikes rented). For each time, we want to find a strategy s in [-n, n] (s is an integer, assume n means moving n bikes from station 1 to station 2 and $$-n$$ means moving $$n$$ bikes from station 2 to station 1) such that the we can maximize the future renting bikes from t, t+1, t+2, t+3, .... In this way, the utilization of the bikes for both stations is maximized. The metric that we want to maximize is the future available for renting bikes.

To solve this problem I used Policy Iteration algorithm. Policy Iteration is a dynamic programming related optimization algorithm based on the Markov Decision Process(MDP). Here are some helpful links:
- https://www.cs.cmu.edu/afs/cs/project/jair/pub/volume4/kaelbling96a-html/node20.html
- https://en.wikipedia.org/wiki/Markov_decision_process

It is an iterative algorithm to approach the optimal solution. Briefly, throughout each iteration, the evaluation for each strategy gets more accurate, resulting in the change of strategy. When there is no change in the evaluation and strategy. The algorithm is then converged and we will obtain the optimal solution for this problem. 

## Statistical Analysis
In the statistical analysis part of this project, I calculated three different correlation coefficients and the p-values, which are:
- bike stations' # of docks and outgoing trip number
- bike stations' # of docks and incoming trip number
- the outgoing number and incoming trip number

The results are recorded in the MongoDB. The reason to find the correlation coefficients and p-values of those dimensions is that I want to make sure the relationships between docks and trips this may relate to the optimization problem of allocating bikes.  

## Python files
There are 4 python files in this project which are:
- ```Project2_load_data.py```
- ```Project2_most_popular_stations.py```
- ```Project2_optimal_allocation.py```
- ```Project2_data_analysis.py```

All of the files contain provenance recording.

## How to run the project code
### Normal mode
Follow the project 1 instruction to set up the MongoDB environment and run the provided code such as **reset.js** and **setup.js**. Then run the following terminal code:
```
python execute.py cdeng
```
Please notice that this program in normal mode will run about 4-5 hours. In the normal mode, all bike returns and rentals are unstable and the program will consider all the possible situations so that the program runs slow. 

To check the correctness of the code, there is a trial mode available. 
### Trial Mode
```
python execute.py cdeng --trial
```
In this mode, the program assumes that the incoming bikes and outgoing bikes are stable for each time. Although it is not natural in the real situation, in this way, the program will run fast so that the correctness of the code can be quickly inspected. The program will run about 2-3 minutes in trial mode (I realize it might not quick enough still, but this is the best I can do for this algorithm if we still want to fully run the algorithm). I provide a text file called ```Project2_sampleOutput.txt``` to show the trial mode result for any reference. 







