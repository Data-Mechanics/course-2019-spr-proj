<h1>CS 504 Project #1 & Project #2</h1>

<h2>Inspiration</h2>

Bluebikes is a public bike share system in Boston, Brookline, Cambridge and Somerville. I ride their bike to BU everyday and it's really convenient. However, sometimes I find it hard to find a bike or a dock to park my bike because the bike stations are not located very reasonably. For example, the Bluebike station at BU campus has only one bike station with 10 docks, so when I ride to campus, I sometimes have to look for another bike station nearby because that one is already full. That actually made me late for CS504 class for twice this semester! So I am thinking, if Bluebike can learn more about their bike-using situation and set their bike stations more properly, it will be very nice for people who use their bike service like me. Thus in this project, I will try to find out which places are good choices to place a Bluebike station and how many docks each station should have.
The first step to address the problem I chose is to look for some datasets that are useful for this project. I picked 5 datasets as beginning and I'll introduce them below. The second move is to use these datasets to get some information I need, that requires me to perform some data transformations. After project 1, I’ve analyzed the Bluebike trip data and other information and found out how many bike stations are close to each college and university. I also got the frequency of Bluebike trip around each college. Then in project 2, I did some statistical analysis to see if college students are the main customer of the Bluebike service and if it’s practicable to place more bike stations for them. I also got some statistics about the relation between a college’s student number, bike station number and trip number. Then I set some constraints and solve them as satisfaction and optimization problems to find out what is the best way to place new bike stations nearby colleges and universities. After that I got some new relation between these information and compared them with the old one.

<h2>Datasets</h2>
There are five datasets at this time, each has a retrieve algorithm in separated python files:

Subway stop locations:<br>
http://datamechanics.io/data/yufeng72/Subway_Stops.json

Bus stop locations:<br>
http://datamechanics.io/data/yufeng72/Bus_Stops.csv

College and university locations:<br>
http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.csv

Bluebike station locations:<br>
https://s3.amazonaws.com/hubway-data/Hubway_Stations_as_of_July_2017.csv

Bluebike trip data 2018.9:<br>
http://datamechanics.io/data/yufeng72/Bluebikes_Tripdata_201809.csv

<h2>Transformations</h2>

Implemented 3 transformations, all three using selection, projection and combination:

Transformation 1: find bus stops, colleges and universities with valid latitude and longitude in Boston to find possible places for placing bike stations.

Transformation 2: calculate the distance between every Bluebike station and every college & university, then count the number of the Bluebike stations near these colleges and universities.

Transformation 3: for each college and university, find out how many Bluebike trips take them as destination (in a month) to see if it is popular.

<h2> Constraint satisfaction and Optimization </h2>

