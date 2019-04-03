# Study on Optimal Location and Amount for Placement of New Bike Rental Stations in 5 Major US Cities (Project 2)

## Summary

We planned to study both the optimal location and amount for addition of new bike rental stations to maximize profits for rental
bike companies in different cities.  To do this, we first used trip data for all 5 cities, as well as population, to determine a 
ratio for how much each city used the system relative to its population.  This information gave us an idea of which city would be
best to add new bike stations to.  To determine the amount of stations to add, we used a linear regression (max constraint of 10 
stations that we could afford) using each cities' station amount, total bike time, and population.  Viewing the data in scatter 
plot form, it was clear that a linear fit was the best option, as opposed to quadratic, cubic, etc.  From this regression, it was 
clear that adding additional stations could only increase profits.  Knowing k=max_constraint=10, we inputted it into a K-means 
algorithm as k (once for each city), with the points being represented by the latitude/longitude pairs for each station.  This 
gave us k cluster centers, which we claim are the optimal locations to add k new stations for each city.  We chose the city with
the highest ratio of bike time to population as the location to build our new stations.  Note: as each city's data ranged in the 
length of time it covered, we picked an arbitrary recent month to collect data for (09-2018)

## Data Sets
- Boston Bike Data			- Boston Station Data
- New York City Bike Data	- New York City Station Data
- Washington Bike Data		- Washington Station Data
- Chicago Bike Data			- Chicago Station Data
- San Francisco Bike Data	- San Francisco Station Data
- Census Data

While the station and bike data are similar to that of each city, they had differences in syntax and download format.

## Transformations
- Sum Aggregation for each City's Bike Data
- Union of the 5 Sum Aggregations
- Join of the bike data aggregation and census data for each city

## Algorithms
- Optimization: Linear Regression
- Statistical Analysis: K-means

# Comparing Recreational Bike Use by Population for 4 Major Cities (Project 1)

## Summary

Our goal was to determine how much time people in different cities spend biking for pleasure.  We decided to use publicly 
available data sets for major bike rental companies for 4 cities to determine how much time was spent renting bikes.  We chose
Chicago, NYC, Washington DC, and Chattanooga (as a control group for a smaller city), and picked a recent month with data
available for all 4 cities (2018-09).  As the 4 cities had moderately different schemas and range of information, we had to
parse the data using selections and projections to add them into our DB in a format of 4 tables (ObjectID, Duration, Month).
We added a field for month so that we could easily select the data pertinent to our required window.  We then aggregated the
total time spent on bikes for these 4 datasets and combined them to form a new data set of the form (ObjectID, City,
TotalDuration).  We treated the aggregations as 1 unique transformation and the union as a second.  We then joined this
with our 5th dataset, which included census data on all American cities, to create a final data set of the form 
(ObjectID, City, TotalDuration, Population) to give us an idea of the ratio between time spent on bikes vs population to 
determine which cities utilized rental bikes more.

## Data Sets
- Chicago Bike Data
- New York City Bike Data
- Washington Bike Data
- Chattanooga Bike Data
- Census Data

While the first 4 data sets contain similar information, they were made available in various methods including zipped CSV's
and JSON.  They also had varying schemas, which necesitated different code to parse each.

## Transformations
- Sum Aggregation for each City's Bike Data
- Union of the 4 Sum Aggregations
- Join of the bike data aggregation and census data for each city