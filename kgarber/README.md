# Commuting in Boston

**NOTE: This is the README for the semester's repository, it is NOT the report for project 3. That's at `/project-3/README.md`.**

# Project 1

## Why

Commuting in Boston hits close to home for me - I commute every day from home to school. I feel bad for driving every day but from where I live there's no better alternative. I would rather take public transit or a carbon neutral alternative like biking, but it's too inconvenient. Factors like traffic, the weather, and distances play a role.

In this project I try to derive some new knowledge about commuters using boston data. This includes data about universities in Boston, bike sharing programs, and the trains. I'll go into more detail about the datasets I derive below.

In this project I'll investigate a few questions, including:

* Students take bikeshare bikes to school - where are they coming from?
* Weather impacts bicycling in Boston, but to what extent?
* Does the weather similarly impact the trains in Boston?

## Data Portals and Datasets

Here are the portals and datasets I used

* Data.boston.gov
	* Universities in Boston
* BlueBike
	* System data (all 1.7 million rides of 2018)
* NOAA
	* Boston Weather 2018
* MBTA
	* Train routes
	* Past live alert notices

## (Non-trivial) Derived Datasets

I have found three important datasets.

First, the rides_and_weather dataset (db.kgarber.rides_and_weather). This dataset uses the weather dataset and the BlueBike dataset to quantify what we might think is clear - the colder it is outside, the less people use bikesharing and the shorter their rides are. The dataset is 10 degree buckets (0-10, 10-20, etc), and shows the average number of rides in the day as well as the average duration of a ride.

Second is the university_bluebike_stations dataset (db.kgarber.university_bluebike_stations). It combines the BlueBike dataset as well as the university dataset to tell us an important piece of information: for each university, from where are people commuting to it? For BU for example, the most popular station to leave in order to arrive at BU is not surprisingly Packard's Corner. 

The dataset includes information for all universities of greater than 500 students, shows which BlueBike stations are near these unis (using a mongo geographical search), and shows from where people ride to the university by doing a search for 7AM - 11AM weekday rides (most popular departure stations).

The third and final derived dataset, alerts_and_weather, takes a look at the Green, Red, Orange, and Blue line in Boston, and tells us how many alerts there are for Boston Trains on an average day in different temperatures. I was curious to see if particularly cold weather caused lots of delays. This dataset uses the alerts, routes, and weather datesets.

## Files

All python files with "download_" prepended are responsible for downloading the data from the various data portals, the internet, and datamechanics.io. 

`alerts_and_weather.py` generates the alerts_and_weather dataset. `rides_per_day.py` creates an intermediary dataset of the number of BlueBike rides per day, it is not one of my final three datasets. `rides_and_weather.py` creates the rides_and_weather dataset. `university_bluebike_stations.py` creates the dataset of bluebike stations near universities as well as from where people ride to them.

## Running

If you configure your `auth.json` as described below, simply writing `python execute.py kgarber` from the root directory of the repository should run all of the scripts in the required order, and generate the provenance document.

## auth.json

The auth.json file should have a key to access the MBTA data. A free and unprotected key is listed below because the MBTA provides it as a developer key which may be removed at any time but works as of writing this.

```
{
	"services": {
		"mbta": {
			"service": "MBTA",
			"dev-API-key": "wX9NwuHnZU2ToO7GmGR9uw"
		}
	}
}
```

# Project 2

## Why 

To help me answer questions about the good, the bad, and the ugly of commuting in Boston, I'll analyze the data I collected in Project 1 to draw some conclusions about our transportation. I use that data to answer a few questions

* Surely the amount of cyclists goes down in the Winter, but by what extent? Is there a strong correlation between the weather and the number of cyclists? Can we model this predictively with a regression?
* Do the MBTA trains run more or less reliably depending on the weather? I suspect extreme cold can slow our trains down - is that the case? Could we model it so that we can predict delays depending on the weather?
* How can Bluebike place stations in a way to expand their network and provide better service to users?

## Process

To see the relationship between cycling and the weather, we can calculate the correlation between 1) the number of cyclists on a given day, and 2) the average temperative on that day. I use SciPy to calculate a correlation coefficient and p-value between these two datasets, and I have 365 data points (one for each day of 2018). I use Numpy to do a linear regression and calculate a line of best fit. I do this against both 1) the average number of trips in a day, and 2) the average trip duration for that day.

I repeat the above process with my dataset of weather and MBTA train alerts. The weather dataset and time period (all of 2018) are the same.

I ran the current bluebike stations through the z3-solver to optimize the placement of a potential new stations in the bluebike network. I wanted to maximize the distance of the new station to other stations in order to expand the network, but keep the station within a certain distance of several other stations, so that the network isn't too fragmented. This involved a maximization optimization along with constraints on how far a station gets placed from other stations, and a constraint so that it doesn't get placed in the ocean, since Boston is on the water.

The optimization problem also required that I implement a new "distance" metric. Traditionally euclidean distance requires squares and square roots, but linear programming optimizers can't deal with non-linear objectives. I created a z3 optimizer-friendly absolute value function and used that for a distance-like metric.

I also used k-means to reduce the number of stations to simplify the objective. There are over 300 stations which meant that the objective function had over 600 variables (longitude and latitude), which really slowed down my solution. By using k-means I could preserve the shape and distribution of the data while reducing the number of points to 50, resulting in quicker optimizer outputs.

## Conclusions

Unsurprisingly, there's a correlation between temperature and average number of daily trips, as well as between temperature and average trip duration. We conclude that colder weather leads to, on average, fewer and shorter rides.

The correlation coefficient between temperature and ride count is 0.83 with a p-value of 0.3e-94 (extremely close to zero). Between temperature and ride duration there's a correlation of 0.42 with a p-value of 0.4e-17 (also almost zero).

The correlation coefficient for MBTA alerts and temperative is -0.14, with a p-value of 0.16. This is as we suspected - the warmer it is, the fewer alerts there are. The p-value isn't significant, though.

We also obtained a line of best fit for all above datasets. It'll be included in Project 3.

The new bluebike station optimization answered that the new station should go in Cambridge, which makes sense given that Cambridge has lots of close-together stations which end abruptly near Medford. Placing a station just beyond this artificial bound of the network gives us a good result - more users can now access the bikes, and the station isn't too removed from the remainder of the network.

## File Changes (and Project Requirements)

`ride_weather_stats.py` is the algorithm to get the correlation coefficient, p-value, and best fit line for bicycle rides and weather datasets. It writes all of the obtained statistics to a Mongo collection `kgarber.bluebikes.ride_weather_stats`.

`alert_weather_stats.py` is the algorithm to get the correlation coefficient, p-value, and best fit line for mbta alerts and weather datasets. It writes all of the obtained statistics to a Mongo collection `kgarber.alert_weather_stats`.

I modified `alerts_and_weather.py` to output an intermediary dataset with a datapoint for each day that has the alert count and temperature. 

`new_station.py` has the z3-optimizer logic. It writes the latitude and longitude of the new station to a collection in Mongo named "new_station".

# Project 3

Project 3 includes a data visualization of bluebike data. To run it, make sure you run projects 1 and 2 to generate the necessary collections in MongoDB. Then navigate to the project-3 folder and run the command `flask run` to start the flask server, then visit your localhost to see the D3-generated graphics.

**NOTE: The report for project 3 is at `/project-3/README.md`**
