# ctrinh

Subfolder for Christopher Trinh, Tom Kang, and Vee Nguyen in the Spring 2019 iteration of the Data Mechanics course at Boston University.

## External Dependencies
* `pandas` module
* `json` module

## Project 2

### Purpose

Our idea revolved around coffee shops and transportation. Boston is a city full of busy commuters, resulting in an often hectic morning commute. We wanted to explore those commutes that revolves around stopping at a coffee shop to get one's morning caffeine before heading to the train station continue their commute to their workplace -- a narrative that we all know much too well. Many commuters struggle to optimize their morning routine in order to both energize and prepare themselves while still managing to get to work on time, and we challenged ourselves to try to optimize this situation.

### Goal

Our goal is to find the optimal placement of coffee shops, whether that involves moving existing ones or adding new ones, along the MBTA train lines. More specifically, we will retrieve data focused on just the Green Line to limit the scope of our calculations. In order to assign priority to parts of the Green Line, we will put more focus on stations that are more busy than others during peak morning commute times. By determining optimal locations of coffee shops based on MBTA train station locations, we hope to theoretically minimize travelling time in Boston during the morning commute for those who need to get from a coffee shop to the nearest train station, which would hopefully decrease both traffic and additional environmental pollution from motor vehicles.

### Assumptions

The "peak morning commute times" that we mentioned earlier no doubt vary for many parts of Boston, but for the sake of convenience, we will be operating under the assumption that peak morning commute times are any time from 6am to noon. We also needed to determine a metric that measured how busy a train station was, so we decided to use number of departures from a particular station in order to measure this. On certain stretches of the Green Line during morning commutes, the MBTA often features express trains that pass through multiple stops without stopping to allow for a departure at that stop. Thus, we worked under the assumption that the more departures a station has, the more people waiting to get on at that stop, which would be an effective measure of how busy that station is. 

### Methodology

We utilized the `k-means` algorithm in order to the optimal placement of coffee shops along the Green Line. The data sets we used are as follows:

1.

## Methods
Our project utilizes k-means to identify where train stations are most busy and place a coffee shop there. We have the following data sets (and the algorithms associated with them):

1. MBTA Green Line stations and their coordinates
2. Coffee shops and their coordinates
3. MBTA Green Line stations and how busy they are

Determine the optimal location for each coffee shop based on station busyness.

1. Create a dictionary called `busyStations` where keys are stations and values are busyness.
2. Create a `busyStationCoords` list, which is a list of coordinates of busy stations. We run k-means on `busyStationCoords`, which will give us an idea of where to place a new coffee shop or where to move an existing one.

## Project 1

The idea that I was pursuing in this project was the notion of how the weather influences transportation, whether it be cars, bikes, or even trains. I was hoping that by finding examining monthly weather and climate data along with the usage levels of different modes of transportation, an analysis could be performed discerning the different kinds of transportation in relation to the type of weather.

To achieve this, I used five distinct datasets:
* Monthly Boston weather from 2018 and 2015 (`weather.py`)
* MBTA train data (`mbta.py`)
* Monthly Uber trip usage from 2018 (`uber.py`)
* Monthly Bluebike trip duration from 2018 (`bluebikes.py`)
* Monthly cars parked at parking lots from 2015 (`parking.py`)

After gathering each of these datasets and storing them in MongoDB, I then transformed the weather dataset and combined them with projections of the Uber, Bluebike, and parking datasets. This created `reWeatherUber.py`, `reWeatherBluebike.py`, and `reWeatherParking.py` respectively. The idea was that each of these three distinct transformed datasets can be used to examine the correlation (if any) between weather and the usage levels of each mode of transportation examined in each data set.
