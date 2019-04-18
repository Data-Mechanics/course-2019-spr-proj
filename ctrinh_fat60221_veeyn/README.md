# ctrinh

Subfolder for Christopher Trinh, Tom Kang, and Vee Nguyen in the Spring 2019 iteration of the Data Mechanics course at Boston University.

## External Dependencies
* `pandas` module
* `json` module
* `sklearn.cluster` module
* `numpy` module

## Project 2

### Purpose

Our idea revolved around coffee shops and transportation. Boston is a city full of busy commuters, resulting in an often hectic morning commute. We wanted to explore those commutes that revolves around stopping at a coffee shop to get one's morning caffeine before heading to the train station continue their commute to their workplace -- a narrative that we all know much too well. Many commuters struggle to optimize their morning routine in order to both energize and prepare themselves while still managing to get to work on time, and we challenged ourselves to try to optimize this situation.

### Goal

Our goal is to find the optimal placement of coffee shops, whether that involves moving existing ones or adding new ones, along the MBTA train lines. More specifically, we will retrieve data focused on just the Green Line to limit the scope of our calculations. In order to assign priority to parts of the Green Line, we will put more focus on stations that are more busy than others during peak morning commute times. By determining optimal locations of coffee shops based on MBTA train station locations, we hope to theoretically minimize travelling time in Boston during the morning commute for those who need to get from a coffee shop to the nearest train station, which would hopefully decrease both traffic and additional environmental pollution from motor vehicles.

### Assumptions

The "peak morning commute times" that we mentioned earlier no doubt vary for many parts of Boston, but for the sake of convenience, we will be operating under the assumption that peak morning commute times are any time from 6am to noon. We also needed to determine a metric that measured how busy a train station was, so we decided to use number of departures from a particular station in order to measure this. On certain stretches of the Green Line during morning commutes, the MBTA often features express trains that pass through multiple stops without stopping to allow for a departure at that stop. Thus, we worked under the assumption that the more departures a station has, the more people waiting to get on at that stop, which would be an effective measure of how busy that station is. 

Another important assumption we made is that commuters take Uber rides from the coffee shop to the nearest train station. By performing a statistical analysis at the end to the total Uber mean travel times of 2018 with our new coffee shop location data, we could provide an estimate for how much Uber travel times could decrease if coffee shops were moved closer to the nearest train station using the `k-means` algorithm.

In order to further optimize the locations of the coffee shops to lean towards fitting more busy train locations ("busyness" calculated by the number of departures of that station), we added a weight to each train station. The weight is assigned by adding more data points to the train station location. Thus, when running the `k-means` algorithm, the optimized locations of the coffee shops will become more centralized and/or closer to train stations that are more busy.

### Methodology

We utilized the `k-means` algorithm in order to the optimal placement of coffee shops along the Green Line. The data sets we used are as follows:

1. `coffee.py` to get the coffee shops from the Yelp Fusion API.
2. `stations.py` to get the MBTA Green Line train station location data along with their closest coffee shops from the MBTA API.
3. `uber.py` to get the mean Uber travel times of 2018 sorted by hour of day, provided they fall under our "peak morning commute times", from the Uber Movement API.
4. `getMBTA.py` to get data from the MBTA API and calculate each coffee shop's distance from their closest train station. 
5. `getMean.py` to get the average distance (in meters) that a coffee shop was from their closest train station.
6. `getKMeans.py` to perform the `k-means` algorithm and calculate new locations of each of our coffee shops.
7. `kmeansDist.py` to finds the closest distance for a station from the list of generated `k-means` and get the new distance for each coffee shop, averaging it over the number of stations.
8. `savings.py` to calculate how much more efficient the new coffee shop locations are in terms of Uber travel times.

### Building

Simply run `execute.py` on the root project directory to run the scripts.

## Project 1

The idea that I was pursuing in this project was the notion of how the weather influences transportation, whether it be cars, bikes, or even trains. I was hoping that by finding examining monthly weather and climate data along with the usage levels of different modes of transportation, an analysis could be performed discerning the different kinds of transportation in relation to the type of weather.

To achieve this, I used five distinct datasets:
* Monthly Boston weather from 2018 and 2015 (`weather.py`)
* MBTA train data (`mbta.py`)
* Monthly Uber trip usage from 2018 (`uber.py`)
* Monthly Bluebike trip duration from 2018 (`bluebikes.py`)
* Monthly cars parked at parking lots from 2015 (`parking.py`)

After gathering each of these datasets and storing them in MongoDB, I then transformed the weather dataset and combined them with projections of the Uber, Bluebike, and parking datasets. This created `reWeatherUber.py`, `reWeatherBluebike.py`, and `reWeatherParking.py` respectively. The idea was that each of these three distinct transformed datasets can be used to examine the correlation (if any) between weather and the usage levels of each mode of transportation examined in each data set.
