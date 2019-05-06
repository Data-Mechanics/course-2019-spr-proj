# South Boston Affordable Housing Project Report

## Authors
##### Ellen Mak, Xiaoyi Gabby Zhou, Kayla Ippongi, Ziyu Shen (ekmak_gzhou_kaylaipp_shen99)

# Introduction
Gentrification has long been a problem of the Greater Boston area. South Boston Neighborhood Development Corporation (NDC) aims on preserving and creating affordable housing in order to improve the quality of life in the neighborhood. NDC looks into finding target properties and retrieve contact information of owners before target housing is posted on to market for selling. In this project, our goal is to identify target buildings for South Boston NDC to consider purchasing and contact the property owners.


## Data Portals and Datasets
Following are the datasets we retrieved from public resources available online:
*  [Boston Street Address Management](https://data.boston.gov/dataset/live-street-address-management-sam-addresses/resource/26933f1b-bcaa-4241-b0f2-7933570fd52d)
	* All addresses in Boston
* [Boston property assessment](https://data.boston.gov/dataset/property-assessment/resource/fd351943-c2c6-4630-992d-3f895360febd)
	* Tax assesment for properties in Boston
* [Zillow Search Results Data](http://datamechanics.io/data/zillow_getsearchresults_data2.json)
	* Zillow valution/zestimate, full address, zillow links 
* [Zillow Property Results Data](http://datamechanics.io/data/zillow_property_data.json)
	* Number of bed,baths, valuation, home description for given property 
* [Boston Permit Database](https://data.boston.gov/dataset/approved-building-permits)
	* List of approved building permits for construction in Boston

# <b>Project 1: Derived Datasets</b>

<b>Dataset 1(num_per_street1):</b> We performed <b>union</b> on the Boston address and accessing datasets, took counts for every address, and removed any duplicates. Then, we called <b>map reduce</b> to <b>aggregate sum</b> on every address in order to get the number of properties/buildings per street. Our new datasets show 355 unique streets in South Boston. 

<b>Dataset 2 (type_amount):</b> We took two Zillow API calls (getPropertyData and getSearchResults) and combined them to get the different types of units and their average cost. We did this by first <b>filtering</b> out only the South Boston addresses then took the <b>product</b> and took out the duplicates. Finally we combined the two datasets by taking the <b>average</b> of the amounts and <b>mapping</b> it to the respective unit type. 

<b> Dataset 3 (newly_renovated_5_years): </b> The Zillow API and Permit records were <b>unioned</b> to get the most recent constructions in South Boston. We used <b>map reduce</b> to filter out the most recent renovations that happened in the last 5 years.  

With these three new datasets, we can gain a better understanding of the housing status of the South Boston’s streets and the potential properties for NDC to keep an eye on. With the transformed datasets, we see which streets are most populated and which streets are more family-friendly for those units locating on certain streets are more cost-efficient and newly renovated. Dataset 2 (type_amount) is particularly beneficial for the South Boston NDC because it gives them an idea of which streets contain more individual houses versus apartments, and offers them the option to keep track of some streets with more target types.  

# Project 2: Data Transformation
### Constraint Satisfaction 
Our main goal is to identify buildings that may not even be on the market yet and then finding and contacting the property owners. To solve our problem, we utilized constraint satisfaction to filter and narrow down the properties in South Boston in order to figure out which properties to target. We filtered based on <b>property valuation, property type, overall property condition and the number of rooms/bedrooms</b> and returned all properties that fit those constraints. 
Additionally, we used a greedy algorithm approach to optimize a user’s specified budget to maximize or minimize the number of properties to buy, and all of the above can be found in `filter_constraints.py`.  
Moving forward, these constraint and optimization techniques that we formed in project two can be reused in project three to build an interactive ui. For example, a user can input a price range/budget and type of property they’re looking for and the site outputs a map or list of properties that fit their given constraints. 

### K-means
Our team was also interested in determining if there was a correlation between property price and location in South Boston. Rather than using correlation coefficient, we used k-means on the price and scaled longitude and latitude coordinates in order to cluster the properties, then compared the standard deviation (std) and mean for each cluster to the overall mean and std of the dataset. The optimal k = 4 was determined via the elbow method which is in `k_means_price.py` along with the rest of the k-means code and graphed in `figure_1.png`. 

## South Boston Property Price Statistics
* Mean: $860,603.44
* STD: $405,760.96
* Number of Properties: 11,478

## K-Means Statistics
![Figure 1: Cluster Result](https://github.com/kaylaipp/course-2019-spr-proj/blob/master/ekmak_gzhou_kaylaipp_shen99/k_means.png)
	
|*Cluster*|Mean|STD|Varience|Range|
|---|---|---|---|---|
|0|777,963.67|255,664.67|65364426342.38|318,167 - 2,015,608|
|1|728,779.33|220,385.26|48569666667.76|313,645 - 1,614,889|
|2|796,221.34|225,201.55|50715741323.79|348,639 - 1,414,535|
|3|1,908,919.74|493,819.63|243857832656.78|1,309,041 - 10,808,461|

# Project 3: Web Interface
We built the web interface with Flask and Python. There are several main features in the site - firstly the interactive map built with folium, filtering and listing properties based on what the user specifies and also the option to list properties that maximize their budget. Both features are used in the interactive web service we created to generate an individualized search result list for our client (SBNDC). The client can efficiently utilize budget to purchase target properties and maintain the affordable housing in South Boston neighborhood based on the optimized choices of property result list. 

 <p align="center"> 
    <img src="https://github.com/kaylaipp/course-2019-spr-proj/blob/master/ekmak_gzhou_kaylaipp_shen99/demo.gif" 
     style="width: 10em; height: 20em;">
 </p>
 
 To run the web service, go into the `web_app` directory and run `python webapp.py`

# Limitations and Future Work
In the future, we would like to implement more filters to narrow target housing (e.g., transportation proximity). A limitation we encountered was we did not have access to owner data freely - while we did have property owner's name and mailing address (via Boston Tax Accessing dataset), we were unable to access their phone numbers or email, which was necessary in order to fulfill the original goal of building an automated contacting service. We were provided a Boston voter listing from Spark which had ~2,000 voter information, including age and phone number, however none of the owners from the accessing dataset were actually in the file, thus we didn't end up using that dataset. We hope to one day obtain a data set of property owners’ contact information (e.g. census, WhitePages API) to automate the contact process and expand our project. 



# Conclusions
Our analysis indicates that there is a correlation between property location and price - properties inland tend to be below the average south boston property price, while properties closer to the coast tend to be more expensive. While we were unable to build an automated contacting service due to our restricted API usage, the interactive web service acts as an aggregated data portal that lists property owners in a user friendly manner. Here, SBNDC can target filtered properties and at least send out snail mail to properties of interest. With the data and analysis we collected, we hope that this can be of use to the  South Boston Neighborhood Development Corporation in their commitment towards making South Boston a safe and affordable community. 

# Auth.json
The auth.json file should contain a key to acess the Zillow api. You can retrieve a key [here](https://www.zillow.com/howto/api/APIOverview.htm) 
```
{
	"services": {
		"zillow": {
			"service": "Zillow",
			"key": "X1-ZWz1gx7ezhy3uz_9abc1"
		}
	}
}
```
# Running 
```
python execute.py ekmak_gzhou_kaylaipp_shen99
```

