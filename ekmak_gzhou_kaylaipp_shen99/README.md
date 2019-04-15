# Project 1

##### Ellen Mak, Xiaoyi Gabby Zhou, Kayla Ippongi, Ziyu Shen (ekmak_gzhou_kaylaipp_shen99)

## Data Portals and Datasets

*  Boston Street Address Management 
	* All addresses in Boston
* Boston property assessment
	* Tax assesment for properties in Boston
* Zillow Search Results Data
	* Zillow valution/zestimate, full address, zillow links 
* Zillow Property Results DAta
	* Number of bed,baths, valuation, home description for given property 
* Boston Permit Database
	* List of approved building permits for construction in Boston

## Derived Datasets

<b>Dataset 1(num_per_street1):</b> We unioned the Boston address and accessing datasets and took counts for every address and removed any duplicates. Then, we called map reduce to aggregate sum on every address in order to get the number of properties/buildings per street. Our new datasets show 355 unique streets in South Boston. 

<b>Dataset 2 (type_amount):</b> We took two Zillow API calls (getPropertyData and getSearchResults) and combined them to get the different types of units and their average cost. We did this by first filtering out only the South Boston addresses then took the product and took out the duplicates. Finally we combined the two datasets by taking the average of the amounts and mapping it to the respective unit type. 

<b> Dataset 3 (newly_renovated_5_years): </b> The Zillow API and Permit records were unioned to get the most recent constructions in South Boston. We used map reduce to filter out the most recent renovations that happened in the last 5 years.  

With these three new datasets, we can gain a better understanding of the status of South Boston and it’s streets and potential properties to keep an eye on. With this information we can see which streets are most populated and which streets are more family friendly by figuring out which units are more cost efficient and newly renovated. This is particularly beneficial for the South Boston Neighborhood Development because this gives them an idea of which streets contain more individual houses vs apartments and offers them the ability to keep an eye on up and coming streets.  

## Project 2 

Our main goal is to identify buildings that may not even be on the market yet and then finding and contacting the property owners. To solve our problem, we utilized constraint satisfaction to filter and narrow down the properties in South Boston in order to figure out which properties to target. We filtered based on property valuation, property type, overall property condition and the number of rooms/bedrooms and returned all properties that fit those constraints. Additionally, we used a greedy algorithm approach to optimize a user’s specified budget to maximize or minimize the number of properties to buy, and all of the above can be found in `filter_constraints.py`.  Moving forward, these constraint and optimization techniques that we formed in project two can be reused in project three to build an interactive ui. For example, a user can input a price range/budget and type of property they’re looking for and the site outputs a map or list of properties that fit their given constraints. 

Our team was also interested in determinging if there was a correlation between property price and location in South Boston. Rather than using correlation coefficient, we used k-means on the price and scaled longitidue and latitude coordinates in order to cluster the properties, then compared the std and mean for each cluster to the overall mean and std of the dataset. The optimal k = 4 was determined via the elbow method which is in `k_means_price.py` along with the rest of the k-means code and graphed in `figure_1.png`. 
 
### South Boston Property Price Statistics
* Mean: $860,603.44
* STD: $405,760.96
* Number of Properties: 11,478

### K-Means Statistics
![Figure 1: Cluster Result](https://github.com/kaylaipp/course-2019-spr-proj/blob/master/ekmak_gzhou_kaylaipp_shen99/k_means.png)

	
|*Cluster*|Mean|STD|Varience|Range|
|---|---|---|---|---|
|0|777,963.67|255,664.67|65364426342.38|318,167 - 2,015,608|
|1|728,779.33|220,385.26|48569666667.76|313,645 - 1,614,889|
|2|796,221.34|225,201.55|50715741323.79|348,639 - 1,414,535|
|3|1,908,919.74|493,819.63|243857832656.78|1,309,041 - 10,808,461|



## Auth.json
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
## Running 
```
python execute.py ekmak_gzhou_kaylaipp_shen99
```
