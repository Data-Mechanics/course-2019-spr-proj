### Project1 and spark project Description 

T1he Main project we are assigned is a spark project named "South Boston Neighborhood Development" the goal of which is to identify buildings that may not even be on the market yet and then finding contact information of the person/people who own it and permit number of the buildings. Since the initial project doesn't meet the requiemetns for project#1 we decided to define some interesting questions whithin the scope of our main project which could be potentially answered using the datasets given to us by spark. Answring these questions and analysing their answers could also provide a solution for the main project. 

The two quesotions to be answred are the following:

1- Finding information about the buildings/properties in the most dangereous neighborhood of Boston area. These infomation include the value of these porpeties, type of the crimes happening in the neighbourhood, permit number of buildings, owners of the properties,...

2- Clustering food establishments in Boston area using their location and then finding infomation about properties in the most compact cluster (with the most food establishments in it) to see if access to food establishments has any impact on the population living in that area or on the value of properties.

---
Data Sets
---

Permit Database : https://data.boston.gov/api/3/action/datastore_search?resource_id=6ddcd912-32a0-43df-9908-63574f8c7e77&limit=125650

Crime Incident Database : https://data.boston.gov/api/3/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&limit=366640

Active Food Establishments: https://data.boston.gov/api/3/action/datastore_search?resource_id=f1e13724-284d-478c-b8bc-ef042aa5b70b&limit=3010

Street Names: https://data.boston.gov/api/3/action/datastore_search?resource_id=a07cc1c6-aa78-4eb3-a005-dcf7a949249f&limit=18992

---
Aditional Resources
---

Zillow Search API: "https://www.zillow.com/howto/api/GetSearchResults.htm"

We are using this API to create a database of the property addresses that are on the market which we are not using for project#1 but is required for the main South Boston Neighborhood Development project. 

Assesssors (Assessing online - City Of Boston): https://www.cityofboston.gov/assessing/search/ 

We are scraping this website to find the information we are interested in about the propeties in City Of Boston including the value of propeties. This resource is useful for both project#1 and Main project.

**Please note that scraping a website or calling an API is such a slow process and scraping assessors for all the street addresses is beyond the time and resources available so we have put a limit on the number of addresses we want to scrape assessors for or call the api for**

----
Overview Of Transformations 
----

"aggregateCrimesIncident", "mergeValueWithCrimeRate" and "mergeValueWithPermitAndCrime" are 3 transfomations done to provide an answer for the first question by first aggregating crime incident data set using the "street" attribute as keys and then sorting the result in ascending order then finding the value and ParcelID for the result by scraping `Assesssors` (we can look at this part of tranfomation as a merge but one of the data sets is in the cloud and not stored in the data base due to time constraints explained) and finally merging the result with crime indincet and permit databases and projecting the desired attributes. Please note that our notion of "the most dangerous" here is the street addresses with the most records in "crime incident report" database.

"foodStablishmentClusters" provides an answer for the second question asked by fisrt clustering food stablishments by ther locations, then counting the number of food stablishments in each cluster, then finding the closest food stablishment center for each building in permit data base) and finally storing the infomation about propeties in the most compact cluster (with the most food stablishments) into a databse.

-------------------------------------------------------------------------------------------------------------------------

## Project 2


### Project 2 Narrative:

**Optimization Problem**:

For project 2 we are interested in finding a linear regression model between crime rate of streets and the average value of of the properties in that street. since the logic behind finding a best fit linear model is to maximize a maximum likelihood estimator which means minimizing a sum of square errors function, this counts as an optimization problem. we are have also found the regression model between age of the owner and number of the properties the own and we have done this for multiple sub sets of data and for different range ages. Please see next sections short report of results. Using these results we can come up with interesting interactive visulization for project 3.

**Statistical Analysis**:

we have found the correlation coefficient and their p-values between crime rate and Average value of properties per street.
Also we have done correlation analysis between age of owners and the number of properties the own in Boston for different age ranges. Please see next sections short report of results.

---------------------------------------------------------------------------------------------------------------------------

### New data sets Added for project2 and spark project:

**Voter File**: Spark has been able to get the voter file from city hall which contains information about all the people in south Boston who voted. This data set is really important for completing both spark project and proj#2 and proj#3 since it contains personal information of people including their age and occupation and phone number. (the purpose of spark project is to find the contact information of owner of buildings which are not on the market. we have cleaned up this data set and converted it into json format. now this data set is available at: http://datamechanics.io/data/asadeg02/Voter-File.json

please note that the code for cleaning up is not included. it will be included in spark project GitHub repository.

**Property Per Voter**: the addresses in voter file are the voters residency addresses but we are interested in finding first all the properties (Potentially in Boston) they own and then finding some information including value about those properties.
we extracted all the first names and last names from voter file and we used last name + first name the search key to scrap assessors website: https://www.cityofboston.gov/assessing/search which gives us the exact addresses of all buildings/properties in Boston owned people and so merging this data set with voter file data set allows us to have access to both contact information of the owner and address of the their properties. again since scraping is really slow and it took a couple of hours to scrape the Assessors for all the voters in voter file we have done that separately. the code is not included in this repo but will on spark GitHub repository. this data set is now available at: http://datamechanics.io/data/asadeg02/Property-Per-Voter.json

-----------------------------------------------------------------------------------------------------------------

### New transformations added for project2:

One of the things that we were interested in for project 2 is to find the correlations between the safety of a street and the value of the properties in that street and see if there is a good linear model for modeling the relationship between these two. for that we have added a new transformation algorithm which operated on top "address_crime_rate" data set derived in project 1 and Boston streets data sets also derived in project 2. 
we have divided the Boston streets into two categories: safe street and dangerous street. safe streets are the ones which don't have any record in "address_crime_rate" data set and dangerous streets which appear in address_crime_rate" data set. obviously crime rate for safe streets is zero. for modeling the regression and correlation we only sample a few streets from safe streets since the number of street with crime rate value equal to one is a lot more than dangerous streets. In order to get good regression results we have constrained the number of safe street we include in our analysis.  
In order to computer the average property value in a street, we have scrapped assessors and stored the result data set in "crime_rate_mean_value" repo. the ids in this data set are the street names and value is the average property value in that street. 

Another analysis that we wanted to do for project 2, as to find the correlation between age of a person an the number of properties that person owns. for that, we have merged "voters_info" repo and  "properties_per_voter" repo to be able to get a data set whose documents include "age", "numProperties", "phone", "occupation" "addr" attribute. please note that this address is the address of the buildings they own and not (the address where the live included in voter file) that's why we we had to merge these two data sets to be able to access personal information of owners. the merge has been done on both first name and last name. and finally since we aggregate the results in "age_numProperties_phone_ocupation_addr" by owner's complete names to count the number of properties each person owns and get their age and store the results in "owner_num_properties_age". the documents in this repo have the format doc = {'id': "completename", value:{num_properties: number, age:age}}.

---------------------------------------------------------------------------------------------------------------------------

## Results


 **Correlation results between age of owners and number of peroperties owned by them**
 
| **Age Range** | **Correlation coefficient**  | **P-Value** |
| :---         |     :---:      |          ---: |
| 19-35 | 0.08669242032631995   | 0.464   |
| 25-45 | 0.0633716882769931    | 0.4545  |
| 20-45 | 0.0628646535726424    | 0.453   |
| 35-65 | 0.08957347833195095   | 0.2965  |
| 40-70 | 0.07076712290817734   | 0.453   |
| 30-60 | 0.10996008434546152   | 0.1635  |
| 40-95 | 0.06203316105818047   | 0.4585  |
| 50-95 | -0.03671870899042569  | 0.7155  |
| 30-95 | 0.10281159124264036   | 0.1225  |
| 19-95 | 0.1296640596971729    | 0.0415  |

--------------------------------------------------------------------------------------------------------------
 **Regression results between age of owners and number of peroperties owned by them**
 
| **Age Range** | **Coefficient**  | **mean_square_error** | **score** |
| :---:          |     :---:      |        :---:           |  :---:              |
| 19-35 | 0.06457659574735818   | 0.011447527459944972   | 0.016812768377152065  |
| 25-45 | 0.059868320318481105  |  0.022085463487526723  | 0.007515575742035541  |
| 20-45 | 0.03787878787878789   |  0.02559387479445619   | 0.004015970875076436  |
| 35-65 | 0.04390117273393598   | 0.02517711446647403    | 0.0039519646688082055 |
| 40-70 | 0.05428317736824225   | 0.035170128651494326   | 0.008023408020484402  |
| 30-60 | 0.045195202153052456  | 0.038141836940601415   | 0.005007985684701399  |
| 40-95 | 0.061237256327455324  | 0.027621092299317116   | 0.012091220149260895  |
| 50-95 | 0.03448494219519718   | 0.01646814358250919    | 0.0038481130708705176 |
| 30-95 | -0.0233407012541895   | 0.020609223023722707   | 0.0013482635899237927 |
| 19-95 | 0.04826590216661626   | 0.01256355467925116    | 0.010570223293843717  |

**Regression results between crime rate and average property value per street(with a small number of safe streets included whose crime rate is zero)**

![rsz_1crime-rate-regr](https://user-images.githubusercontent.com/32320836/56332285-ae8f7200-615d-11e9-8d7f-119d1af70fe6.png)

Regression results for age and number of properties owned for different age ranges are note included here. but after running the code they will be stored in root directory automatically.

-------------------------------------------------------------------------------------------------------------------------

**Note about trial mode**: 

I have already imposed really hard limits on the length of subsets of data we're working with but since we are scraping a web site it's still takes a little bit over 2 minutes. generally this not a problem since for spark project every thing is stored in a file like "Property Per Voter" and we read from that. The reason why we aren’t using them here is because we wanted to use different techniques of data retrieval including scraping for class project.

---
Running The Code
---

In Order to be able to run the code you need to install `selenium` and `xmltodict`. We are using selenium for scraping Accsesssors and using xmltodic for parsing the Zillow Search API responses since they are in xml foramt.

For scraping Accsesssors in scrapeBostonGov method in "mergeValueWithCrimeRate.py" module, you need to specify the path to your chrome web driver. The path already used, is the default path when you install chrome web driver. 

-----------------------------------------------------------------------------------------------------------------
## Project 3
For project 3 we have implemented a web app which runs the statistical analysis for correlation between age and number of properties owned. this web app is impelemnted using flask.(app.zip)
We have also implemented an interactive map for all the properties not on the market in south Bsoton.
this map has been created using folium and we have embedded the html file witihn our java script app which is to be delivered to spark.(nteractivemapapp.zip) 

Below is the picture of our interactive map

![map3](https://user-images.githubusercontent.com/32320836/57174243-e9ee9980-6e09-11e9-8959-7fd979acb32e.png)

Red makerker are the porperties that we have their owners's contact information and greens onew are the ones that we don't have thier owners' contact info.
