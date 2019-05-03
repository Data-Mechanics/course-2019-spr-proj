# CS 504 Project
Author: Nicole Mis  
Email:  misn15@bu.edu

## Project #1

In 2016, the worlds’ cities generated 2.01 billion tonnes of solid waste, amounting to a footprint of 0.74 kilograms per person per day. As the world continues to urbanize and grow in size, annual waste generation is expected to increase by 70% from 2016 to 2050, reaching 3.4 billion tonnes ([World Bank](http://www.worldbank.org/en/topic/urbandevelopment/brief/solid-waste-management)).

For project one, I wanted to analyze how effectively Boston has been handling its solid waste, and how this may impact the health of its residents. I wanted to be see whether I could pinpoint whether certain areas of the city are healthier than others and whether this was at least partly due to the environment. 

In order to tackle these questions, I decided to retrieve five datasets, which are outlined below:

  1) [Census Bureau API](https://github.com/datamade/census): This is a simple wrapper for the U.S. Census Bureau API. You can retrieve information from the American Community Survey 5-Year and 1-Year estimates as well as from the Decennial Census. I used the API to retrieve data about the average income and population for every census tract within Suffolk County. A census tract is a geographic unit within a county that is determined by the U.S. Census Bureau.
  
  2) [Chronic Health Data](https://chronicdata.cdc.gov/500-Cities/500-Cities-Local-Data-for-Better-Health-2018-relea/6vp6-wxuq/data): The Centers for Disease Control provides a dataset called _500 Cities: Local Data for Better Health_ which includes 2016 and 2015 model-based small area estimates for 27 measures of chronic disease. I used this dataset to retrieve prevalence rates of different chronic diseases such as cancer and kidney disease for every census tract in Boston. 
  
  3) [Oil Data](https://docs.digital.mass.gov/dataset/massgis-data-massdep-tier-classified-oil-andor-hazardous-material-sites-mgl-c-21e): This dataset was provided by the Massachusetts governtment. It is a statewide point dataset that provides the approximate location of oi and/or hazardous material disposal sites that have been reported and Tier Classified under M.G.L. Chapter 21E and the Massachusetts Contingency Plan (MCP).
  
  4) [Waste Data](https://docs.digital.mass.gov/dataset/list-massachusetts-hazardous-waste-generators-january-23-2018): The Massachusetts Department of Environmental Protection provides this data about all of the hazardous waste generators in the state of Massachusetts. I used this dataset to retrieve the locations of these hazardous waste sites. 
  
  5) [Crime](https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system): This dataset is retrieved from Boston's data portal, Analyze Boston. It provides data from August 2015 to the present day about all crime incident reports filed with the Boston Police Department. 
  
  6) [Zip Codes](https://www.huduser.gov/portal/datasets/usps_crosswalk.html) I used the U.S. Housing and Urban Development website to retrieve a data set that mapped census FIPS codes to zip codes because some of my data may only contain a FIPS code or a zip code so having this data set allows me to easily switch between the two. 

You can find these datasets in the files: _getIncome_, _getWaste_, _getCrime_, _getOil_, _getZipcodes_, and _getHealth_.

#### Transformations:

After retrieving this data, I tried to match up Boston zip codes with the number of hazardous sites, median income, crime rate, and health issues to see whether there was a correlation between these factors. I wanted to determine whether certain zip codes within Boston contained more hazardous waste sites and if these sites were located in areas with more poverty and more health problems.By looking at the proximity of areas with high crime, poverty and health issues to these waste sites, we can possibly determine whether this is contributing to some of these areas problems. 

  1) _transformDem_: This algorithm combined average income for every census tract with the waste site dataset. I counted the number of waste sites for every zip code and matched these zipcodes up with the average income from the census bureau tracts so the final dataset had the total number of waste sites and average income for every zip code within Boston.
 
  2) _transformHealth_: I combined the chronic health dataset with the waste site dataset to create a new dataset that had a list of chronic diseases that were found in the same zip code where that waste site was located. 

  3) _transformWaste_: This algorithm gets the coordinates and census bureau tract for each waste site and oil/hazardous material site so that it is easy to match each site with data collected on zip codes. I then combine the oil and waste site data into one dataset. 

## Project #2

For project 2, I first had to collect additional data sets in order to conduct an optimization and statistical analysis. 

   1) _getWasteAll_: I found several data sets on oil/hazardous waste sites within Massachusetts from the Massachusetts government and I     decided to merge them all into one complete data set. This data set ultimately adds to the oil and waste data sets that I retrieved in project one. The three data sets used are: [Oil/Hazardous Waste](https://docs.digital.mass.gov/dataset/massgis-data-massdep-tier-classified-oil-andor-hazardous-material-sites-mgl-c-21e), [Hazardous Waste Generators](https://docs.digital.mass.gov/dataset/list-massachusetts-hazardous-waste-generators-january-23-2018), and [Oil/Waste with Activity and Use Limitations](https://docs.digital.mass.gov/dataset/massgis-data-massdep-oil-andor-hazardous-material-sites-activity-and-use-limitations-aul).

   2) _getOpenSpace_: I retrieved data on all the green spaces in Boston from BostonMaps open data portal which is a website that contains geospatial data for Boston. You can find the data source [here](http://bostonopendata-boston.opendata.arcgis.com/datasets/open-space).

   3) _getSchools_: I retrieved data on all the public and non-public schools in Boston from Boston Maps open data portal. You can find the data source [here](http://bostonopendata-boston.opendata.arcgis.com/datasets/public-schools) and [here](http://bostonopendata-boston.opendata.arcgis.com/datasets/non-public-schools).

_transformWasteAll_: This combines all of the data sets that were retrieved using the getWasteAll algorithm and makes sure that every waste site has its corresponding coordinates and census tract. This data set is then stored in MongoDB and is used for the waste data set for project 2. 

_transformOpenSpace_: This gets all of the centroids for all of the open spaces and classifies them into their respective census tract so that the centroids and census tracts can be used in other computations.

_cleanHealth_: This transforms the health data set from having every row represent a different disease to every column representing a different disease so that for every census tract I could have the prevalence rate for all the diseases.

_crime_health_waste_space_: This algorithm sums up all of the crime, health issues, open spaces, and schools in a census FIPS tract and combines it with the population and average income of that area.

#### Optimization

_linearRegression_: I decided to use gradient descent and linear regression to determine whether there was a relationship between the data sets. I first used gradient descent to see if I could find a solution that minimized the mean squared error more than linear regression. However, after running the algorithm using hundreds of iterations and a very small step size, the gradient descent algorithm had a very high mean squared error. I found that gradient descent is computationally less expensive but doesn't yield the most accurate results. So, instead, I used the statsmodel api in python which yielded more robust results. In addition to finding the coefficient, it allows me to find the p-values, t-values, and standard errors. I am also able to use robust standard errors with the stasmodel api linear regression which allows me to control for heteroskedasticity.

However, since one of my health outcome variables is prevalence rate, which is a proportion, I decided to also average all of the prevalence rates for every fips tract to get an average disease prevalence rate for an area. Then, I performed logistic regression using crime, waste, income, and open spaces as my regressors to see if this was better at approximating the relationship than the linear regression. The equations for the linear regression I estimated are below:

 ![equation](https://latex.codecogs.com/gif.latex?y_%7Bhealthproblems%7D%20%3D%20%5Cbeta_%7B0%7D%20&plus;%20%5Cbeta_%7B1%7Dx_%7Bcrime%7D%20&plus;%20%5Cbeta_%7B2%7Dx_%7Bwaste%7D%20&plus;%20%5Cbeta_%7B3%7Dx_%7Bopenspace%7D%20&plus;%20%5Cbeta_%7B4%7Dx_%7Bincome%7D%20&plus;%20e)
 
 ![equation](https://latex.codecogs.com/gif.latex?y_%7Bcrime%7D%20%3D%20%5Cbeta_%7B0%7D%20&plus;%20%5Cbeta_%7B1%7Dx_%7Bwaste%7D%20&plus;%20%5Cbeta_%7B2%7Dx_%7Bopenspace%7D%20&plus;%20%5Cbeta_%7B3%7Dx_%7Bincome%7D%20&plus;%20e)
 
The logistic regression equation I estimated is below:

 ![equation](https://latex.codecogs.com/gif.latex?y_%7Bdiseaseprevalence%7D%20%3D%20%5Cbeta_%7B0%7D%20&plus;%20%5Cbeta_%7B1%7Dx_%7Bcrime%7D%20&plus;%20%5Cbeta_%7B2%7Dx_%7Bwaste%7D%20&plus;%20%5Cbeta_%7B3%7Dx_%7Bopenspace%7D%20&plus;%20%5Cbeta_%7B4%7Dx_%7Bincome%7D%20&plus;%20e)

_WasteOptimization_: I also thought it would be useful to find the centroids of these waste sites and find the centroids that maximize the distance from these waste sites to schools and green spaces. I also wanted to find the centroids that are located in the least densely populated areas. I wanted to find these clusters so that in the future, waste sites could be situated in these areas so as to minimize the impact hazardous waste has on people's lives.

#### Statistical Analysis

_Correlation_: I computed the correlation coefficients between health and crime, waste, open spaces, and income as well as between open space and income to see whether there was a relationship between any of these factors. I also defined my own custom scoring metric to determine which neighborhoods have a higher quality of life. This metric is defined as follows:

  ![equation](https://latex.codecogs.com/gif.latex?y%20%3D%20x_%7Bhealth%20problems%7D%20&plus;%20x_%7Bopen%20space%7D%5Ccdot%20w_%7Bopen%20space%7D%20&plus;%20x_%7Bcrime%7D%5Ccdot%20w_%7Bcrime%7D%20&plus;%20x_%7Bwaste%7D%5Ccdot%20w_%7Bwaste%7D) 
  
The w's are the corresponding correlation coefficient between that factor and health and the x variable is the number of those factors. I calculate this metric for all zip codes in Boston to get a sense of how healthy every neighborhood is. I used the correlation coefficient as a weight and multiply it by the actual number because I thought this would be a good indicator of their absolute effect on health.

_Justification_: For this project, I wanted to explore the relationship between the various data sets that I retrieved. I decided that a good way to approach this would be to employ gradient descent to find the parameters that best explained the number of health occurrences for each FIPS code in Boston. I decided to use gradient descent at first because it is a computationally efficient way to do linear regression. However, after doing gradient descent, I had a large mean squared error. So I decided to also do linear regression using the statsmodel api. The coefficients are calculated along with their corresponding t-values, p-values, and standard errors so it is easier to see whether your results are significant. In general, it gives you a good idea of how your independent variables such as waste and open spaces in this case affect health in a certain area and it also gives you a good idea of if your data is linear. But since I could easily create a probability as my outcome variable since I have prevalence rates, I decided to also do logistic regression to see if there would be a better fit. 

If waste turned out to be a significant factor affecting health, I wanted to calculate areas where waste sites would have minimal impact on people's lives. In order to do that, I used DBSCAN clustering because this library allows me to use the haversine metric which is better for geospatial data. I find waste clusters and then find the distances from these clusters to schools, open spaces, as well as the population of the fips tract that the cluster resides in. Then I rank the clusters based on these factors.

Finally, I find the correlations between health and factors such as waste, open spaces and crimes to see if there is a relationship and how strong that relationship is. I also create a custom metric that gives you a sense of how healthy a certain area is. I hoped that by doing this conclusion I could determine what sort of external factors affect a person's health and thereby, pinpointing areas where the city could help improve the quality of life for its citizens.

#### Dependencies
pyproj
uszipcode
geopy
math
census 
us
sklearn
statsmodels
scipy
shapely
copy
requests