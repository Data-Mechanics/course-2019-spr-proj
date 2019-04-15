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
  
You can find these datasets in the files: _getWaste_, _getCrime_, _getOil_, _getZipcodes_, and _getHealth_.

#### Transformations:

After retrieving this data, I tried to match up Boston zip codes with the number of hazardous sites, median income, crime rate, and health issues to see whether there was a correlation between these factors. I wanted to determine whether certain zip codes within Boston contained more hazardous waste sites and if these sites were located in areas with more poverty and more health problems.By looking at the proximity of areas with high crime, poverty and health issues to these waste sites, we can possibly determine whether this is contributing to some of these areas problems. 

 _transformDem_: This algorithm combined average income for every census tract with the waste site dataset. I counted the number of waste sites for every zip code and matched these zipcodes up with the average income from the census bureau tracts so the final dataset had the total number of waste sites and average income for every zip code within Boston.
 
_transformHealth_: I combined the chronic health dataset with the waste site dataset to create a new dataset that had a list of chronic diseases that were found in the same zip code where that waste site was located. 

_transformWaste_: This algorithm gets the coordinates and census bureau tract for each waste site and oil/hazardous material site so that it is easy to match each site with data collected on zip codes. I then combine the oil and waste site data into one dataset. 

## Project 2

#### Optimization

For the optimization portion of the project, I first had to collect additional datasets. 

  1)_getWasteAll_: I found several datasets on oil/hazardous waste sites within Massachusetts from the Massachusetts government and I     decided to merge them all into one complete dataset. This dataset ultimately adds to the oil and waste datasets that I retrieved in project one. The three datasets used are: [Oil/Hazardous Waste](https://docs.digital.mass.gov/dataset/massgis-data-massdep-tier-classified-oil-andor-hazardous-material-sites-mgl-c-21e), [Hazardous Waste Generators](https://docs.digital.mass.gov/dataset/list-massachusetts-hazardous-waste-generators-january-23-2018), and [Oil/Waste with Activity and Use Limitations](https://docs.digital.mass.gov/dataset/massgis-data-massdep-oil-andor-hazardous-material-sites-activity-and-use-limitations-aul).

  2)_getOpenSpace_: I retrieved data on all the green spaces in Boston from BostonMaps open data portal which is a website that contains geospatial data for Boston. You can find the data source [here](http://bostonopendata-boston.opendata.arcgis.com/datasets/open-space).

  3)_getSchools_: I retrieved data on all the public and non-public schools in Boston from Boston Maps open data portal. You can find the data source [here](http://bostonopendata-boston.opendata.arcgis.com/datasets/public-schools) and [here](http://bostonopendata-boston.opendata.arcgis.com/datasets/non-public-schools).

_transformWasteAll_: This combines all of the datasets that were retrieved using the getWasteAll algorithm and makes sure that every waste site has its corresponding coordinates and census tract. This dataset is then stored in MongoDB and is used for the waste dataset for project 2. 

_linearRegression_: 

I decided to use gradient descent and linear regression to determine whether there was a relationship between the datasets that I already collected. 



Limitations:
Because the small area model cannot detect effects due to local interventions, users are cautioned against using these estimates for program or policy evaluations. 
