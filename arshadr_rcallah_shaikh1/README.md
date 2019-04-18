# Project 2

## income_data.py
Updated script to ingest the income-in-the-past-12-months datset into our database. This dataset was used in our statistical analysis.

## price_per_sqrt_data.py
Updated script to ingest the City_ZriPerSqft_AllHomes.csv dataset into our database. We also transformed this data by removing many of the rows and only selecting those that are relevant to our project focus.

## unemployment_data.py
We used a new dataset that gave us the unemployment rates of the residents of Chelsea, MA. This script was used to put that dataset into our database. We also transformed this datset and removed unnecessary columns. 

## statistical_analysis.py
We performed our statistical analysis in this file by finding the correlation between unemployment rates and average home price per square foot. We also calculated the p-value in this file. Are results are explained in the description below. 

## optimization_and_constraint_satisfaction.py
In this file we have the scripts that perform the optimization and constraint satisfaction algorithms on our datasets. We used the income in the past 12 months dataset and found how much a new home should cost for at least 50% of people to be able to afford it. We used the Z3 library to do this but also devised a brute force solution since the Z3 route was returning irregular results. In our dataset we had the percentage of people in a certain income bracket, so we used this to create several uniform distributions of individuals so we could simulate it. 

## stat_functions.py
This file contains several helper functions that are used in other files. This is purely for readability and clean code.

## Narrative and Justification
The datasets we used in this project were incomes of residents of Chelsea, MA from the past 12 months, unemployment rates in Chelsea, MA, and the Zillow ZRI per square foot of all homes in Chelsea, MA. The problem we are trying to solve is determining if gentrification is occurring in Chelsea, Ma, and if so, what factors could be causing it. 

For the optimization and constraint satisfaction technique, we used the dataset that showcases the incomes of residents and determined what could be the maximum value of a home to be affordable to the current residents if it were to be built and put on the market. We made a few assumptions with this technique, including classifying a home as affordable if it was 4.5x the annual household income. The optimization in this technique is finding the maximum price of a home and the constraints that we placed were making sure at least 50% of residents could afford it. This relates back to our original problem because it is a tool that the city can use when issuing building permits for new homes.  

Using z3 was giving us very interesting answers. After repeatedly checking our work we still could not figure out why it was giving such low results for the maximum house price. We decide to devise a brute force solution that also had the same constraints and optimization task which gave us better results but took much longer. 

We used the unemployment rates and Zillow datasets to perform out statistical analysis. We wanted to determine if there was any correlation between the unemployment rates and home values and ultimately found that there was actually a strong negative correlation (~-82%). This conclusion is probable because when people have more disposable income, the demand for homes goes up along with the prices. The p-value for this analysis was relatively low at .001 which indicates it was a significant and unique result. 
 

# Project 1

# Data Pools
* datamechanics.io
* zillowstatic.com
* chelseama.gov

# Datsets
* ChelseaAssessorsDatabase2018.json
* 24a90fa2-d3b1-4857-acc1-fbcae3e2cc91.json
* income-in-the-past-12-months.json
* City_ZriPerSqft_AllHomes.csv
* Metro_Zhvi_Summary_AllHomes.csv
* housing-data.xls

We got our datasets from multiple different sources. Some of the datasets were giving us trouble in regards to having an api to read them, so we decided to download them and upload them to datamechanics.io, where we then read the data. Before we uploaded them to datamechanics.io, we used a script to convert them into the json format. For the datasets that didnt need to be uploaded to datamechanics.io, we used their html link to read them and then used pandas to convert the tabular data to json format. We also found more datasets that were relevant to our project, than were required so we put all of them into our database.  

# Transformations
For each of these datasets, they were in a csv/xls format and our main transformations were to convert them from tabular data to json so that they could be properly inserted into our database.

# Running the Tools
We split up the scripts that we wrote to collect and insert the data into two files, data_scripts.py, and data_scripts1.py. The reasoning behind this was simply for organization and keeping track of who works on which parts. The files can be run independently and will collect the data from their respective sources, perform the transformations, insert the data into our database, and generate the prov models. Our code is heavily based on the example.py file that we were given. To run the code, it is very similar to running the example.py file, as all thats needed is to uncomment the lines at the bottom of both data_script files.   