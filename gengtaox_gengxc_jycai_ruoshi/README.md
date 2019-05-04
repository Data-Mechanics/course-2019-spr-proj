# Growing Voter Engagement in Communities of Color
We are currently working on a BU Spark! Data Mechanics class project. Our partner is Amplify Latinx. We are looking for areas where there is an opportunity to increase the voter registration rate and voter turnout, particularly among communities of color.


-----

For project #3:

### Running Instruction: 

1. Please do the data preprocess (python execute.py gengtaox_gengxc_jycai_ruoshi) before running the web server;
2. The web server file is in "Project#3-Web" folder, called "server.py";
3. Just run it by "python server.py".

1. We created a web-based visualization for voter and election data in boston city and its 22 wards.
In this page, user can see the registered-eligible voters amount, voted-registered voters amount and the difference between the first two winning candidates on city scale, as well as ward scale.
By clicking the ward on the map, user can see the bar chart and pie chart of different age groups, which helps visualize the proportion.

2. We also created a couple of Restful API which can fetch the numerical data by ward level. 

    - /api/v0.1/Ward/<id> : get all the data for ward <id>
    - /api/v0.1/Election/<id> : get the election data of the first two candidates for ward <id>
    - /api/v0.1/Voter/<id> : get the voters' distribution data for ward <id>

-----

For project #2:

1. We tried to find the strength of the relationship between each age group in Boston for non-registered voter and registered voters. In order to solve this problem, we decided to try to calculate the correlation coefficient for each age group. We first fetch the data from a bunch of excel spreadsheets. Then, we aggregate the population data by different age group in each ward. Then we calculate the correlation coefficient for different age group and the total number, as well as the p-value. In this way, we can find out how strongly each age group is connected to the total non-registered/registered group.

2. In 9 congressional districts of MA (excluding none marked area), if we aim to choose at most 4 of them for election propagating and promote the voter population. For the west part 3 districts (Springfield, Worcester, Lowell), at leaset one 
district should be covered, and one should form Taunton, Framingham, Gloucester, and for the south 3 congressional districts, there shoulb be one covered. If the 3th and 4th district be chosen for election promotion, Farminghamthe can be partly covered, so we just need one of them selected. If we want to cover most population in MA, we need to figure out the CDs to make election promotion.

-----

For project #1, we combined several datasets into three which we may use in the future projects. 

1. We fetch the dataset of ages in different congressional districts from US Census Bureau website, and combine them with the dataset from our partner’s more detailed dataset to get the age and gender distribution in different CDs.
2. We fetch the map of 10 CD in MA from US Census Bureau website for the past couple of years, convert them into standard GeoJson format (using ogr2ogr tool) and categorize them by their CD id.
3. We fetch the dataset of candidates and candidate's committees from MA OCPF website, and combine the information with all registered candidate office districts to find the correlation between candidates and districts.
