# Growing Voter Engagement in Communities of Color
We are currently working on a BU Spark! Data Mechanics class project. Our partner is Amplify Latinx. We are looking for areas where there is an opportunity to increase the voter registration rate and voter turnout, particularly among communities of color.

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
