<Project 1>
We chose five datasets from Analyse Boston: Property Assessment, Police Station, Public School, Private School and Hospitals. We are trying to develop a relationship between the average house price in each zipcode area and the number of Police station, schools and hospitals in that area. Each dataset will be transformed in the form {"zipcode": xxxxxx, "value": xxxxxx}, where the value is average house price for Property Assessments and number of corresponding facilities in this area. In this form we can find a relationship between housing price and number of public facilities.




<Project 2>
	Our problem is to find a correlation between Average house price in each zipcode and the number of public facilities in this region (ex. Police Station, School, Hospital). We will divide the problem into 2 parts:
		1. Develop a linear function to predict average house price using existing data. We solve this problem as a optimization problem. We define our linear function as y=w*x+b where y is the actual price, w is the weight vector, b is the bias and x is the existing data. We define the cost function of our linear function to be MSE = (1/N)∑(yi - (w * xi + b)). In our problem, we want to minimize this cost to get the best linear function to predict average house price. We employ linear regression package from scikit-learn to minimize the cost and derive the linear function for prediction.

		2. Compute the correlation between number of each facility to the housing price. We use correlation coefficient to compare the relation between each attribute (school, hospital,police station) and property price. Firstly, we extract the property value and the number of facilities mentioned above within a specific zipcode zone from the mongod collections created in project 1. Then, we use numpy as tool to calculate correlation coefficients. According to our results, each zip code has exactly one police station, so the number of police stations does not affect the property price. The number of hospital has a positve correlation with the property value. The correlation coefficient is 0.56. The number of school has the strongest positive correlation with property value, which is consistent with our expectation. The correlation coefficient is 0.69.

	The output of both files are located in output.txt in folder ruipang_zhou482.
	External python packages: scikit
