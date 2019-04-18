# Project 2 
### Gijung(Tony) Kim : tkixi@bu.edu
### Nuan Huang : nhuang54@bu.edu
### Dennis Wu : wud@bu.edu

## Narrative
Tony, Nuan, and Dennis have always preferred to ride bikes to their destinations in Boston.

As a biker, they would like to know what are some of the factors that may contribute to bike accidents or a potential correlation that increases bike accidents. Especially in a city like Boston, it is critical to analyze data on bikes. We are looking also to see the minimum hubway stations to cover all streets. By using a Z3 solver and pandas, we are able to compute solutions that help quantify the correlation between the accumulated number of street lights with total number of accidents on a particular streets of all streets with bike collisions in Boston, along with an optimal placing of Hubway stations along all biking lanes in Boston.




## Required libraries
```
pip install opencage
z3 -- go to https://github.com/Z3Prover/z3 for instructions
```

## Formatting the `auth.json` file

I use OpenCage's reverse geocoder in my transformations so you will need to adjust your auth.json file accordingly.
Here is the link to OpenCage https://opencagedata.com/ 
You should be able to test my transformation with the free version as I have set a bound.
```
{   
    "services": {
        "openCagePortal": {
            "service": "https://opencagedata.com",
            "api_key": "somekey"
        }
    }
}
```


## Running the execution script

```python
python execute.py nhuang54_tkixi_wud 
or
python execute.py nhuang54_tkixi_wud --trial
```

## Optimization and Statistical Analysis

# Pandas Pairwise Correlation 
In pairwiseCorrelation.py, we utilized a dataset from a new transformation (streetlights_collsions.py). We took the number of bike accidents that occur on a particular street and accumulated the present number of streetlights on that particular street during the bike accident. After aggregating the two values with the street as the key, we were able to discover the total number of streetlights that were present over all the bike accidents on any particular street that had any bike collisions. From the value, we saw a positive pairwise correlation between the two values.

# Z3 Solver (Constraint Satisfaction Problem)
In findMinimalStations.py, we utilized the dataset of all biking paths in Boston. This is a constraint satisfaction problem that seeks to find the minimal amount of Hubway stations needed cover all of the biking paths in Boston. We start by finding the geolocation of streets using their latitude and longitude (obtained using OpenCageData). Then, we consider all points that a Hubway station can be placed (represented by 1:placed there, 0:not placed). Note that one of our constraints states that if two streets are close enough to each other (specified by some arbitrary value that we deemed an acceptable walking distance), we would only need one station to cover both streets. 

Then, using the constraints in our python file, we use the Z3 solver to find a solution. We tighten the constraints (by adding a constraint that requires the total amount of allowed Hubway stations to be 1 less than the previous iteration) each time we find a successful solution until we're unable to find one. The previous constraint is then the minimum amount of Hubway stations needed to cover all of the Boston biking lanes, our solution.


