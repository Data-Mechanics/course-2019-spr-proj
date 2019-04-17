# Project #1

# Project #2
**Duy Nguyen** and **Sarah M'Saad** and **Colleen Kim** and **Kelly Zhang**

## How to run this code?

### About mongoDB
- If the mongoDB is not running on the localhost, like how this code initially set-up, there must be an ssh tunnel to the server.
`ssh -L 27017:localhost:27017 user@example.org`

## Choice of Datasets/Services

- From Analyze Boston:
    - Food Inspection (Violation)
- From Yelp API:
    - Businesses Around Boston (Restaurant/Bars)
- From Google Maps Geocoding API
    - Locations for interesting_spaces

## Objective of these Datasets:
In Boston, there are many great places to eat. On a day trip to Boston there is only so much you can try. We would like create the perfect travel guide for a foodie in Boston.

## Constraints and Optimization
To create the perfect guide for a one day food adventure in Boston, we did the following:
1. imported food inspection data from Analyze Boston and calculated the food violation rate of each restaurant
2. calculated the average rating of restaurants and the distances between restaurants
3. set the constraints for the z3 solver as the following: for each starting point(a restuarant), the next destinations must have ratings above average, a low violation rate, and short distance to the starting point

## Statistics
We implemented the statistic analysis in the stats_analysis.py script. In the script, we want to find the correlation between the following:

1. Review count and rating
2. Review count and price
3. Review count and violation rate
4. Rating and price
5. Rating and violation rate
3. Price and violation rate

## How to Run

Normally:
```
python execute.py kzhang21_ryuc_zui_sarms
```

Trial Mode:
```
python execute.py kzhang21_ryuc_zui_sarms
```
