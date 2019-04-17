from z3 import *
import pandas as pd
import requests
from statistics import *
import numpy as np

def get_median_household_income():
    url = 'https://chelseama.ogopendata.com/dataset/1305b4ca-7b0f-49fe-b215-7882d223b5f6/resource/659c4348-6b39-4b79-8483-1f3ced5389c9/download/income-in-the-past-12-months.xls'

    df = pd.read_excel(url)
    # df.iloc[5:27, :13]; this gets us the table with all the income info
    # irrelevant cols: 1,2, 4, 5
    df = df.iloc[5:27, :13]

    median_income_households = df.iloc[20, 3]

    (less_ten, ten_onefive, onefive_twofive, twofive_threefive, threefive_fivezero, fivezero_sevenfive, sevenfive_hundred, hundred_onefifty, onefifty_twohundred, over_twohundred) = [df.iloc[i, 3] for i in range(9, 19)]

    return median_income_households

housing_percent = .3

def z3_max_price():
    price_of_new_home = Int('price_of_new_home')
    s = Solver()
    s.add(afford(price_of_new_home) > .5)
    last_model = None
    i = 0
    while True:
        print(i)
        r = s.check()
        if r == unsat:
            if last_model != None:
                return last_model
            else:
                return unsat
        if r == unknown:
            return 'UnKnown'
        last_model = s.model()
        s.add(price_of_new_home > last_model[price_of_new_home])
        i += 1
        if i > 2000:
            return 'Not Found'
    # print(s.check())
    # print(s.model())


population_income = list(np.random.uniform(0, 10000, 88)) + list(np.random.uniform(10000, 15000, 68)) + list(np.random.uniform(15000,25000, 106)) + list(np.random.uniform(25000,35000, 111)) + list(np.random.uniform(35000, 50000, 130)) + list(np.random.uniform(50000, 75000, 197)) + list(np.random.uniform(75000, 100000, 104)) + list(np.random.uniform(100000, 150000, 127)) + list(np.random.uniform(150000, 200000, 38)) + list(np.random.uniform(200000, 1000000, 29))
def afford(price, population_income=population_income):
    income_multiple = 4.5 # buy home 4.5x income
    count = 0
    for i in population_income:
        count += (If(price <= (i*income_multiple), 1, 0))
        # count += 1 if price <= i*income_multiple else 0
    return count/len(population_income)

def afford1(price, population_income=population_income):
    income_multiple = 4.5 # buy home 4.5x income
    count = 0
    for i in population_income:
        # count += (If(price <= (i*income_multiple), 1, 0))
        count += 1 if price <= i*income_multiple else 0
    return count/len(population_income)

def brute_force_max_price(population_income = population_income):
    price = 0
    while afford1(price) > .5:
        price += 10
    return price

if __name__ == '__main__':
    # get_median_household_income()
    # foo()
    # print(find_max_price())

    print(z3_max_price())
    print(brute_force_max_price())
