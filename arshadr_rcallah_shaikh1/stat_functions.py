from random import shuffle
from statistics import *
import pandas as pd
import json
from z3 import *

def avg(x):
    return sum(x)/len(x)

def cov(x, y):
    return sum([(1/len(x)) * (x[i] - avg(x)) * (y[i] - avg(y)) for i in range(len(x))])

def corr(x, y):
    return cov(x,y) / (stdev(x) * stdev(y)) if stdev(x) * stdev(y) else 'Division by 0'

def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def p(x, y):
    c0 = corr(x,y)
    corrs = []
    for k in range(0, 2000):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return len([c for c in corrs if abs(c) >= abs(c0)])/len(corrs)

def json_to_df(json_dict):
    json_str = json.dumps(json_dict)
    df = pd.read_json(json_str)
    return df.sort_index()

def afford(price, population_income):
    income_multiple = 4.5 # buy home 4.5x income
    count = 0
    for i in population_income:
        count += (If(price <= (i*income_multiple), 1, 0))
        # count += 1 if price <= i*income_multiple else 0
    return count/len(population_income)

def afford1(price, population_income):
    income_multiple = 4.5 # buy home 4.5x income
    count = 0
    for i in population_income:
        # count += (If(price <= (i*income_multiple), 1, 0))
        count += 1 if price <= i*income_multiple else 0
    return count/len(population_income)
