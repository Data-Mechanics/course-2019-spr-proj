import pandas as pd
import io
import requests
from statistics import *
import numpy as np
from random import shuffle

def get_ue():
    # unemployment rates 2001 - 2016
    url = 'https://chelseama.ogopendata.com/dataset/96856462-09a8-4384-a5b5-105069245ad4/resource/abda138a-b402-4ca6-9663-0746bf4e32f4/download/labor-force-and-unemployment-data-chelsea-2001-2017.csv.xlsx'

    df = pd.read_excel(url)
    # print(df['Average Area Rate'])

    years = {}
    for i, row in df.iterrows():
        if row['Year'] in [i for i in range(2010,2017)]:
            years[row['Year']] = row['Average Area Rate']
    return years

def get_avp():
    # price per square foot over years
    url = 'http://files.zillowstatic.com/research/public/City/City_ZriPerSqft_AllHomes.csv'
    s = requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('ISO-8859-1')))
    df = df.loc[(df['RegionName'] == 'Chelsea') & (df['State'] == 'MA')]
    df = df.drop(['RegionID', 'RegionName', 'State', 'Metro', 'CountyName', 'SizeRank'], axis=1)

    years = {}
    for col in df.columns:
        # print(df.iloc[0][col])
        # break
        year = col[:4]
        if year in years:
            years[year].append(df.iloc[0][col])
        else:
            years[year] = [df.iloc[0][col]]
    for year in years:
        years[year] = mean(years[year])
    return years

# def avg_price_per_sq_inch(df):
#     years = {}
#     for col in df.columns:
#         # print(df.iloc[0][col])
#         # break
#         year = col[:4]
#         if year in years:
#             years[year].append(df.iloc[0][col])
#         else:
#             years[year] = [df.iloc[0][col]]
#     for year in years:
#         years[year] = mean(years[year])
#     return years

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

if __name__ == "__main__":

    ue = get_ue() # unemployment
    avp = get_avp() # average price per sqft

    # select rates from years
    x = [ue[i] for i in ue]
    # select price from years where 2009 < years < 2017
    y = [avp[i] for i in avp if i in [str(j) for j in range(2010,2017)]]

    print('Correlation (unemployment rate, average price per sq-inch home) = %s'%str(corr(x, y)))

    print('p-value = %s'%str(p(x,y)))
