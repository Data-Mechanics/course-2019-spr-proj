import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

testData = [{'hod': 11, 'mean_travel_time': 803.64}, {'hod': 6, 'mean_travel_time': 679.0}, {'hod': 6, 'mean_travel_time': 1246.59}, {'hod': 8, 'mean_travel_time': 1138.26}, {'hod': 8, 'mean_travel_time': 2747.51}, {'hod': 8, 'mean_travel_time': 1398.47}, {'hod': 9, 'mean_travel_time': 1401.43}, {'hod': 6, 'mean_travel_time': 1026.88}, {'hod': 7, 'mean_travel_time': 1144.26}, {'hod': 7, 'mean_travel_time': 1414.6}, {'hod': 8, 'mean_travel_time': 2412.88}, {'hod': 11, 'mean_travel_time': 129.17}, {'hod': 6, 'mean_travel_time': 992.19}, {'hod': 7,'mean_travel_time': 2243.4}, {'hod': 8, 'mean_travel_time': 621.01}, {'hod': 11, 'mean_travel_time': 669.33}, {'hod': 11, 'mean_travel_time': 2042.73}, {'hod': 11, 'mean_travel_time': 1512.0}, {'hod': 7, 'mean_travel_time': 431.02}, {'hod': 10, 'mean_travel_time': 1375.29}, {'hod': 7, 'mean_travel_time': 1692.14}, {'hod': 9, 'mean_travel_time': 1286.46}, {'hod': 6, 'mean_travel_time': 862.48}, {'hod': 7, 'mean_travel_time': 2398.4}, {'hod': 7, 'mean_travel_time': 615.57}, {'hod': 11, 'mean_travel_time': 711.35}, {'hod': 10, 'mean_travel_time': 1184.76}, {'hod': 9, 'mean_travel_time': 638.4}, {'hod': 10, 'mean_travel_time': 1248.39}, {'hod': 7, 'mean_travel_time': 1707.05}]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

print(testData)

df = pd.DataFrame(testData)

grouped = df.groupby(['hod']).agg(sum)

print(grouped.reset_index().to_dict('records'))

# res = aggregate(testData, sum)

# print(res)