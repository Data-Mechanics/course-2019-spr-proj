import pandas as pd
import json
import urllib.request

file_name = 'zipcodes.csv'

df = pd.read_csv(file_name)
print(df.head(5))

df = df.drop(df.columns[0], axis=1)
df = df.drop(['orig_zip'], axis=1)

df['ZIP']=df['ZIP'].apply(lambda x: '{0:0>5}'.format(x))

df1 = df[['ZIP', 'LAT', 'LNG']]
test_dict = df1.set_index('ZIP')[['LAT', 'LNG']].T.to_dict()

# print(df1.head(5))
# print(test_dict)
r = json.dumps(test_dict)
r = '[' + r + ']'

print(r)

with open('MA_zip_codes.json', 'w') as f:
     f.write(r)
