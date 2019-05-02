# data from previous groups project
import pandas as pd
import json
import urllib.request

# file_name = 'street-book.csv'
#
# sheet = 'street_book'
#
# df = pd.read_csv(file_name)
# print(df.head(5))
#
# remove_words = ['Street', 'Road','Place','Square', 'Boulevard','Highway','Avenue', 'Alley', 'North', 'South', 'Mt.',
#                 'Court', 'Terrace', 'Path', 'Circle', 'Park', 'Way', 'Drive']
#
# pat = r'\b(?:{})\b'.format('|'.join(remove_words))
#
# df['street_name'] = df['street-name'].str.replace(pat, '')
#
# df = df.drop(df.columns[0], axis=1)
# df = df.drop(['street-name', 'gender2'], axis=1)
# print(df.head(5))
#
#
# out = df.to_json(orient='records')
#
# with open('boston_street_names.json', 'w') as f:
#     f.write(out)
#


url = 'http://datamechanics.io/data/boston_street_names.json'
response = urllib.request.urlopen(url).read().decode("utf-8")

df = pd.read_json(response)


df['street_name'] = df['street_name'].str.strip()

print(df.head(5))

out = df.to_json(orient='records')
with open('boston_street_names.json', 'w') as f:
    f.write(out)
