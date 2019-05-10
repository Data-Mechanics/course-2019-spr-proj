import yelpfusion
import json

API_KEY= "QDgDC978iTKi0REpCdl57wm7cj4GdR39pshHcOa1xH2lllBJWAuISYhUnKncrOeZQss43zLgOPzxuD3PlNcgacLIbJmOm5-7ZXQrTmIq5nDZj2Ed_zR6BQkHhDOVXHYx"

res = yelpfusion.search(API_KEY, "coffee shop", "Boston, MA")

r = {}

for num in range(len(res['businesses'])):
    r[res['businesses'][num]['name']] = res['businesses'][num]['coordinates']

print(type(r))

import json
data = json.load(open('../auth.json', 'r'))

print(data['services']['yelpfusionportal']['key'])