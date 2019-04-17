# This function take two variables
# First variable: home address.  Second variable: work address
# Both are strings.  For example, '418 Beachview Drive, North Vancouver, BC'
# This func uses googlemaps distance_matrix API to calculate travel time
# The return is a string.  for example, 26 mins.
import requests
import json
import googlemaps
from datetime import datetime
# import json
# from pprint import pprint

# home = '418 Beachview Drive, North Vancouver, BC'
# work = '138 East 7th Avenue, Vancouver, BC'
#time = datetime.now()

# PUT IN API KEY HERE
my_key = 'AIzaSyAePvzBOkxdh5YYcgTQjhY9bHWlNEn3Sog'


def walk_time(home_addr, work_addr):
    gmaps = googlemaps.Client(key=my_key)
    time = datetime.now()
    # WALKING TIME
    commute_json = gmaps.distance_matrix(origins=home_addr, destinations=work_addr, key=my_key, mode='walking')
    print(commute_json)
    commute_time = commute_json['rows'][0]['elements'][0]['duration']['text']
    return commute_time

def walk_time_url(home_addr, work_addr): 
    url ='https://maps.googleapis.com/maps/api/distancematrix/json?mode=walking'
    x1, y1 = home_addr
    x2, y2 = work_addr

    urlString = url + '&origins=' + str(x1) + "," + str(y1) + '&destinations=' + str(x2) + "," + str(y2) + '&key=' + my_key

    r = requests.get(urlString) 
    res = r.json()
    res = res['rows'][0]['elements'][0]['duration']['value']
    return res
                     


def drive_time(home_addr, work_addr):
    gmaps = googlemaps.Client(key=my_key)
    time = datetime.now()
    commute_json = gmaps.distance_matrix(origins=home_addr, destinations=work_addr)
    print(commute_json)
    commute_time = commute_json['rows'][0]['elements'][0]['duration']['text']
    return commute_time
    
def toLatLong(stop_addr):
    gmaps = googlemaps.Client(key=my_key)
    latlong = gmaps.geocode(stop_addr)
    return latlong


def distance(home_addr, work_addr):
    gmaps = googlemaps.Client(key=my_key)
    time = datetime.now()
    commute_json = gmaps.distance_matrix(origins=home_addr, destinations=work_addr)
    commute_distance = commute_json['rows'][0]['elements'][0]['distance']['text']
    return commute_distance

if __name__ == '__main__':
    home_addr = '509 Park Dr, Boston'
    work_addr = '800 Boylston St, Boston'
    walk_ct_time = walk_time(home_addr,work_addr)
    print(walk_ct_time)
    ct_dist = distance(home_addr,work_addr)
    print(ct_dist)

# print(commute_json[0]["duration"])
# commute = json.load(commute_json)
# geocode_home = gmaps.geocode('418 Beachview Drive, North Vancouver, BC')
# geocode_work = gmaps.geocode('138 East 7th Avenue, Vancouver, BC')
# print(geocode_home)
# print(geocode_work)


# directions_result = gmaps.directions("Sydney Town Hall","Parramatta, NSW",mode="transit",departure_time=now)


# print(geocode_result)
# print(directions_result)