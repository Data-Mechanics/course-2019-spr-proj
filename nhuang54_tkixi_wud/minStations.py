import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import radians, sqrt, sin, cos, atan2
from z3 import *
from opencage.geocoder import OpenCageGeocode

geocoder = OpenCageGeocode(dml.auth['services']['openCagePortal']['api_key'])


def geodistance(la1, lo1, la2, lo2):
  EARTH_R = 6378.0
  la1 = radians(la1)
  lo1 = radians(lo1)
  la2 = radians(la2)
  lo2 = radians(lo2)

  long_dif = lo1 - lo2
  y = sqrt(
      (cos(la2) * sin(long_dif)) ** 2
      + (cos(la1) * sin(la2) - sin(la1) * cos(la2) * cos(long_dif)) ** 2
      )
  x = sin(la1) * sin(la2) + cos(la1) * cos(la2) * cos(long_dif)
  c = atan2(y, x)

  return EARTH_R * c

def findLatLongByName(path):
  # check null case
  name = path['STREET_NAM']
  query = name + ", Boston, MA"
  # print("name is " + query)

  results = geocoder.geocode(query)

  if (results and len(results)):
    answer = [name, results[0]['geometry']['lat'], results[0]['geometry']['lng']]
    return answer
  else:
    raise Exception("No results returned for this street")


class minStations():

    @staticmethod
    def execute(trial = False):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nhuang54_tkixi_wud', 'nhuang54_tkixi_wud')

        # Get the data
        bikePaths = repo.nhuang54_tkixi_wud.boston_bikes.find()
        
        # The plan: Find minimal number of hubway stations to cover entirety of bike networks
        # in boston. Multiple streets can be covered if they are close to each other
        # (i.e., within the walkable distance, 1mi). 
        # After inputting the constraints, we use z3 to find a solution.

        # api key: dml.auth['services']['openCagePortal']['api_key']

        streets = []
        counter = 0

        # Implement trials
        for path in bikePaths:
          streets.append(findLatLongByName(path))
          counter += 1
          if trial:
            if counter > 19:   # trial
              break 
          if counter > 40:   # limit to prevent huge computations
            break


        # ----
        # Finds minimum amount of stations needed  using Z3 given streets and their location.
        s = Solver()
        y = {}
        i = 0

        for street in streets:
          # (key, [z3 int called id, street name, latitude, longitude])
          y[str(i)] = {'id':Int(str(i)), 'street':street[0], 'lat':street[1], 'long':street[2]}
          # Increment the counter to label
          i += 1

        # In the last slot of y, store the sum of all edges
        y[str(len(y))] = {'id': Int(str(len(y))), 'street': 'Total' }

        # First constraint total = 1 + 2 + 3 (names of possible points)
        string = ""
        for i in range(len(y)-1):
          string += "y['" + str(i) + "']['id'] + "
        
        string = string[:-2] + "== y['" + str(len(y)-1) + "']['id']"

        string = "s.add(" + string + ")"
        exec(string)

        # Add next constraint that each value must be greater or eq to 0
        # Add a constraint that this value must be greater or eq to 0
        for k in y:
          s.add(y[k]['id'] >= 0)

        # Next, we calculate each street's distance to every other street.
        # This is for the next constraint
        for i in range(len(streets)):
          # i+1 so no street is matched up with itself, and no reverse repeats.
          for j in range(i+1, len(streets)):

            # If distance between streets is small, only 1 hubway station needed to cover it.
            # We do >= 1 as opposed to = 1 to allow a station in both locations.
            if geodistance(streets[i][1], streets[i][2], streets[j][1], streets[j][2]) < .3:
              s.add(y[str(i)]['id'] + y[str(j)]['id'] >= 1)


        # Add constraint that the sum is less than 1 for each street
        s.add(y[str(len(y)-1)]['id'] <= (len(y)))

        # The while loop looks for the minimum solution
        # Constantly tightens the constraint for amount of possible solutions
        # Until we reach a state without a solution, then the previous was min
        # 
        # Realistically would push and pop previous states, but this is effective as well
        if s.check() == sat:
          size = len(y)-2
          answer = (s.model(), y)
          while(s.check() == sat and size > 0):
            answer = (s.model(), y)
            s.add(y[str(len(y)-1)]['id'] <= size)
            size-= 1

        else:
          print("not_sat")
          raise Exception("No solution")




        # ----

        solution = {}

        for d in answer[0].decls():
          if str(d.name()) == str(len(answer[0])-1):
            solution["total"] = answer[0][d]
          else:
            solution[d.name()] = {y[d.name()]['street'], y[d.name()]['lat'], y[d.name()]['long'], answer[0][d]}

        repo.logout()

        print(solution)

        return solution

if __name__ == '__main__':
    minStations.execute(trial=True)
