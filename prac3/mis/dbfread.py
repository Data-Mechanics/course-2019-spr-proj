from pyproj import Proj, transform
from uszipcode import Zipcode
from uszipcode import SearchEngine
import censusgeocode as cg


inProj = Proj(init='epsg:26986')
outProj = Proj(init='epsg:4326')
x1,y1 = 57982.156174257398,920379.59654884413
x2,y2 = transform(inProj,outProj,x1,y1)
print(x2,y2)

search = SearchEngine(simple_zipcode=True) # set simple_zipcode=False to use rich info database

result = search.by_coordinates(y2, x2, radius=3, returns=1)

res = cg.coordinates(x=-73.2, y=42.5)
print(res)
