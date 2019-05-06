import flask
from flask import Flask, render_template, url_for
import gmplot
import json
import uuid
import urllib.request
import os

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
contributor = 'ido_jconstan_jeansolo_suitcase'

#default page  
@app.route("/", methods=['GET'])
def hello():
	gmapEmpty = gmplot.GoogleMapPlotter(42.283772, 
	                                -71.347290, 13) 

	gmapEmpty.apikey = 'AIzaSyAePvzBOkxdh5YYcgTQjhY9bHWlNEn3Sog'

	gmapEmpty.draw( "templates\\dispmap.html" ) 
	gmapEmpty.draw( "static\\dispmap.html" ) 

	return render_template('busstops.html', name="hello")

@app.route("/updateMapBefore", methods=['GET', 'POST'])
def updateMapBefore():
	# DATA SET 9 [StopsLatLng]
	# Bus Stops Latitude and Longitude
	# r9 = {'lat', 'long, 'og'}
	url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/StopsLatLng.json'
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r9 = json.loads(response)

	# ('lat', 'lng', 'og') of stops r9
	t18 = project(r9, lambda t: (t['lat'], t['long']))

	#parse r9 and plot the points on a map
	latitude_list = [] 
	longitude_list = [] 

	for (x,y) in t18:
	    latitude_list.append(x)
	    longitude_list.append(y)

	  
	gmapBefore = gmplot.GoogleMapPlotter(42.283772, 
	                                -71.347290, 13) 
	  
	# scatter points on the google map 
	gmapBefore.scatter( latitude_list, longitude_list, '#0000FF', 
	                              size = 40, marker = False ) 

	gmapBefore.apikey = 'AIzaSyAePvzBOkxdh5YYcgTQjhY9bHWlNEn3Sog'

	gmapBefore.draw('static\\dispmap.html' ) 
	gmapBefore.draw('templates\\dispmap.html' )

	return render_template('busstops.html', name="test")

@app.route("/updateMapAfter", methods=['GET', 'POST'])
def updateMapAfter():

	#DATA SET 10 [k-means_0]
	url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_0.json'
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r10 = json.loads(response)

	#DATA SET 11 [k-means_1]
	url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_1.json'
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r11 = json.loads(response)

	#DATA SET 12 [k-means_2]
	url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_2.json'
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r12 = json.loads(response)

	#DATA SET 13 [k-means_3]
	url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_3.json'
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r13 = json.loads(response)

	#DATA SET 14 [k-means_4]
	url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_4.json'
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r14 = json.loads(response)

	#DATA SET 15 [k-means_5]
	url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/k_means_school_5.json'
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r15 = json.loads(response)

	k0 = project(r10, lambda t: (t['new_stop']))
	k1 = project(r11, lambda t: (t['new_stop']))
	k2 = project(r12, lambda t: (t['new_stop']))
	k3 = project(r13, lambda t: (t['new_stop']))
	k4 = project(r14, lambda t: (t['new_stop']))
	k5 = project(r15, lambda t: (t['new_stop']))

	temp = 0
	for i in k0:
		list_x = []
		list_y = []

		#for j in range(len(i)):
		j=0
		if i[j] == '(':
			j+=1
			list_x.append(i[j])
			j+=1

		while i[j] != ',':
			list_x.append(i[j])
			j+=1

		if i[j] == ',':
			j+=2

		while i[j] != ')':
			list_y.append(i[j])
			j+=1
		str_x = "".join(list_x)
		str_y = "".join(list_y)
		float_x = float(str_x)
		float_y = float(str_y)
		i = (float_x, float_y)

		k0[temp] = i
		temp += 1


	temp = 0
	for i in k1:
		list_x = []
		list_y = []

		#for j in range(len(i)):
		j=0
		if i[j] == '(':
			j+=1
			list_x.append(i[j])
			j+=1

		while i[j] != ',':
			list_x.append(i[j])
			j+=1

		if i[j] == ',':
			j+=2

		while i[j] != ')':
			list_y.append(i[j])
			j+=1
		str_x = "".join(list_x)
		str_y = "".join(list_y)
		float_x = float(str_x)
		float_y = float(str_y)
		i = (float_x, float_y)

		k1[temp] = i
		temp += 1

	temp = 0
	for i in k2:
		list_x = []
		list_y = []

		#for j in range(len(i)):
		j=0
		if i[j] == '(':
			j+=1
			list_x.append(i[j])
			j+=1

		while i[j] != ',':
			list_x.append(i[j])
			j+=1

		if i[j] == ',':
			j+=2

		while i[j] != ')':
			list_y.append(i[j])
			j+=1
		str_x = "".join(list_x)
		str_y = "".join(list_y)
		float_x = float(str_x)
		float_y = float(str_y)
		i = (float_x, float_y)

		k2[temp] = i
		temp += 1

	temp = 0
	for i in k3:
		list_x = []
		list_y = []

		#for j in range(len(i)):
		j=0
		if i[j] == '(':
			j+=1
			list_x.append(i[j])
			j+=1

		while i[j] != ',':
			list_x.append(i[j])
			j+=1

		if i[j] == ',':
			j+=2

		while i[j] != ')':
			list_y.append(i[j])
			j+=1
		str_x = "".join(list_x)
		str_y = "".join(list_y)
		float_x = float(str_x)
		float_y = float(str_y)
		i = (float_x, float_y)

		k3[temp] = i
		temp += 1

	temp = 0
	for i in k4:
		list_x = []
		list_y = []

		#for j in range(len(i)):
		j=0
		if i[j] == '(':
			j+=1
			list_x.append(i[j])
			j+=1

		while i[j] != ',':
			list_x.append(i[j])
			j+=1

		if i[j] == ',':
			j+=2

		while i[j] != ')':
			list_y.append(i[j])
			j+=1
		str_x = "".join(list_x)
		str_y = "".join(list_y)
		float_x = float(str_x)
		float_y = float(str_y)
		i = (float_x, float_y)

		k4[temp] = i
		temp += 1

	temp = 0
	for i in k5:
		list_x = []
		list_y = []

		#for j in range(len(i)):
		j=0
		if i[j] == '(':
			j+=1
			list_x.append(i[j])
			j+=1 

		while i[j] != ',':
			list_x.append(i[j])
			j+=1

		if i[j] == ',':
			j+=2

		while i[j] != ')':
			list_y.append(i[j])
			j+=1
		str_x = "".join(list_x)
		str_y = "".join(list_y)
		float_x = float(str_x)
		float_y = float(str_y)
		i = (float_x, float_y)

		k5[temp] = i
		temp += 1


	latitude_list_k0 = []
	longitude_list_k0 = []
	for (x,y) in k0:
		latitude_list_k0.append(x)
		longitude_list_k0.append(y)

	latitude_list_k1 = []
	longitude_list_k1 = []
	for (x,y) in k1:
		latitude_list_k1.append(x)
		longitude_list_k1.append(y)

	latitude_list_k2 = []
	longitude_list_k2 = []
	for (x,y) in k2:
		latitude_list_k2.append(x)
		longitude_list_k2.append(y)

	latitude_list_k3 = []
	longitude_list_k3 = []
	for (x,y) in k3:
		latitude_list_k3.append(x)
		longitude_list_k3.append(y)

	latitude_list_k4 = []
	longitude_list_k4 = []
	for (x,y) in k4:
		latitude_list_k4.append(x)
		longitude_list_k4.append(y)

	latitude_list_k5 = []
	longitude_list_k5 = []
	for (x,y) in k5:
		latitude_list_k5.append(x)
		longitude_list_k5.append(y)

	gmapAfter = gmplot.GoogleMapPlotter(42.283772, 
	                                -71.347290, 13) 

	after_size = 40

	# scatter points on the google map 
	gmapAfter.scatter( latitude_list_k0, longitude_list_k0, '# FF0000', 
	                              size = after_size, marker = False ) 
	# scatter points on the google map 
	gmapAfter.scatter( latitude_list_k1, longitude_list_k1, '# FF0000', 
	                              size = after_size, marker = False ) 
	# scatter points on the google map 
	gmapAfter.scatter( latitude_list_k2, longitude_list_k2, '# FF0000', 
	                              size = after_size, marker = False ) 
	# scatter points on the google map 
	gmapAfter.scatter( latitude_list_k3, longitude_list_k3, '# FF0000', 
	                              size = after_size, marker = False ) 
	# scatter points on the google map 
	gmapAfter.scatter( latitude_list_k4, longitude_list_k4, '# FF0000', 
	                              size = after_size, marker = False ) 
	# scatter points on the google map 
	gmapAfter.scatter( latitude_list_k5, longitude_list_k5, '# FF0000', 
	                              size = after_size, marker = False ) 

	gmapAfter.apikey = 'AIzaSyAePvzBOkxdh5YYcgTQjhY9bHWlNEn3Sog'

	gmapAfter.draw( "static\\dispmap.html" ) 
	gmapAfter.draw( "templates\\dispmap.html" ) 

	return render_template('busstops.html', name="test")

@app.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r
@app.context_processor
def override_url_for():
    return dict(url_for=dated_url_for)

def dated_url_for(endpoint, **values):
    if endpoint == 'static':
        filename = values.get('filename', None)
        if filename:
            file_path = os.path.join(app.root_path,
                                     endpoint, filename)
            values['q'] = int(os.stat(file_path).st_mtime)
    return url_for(endpoint, **values)

def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

if __name__ == "__main__":
	app.run()