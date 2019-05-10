import folium
import pandas as pd
from flask import Flask, render_template, request, jsonify, url_for, request, session, redirect
import json
from sklearn.cluster import KMeans
from sklearn.cluster import MiniBatchKMeans
import dml
from flask_pymongo import PyMongo
import bcrypt


def project(R, p):
	return [p(t) for t in R]


def select(R, s):
	return [t for t in R if s(t)]


def get_id_lat_long():
	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('hxjia_jiahaozh', 'hxjia_jiahaozh')

	listings = repo.hxjia_jiahaozh.listings
	listings_information = listings.find({})
	all_listings = []
	for listing_information in listings_information:
		all_listings.append(listing_information)
	id_latitude_longitude = project(all_listings, lambda t: [t['id'], t['latitude'], t['longitude']])
	return id_latitude_longitude


lat = []
long = []
id_lat_long = get_id_lat_long()
for i in range(0, len(id_lat_long)):
		lat.append(id_lat_long[i][1])
		long.append(id_lat_long[i][2])


app = Flask(__name__)

app.config['SECRET_KEY'] = 'abcdedfghjk'
app.config['MONGO_DBNAME'] = 'repo'
app.config['MONGO_URI'] = 'mongodb://hxjia_jiahaozh:hxjia_jiahaozh@localhost/repo'

mongo = PyMongo(app)


@app.route('/')
def index():
	return render_template('index.html')


@app.route('/login', methods=['POST'])
def login():
	users = mongo.db.users
	login_user = users.find_one({'name': request.form['username']})

	if login_user:
		if bcrypt.hashpw(request.form['pass'].encode('utf-8'), login_user['password']) == login_user['password']:
			session['username'] = request.form['username']
			return render_template('leaflet.html', lat=lat, long=long)
		# redirect(url_for('index'))
	else:
		error = True

	return render_template('index.html', error=error)


@app.route('/register', methods=['POST', 'GET'])
def register():
	if request.method == 'POST':
		users = mongo.db.users
		existing_user = users.find_one({'name': request.form['username']})

		if existing_user is None:
			hashpass = bcrypt.hashpw(request.form['pass'].encode('utf-8'), bcrypt.gensalt())
			users.insert({'name': request.form['username'], 'password': hashpass})
			session['username'] = request.form['username']
			return redirect(url_for('index'))
		else:
			error = True

		return render_template('register.html', error=error)

	return render_template('register.html')



@app.route("/test", methods=['GET', 'POST'])
def test():
	price_index = request.form.get('selected_price')
	#print(price_index)
	score_index = request.form.get('selected_score')
	#print(score_index)
	min_price, max_price, min_score, max_score = transformation(price_index, score_index)
	#print(min_price)
	#print(max_price)
	#print(min_score)
	#print(max_score)
	lat1, long1, ids = get_kmeans_results(min_price, max_price, min_score, max_score)
	#print(lat1)
	#print(long1)
	#print(ids)
	return render_template('result.html', lat=lat1, long=long1, housing_urls=ids)


def transformation(price_index, score_index):
	if price_index == "1" and score_index == "1":
		max_price = 100
		min_price = 0
		max_score = 85
		min_score = 0
	if price_index == "1" and score_index == "2":
		max_price = 100
		min_price = 0
		max_score = 90
		min_score = 85
	if price_index == "1" and score_index == "3":
		max_price = 100
		min_price = 0
		max_score = 95
		min_score = 90
	if price_index == "1" and score_index == "4":
		max_price = 100
		min_price = 0
		max_score = 100
		min_score = 95
	if price_index == "2" and score_index == "1":
		max_price = 200
		min_price = 100
		max_score = 85
		min_score = 0
	if price_index == "2" and score_index == "2":
		max_price = 200
		min_price = 100
		max_score = 90
		min_score = 85
	if price_index == "2" and score_index == "3":
		max_price = 200
		min_price = 100
		max_score = 95
		min_score = 90
	if price_index == "2" and score_index == "4":
		max_price = 200
		min_price = 100
		max_score = 100
		min_score = 95
	if price_index == "3" and score_index == "1":
		max_price = 300
		min_price = 200
		max_score = 85
		min_score = 0
	if price_index == "3" and score_index == "2":
		max_price = 300
		min_price = 200
		max_score = 90
		min_score = 85
	if price_index == "3" and score_index == "3":
		max_price = 300
		min_price = 200
		max_score = 95
		min_score = 90
	if price_index == "3" and score_index == "4":
		max_price = 300
		min_price = 200
		max_score = 100
		min_score = 95
	if price_index == "4" and score_index == "1":
		max_price = 800
		min_price = 400
		max_score = 85
		min_score = 0
	if price_index == "4" and score_index == "2":
		max_price = 800
		min_price = 400
		max_score = 90
		min_score = 85
	if price_index == "4" and score_index == "3":
		max_price = 800
		min_price = 400
		max_score = 95
		min_score = 90
	if price_index == "4" and score_index == "4":
		max_price = 800
		min_price = 400
		max_score = 100
		min_score = 95
	return min_price, max_price, min_score, max_score


def get_kmeans_results(min_price, max_price, min_score, max_score):

	client = dml.pymongo.MongoClient()
	repo = client.repo
	repo.authenticate('hxjia_jiahaozh', 'hxjia_jiahaozh')

	collection_data = repo.hxjia_jiahaozh.id_month_price_score_lat_long
	information = collection_data.find({})
	all_data = []
	for data in information:
		all_data.append(data)

	all_data = select(all_data, lambda t: t['number_of_reviews'] > 10 and min_price <= t['price'] <= max_price and min_score <= t['review_score'] <= max_score)
	price_score = project(all_data, lambda t: [t['price'], t['review_score']])
	urls = project(all_data, lambda t: t['id'])
	kms = MiniBatchKMeans(init='k-means++', n_clusters=4, batch_size=100, random_state=0)
	y = kms.fit_predict(price_score)

	count0, count1, count2, count3 = 0, 0, 0, 0
	sum0, sum1, sum2, sum3 = 0, 0, 0, 0
	lat0, lat1, lat2, lat3 = [], [], [], []
	long0, long1, long2, long3 = [], [], [], []
	url0, url1, url2, url3 = [], [], [], []
	for i in range(0, len(y)):
		if y[i] == 0:
			sum0 += price_score[i][0]
			lat0.append(all_data[i]['latitude'])
			long0.append(all_data[i]['longitude'])
			url0.append(urls[i])
			count0 += 1
		elif y[i] == 1:
			sum1 += price_score[i][0]
			lat1.append(all_data[i]['latitude'])
			long1.append(all_data[i]['longitude'])
			url1.append(urls[i])
			count1 += 1
		elif y[i] == 2:
			sum2 += price_score[i][0]
			lat2.append(all_data[i]['latitude'])
			long2.append(all_data[i]['longitude'])
			url2.append(urls[i])
			count2 += 1
		elif y[i] == 3:
			sum3 += price_score[i][0]
			lat3.append(all_data[i]['latitude'])
			long3.append(all_data[i]['longitude'])
			url3.append(urls[i])
			count3 += 1
	center0_x = sum0 / count0
	center1_x = sum1 / count1
	center2_x = sum2 / count2
	center3_x = sum3 / count3
	centers = [center0_x, center1_x, center2_x, center3_x]
	centerscopy = centers.copy()
	centers.sort()
	firstindex = centerscopy.index(centers[0])
	secondindex = centerscopy.index(centers[1])
	thirdindex = centerscopy.index(centers[2])
	fourthindex = centerscopy.index(centers[3])
	url_all = [url0, url1, url2, url3]
	lat_all = [lat0, lat1, lat2, lat3]
	long_all = [long0, long1, long2, long3]
	return lat_all[firstindex], long_all[firstindex], url_all[firstindex]




if __name__ == '__main__':
    app.run()


