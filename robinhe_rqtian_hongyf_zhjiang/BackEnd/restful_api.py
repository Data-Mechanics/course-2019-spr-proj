import jsonschema
from flask import Flask, jsonify, abort, make_response, request
from flask_httpauth import HTTPBasicAuth
import dbsetup
import copy

app = Flask(__name__)
auth = HTTPBasicAuth()
db = dbsetup.setup_mongodb()

users = [
  {'username': u'cs504','password':'cs504'}
]

@app.route('/revere/api/v1/crashes', methods=['GET'])
def get_crashes():
    crashes = []
    reader = db['robinhe_rqtian_hongyf_zhjiang.crash_simplify'].find({},{'_id':0});
    for item in reader:
        crashes.append(item)
    return jsonify({'crashes': crashes})

@app.route('/revere/api/v1/crashes/<int:crash_id>', methods=['GET'])
def get_user(crash_id):
    reader = db['robinhe_rqtian_hongyf_zhjiang.crash_simplify'].find({'number':{'$eq':crash_id}},{'_id':0});
    crash = {}
    for item in reader:
        crash = item
    return jsonify({'crash': crash})

@app.route('/revere/api/v1/locations', methods=['GET'])
def get_locations():
    reader = db['robinhe_rqtian_hongyf_zhjiang.crashSpots'].find({}, {'_id': 0});
    locations = []
    for item in reader:
        locations.append(item)
    return jsonify(locations)


@app.route('/revere/api/v1/locations/dangerous', methods=['GET'])
def get_dangerous_locations():
    reader = db['robinhe_rqtian_hongyf_zhjiang.likelyCrashSpots'].find({}, {'_id': 0});
    dangerous = []
    for item in reader:
        var1 = item['left-up']
        var2 = item['right-bottom']
        lat = (var1[0] + var2[0]) / 2
        lon = (var1[1] + var2[1]) / 2
        dangerous.append({'lat': lat, 'lon': lon})
    return jsonify(dangerous)


@app.route('/revere/api/v1/statistics', methods=['GET'])
def get_statistics():
    category_dict = {'weather':['Cloudy', 'Rain', 'Clear', 'Sleet', 'Fog', 'Unknown'],
                     'severity':['Fatal', 'Non-fatal', 'Property', 'Unknown'],
                     'surface':['Dry', 'Wet', 'Snow', 'Ice', 'Water', 'Sand', 'Slush', 'Unknown'],
                     'ambient':['DarkLighted', 'Dark', 'DarkUnknown', 'DayLight', 'Dusk', 'Dawn', 'Unknown']}

    iter_args = request.args.items();
    baseline = next(iter_args)[1]
    query_list = []
    query_dict = {'$and': []}
    outputs = {}

    for item in iter_args:
        vals = item[1].split(',')
        query_item = {'$or':[]}
        for val in vals:
            query_item['$or'].append({item[0]:val})
        query_dict['$and'].append(query_item)

    print(query_dict)
    for baseline_condition in category_dict[baseline]:
        query_each = copy.deepcopy(query_dict)
        query_each['$and'].append({baseline:baseline_condition})
        print(query_each)
        reader = db['robinhe_rqtian_hongyf_zhjiang.crash_simplify'].find(query_each, {'_id': 0});
        year_count = {
            '01':0,
            '02':0,
            '03':0,
            '04':0,
            '05':0,
            '06': 0,
            '07': 0,
            '08': 0,
            '09': 0,
            '10': 0,
            '11': 0,
            '12': 0,
            '13': 0,
            '14': 0,
            '15': 0,
            '16': 0,
            '17': 0,
            '18': 0,
            }
        for item in reader:
            year = item['date'].split('/')[2]
            if year in year_count.keys():
                year_count[year] += 1
        year_arr = list(year_count.values())
        print(baseline_condition + str(sum(year_arr)))
        outputs[baseline_condition] = year_arr
    print(outputs)
    return jsonify(outputs)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found foo.'}), 404)

@auth.get_password
def foo(username):
    if username == 'cs504':
        return 'cs504'
    return None

@auth.error_handler
def unauthorized():
    return make_response(jsonify({'error': 'Unauthorized access.'}), 401)

if __name__ == '__main__':
    app.run(debug=True)
    dbsetup.logout(db)