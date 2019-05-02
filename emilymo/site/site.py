from flask import Flask, render_template
import os
import pymongo
import json

app = Flask(__name__)

def get_info():
    client = pymongo.MongoClient() 
    a = client.a
    
    inforaw = [r for r in a.info.find()]
    for r in inforaw:
        del r['_id']    
    with open('static/info.json', 'w') as outfile:
        json.dump(inforaw, outfile)
    a.logout()


@app.route('/') # tell which url should call this funct, meaning when homepage (http://localhost:5000/) is opened, this funct is called
def site():
   # return 'impale'
   info = 'static/info.json'
   zerox = 'static/x.svg'
   # return render_template('d3test5.html', info = info, zerox = zerox)
   return render_template('viz.html', info = info, zerox = zerox)



get_info()

if __name__ == '__main__':
   app.run() # runs on local server. calling run method starts app, and needs to be restarted every time change code if debug set to false