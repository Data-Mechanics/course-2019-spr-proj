from flask import Flask
from flask import render_template
from flask import request, redirect, flash

from app import app

@app.route('/')
def index():
    y = request.form.get('input')
    print(y)
    return render_template('index.html')


@app.route('/recievedata', methods=['GET','POST'])
def recieve_data():
    bedroom = request.form.get('bedrooms')
    bathroom = request.form.get('bathrooms')
    cat = request.form.get('categories')
    return '''<h1>the input value is: {} {} {}</h1>'''.format(bedroom, bathroom, cat)
