import React, { Component } from 'react';
import './App.css'
import hospital from './data/hospital.json'
import police from './data/police.json'
import privateSchool from './data/privateSchool'
import publicSchool from './data/publicSchool'
import propertyAssessment from './data/propertyAssessment'
import {Scatter} from 'react-chartjs-2'
import Select from '@material-ui/core/Select'
import MenuItem from '@material-ui/core/MenuItem';
import Input from '@material-ui/core/Input';
import TextField from '@material-ui/core/TextField';
import axios from 'axios';
var database = [hospital, police, privateSchool, publicSchool]

function getData(index) {
	var result = []
	for (var i = 0; i < database[index].length; i++) {
		for (var j = 0; j < propertyAssessment.length; j++) {
			if (propertyAssessment[j].zipcode == database[index][i].zipcode) {
				if (index === 0 || index === 1) {
					result.push({x: database[index][i].count, y: propertyAssessment[j].avg_value})
				} else {
					result.push({x: database[index][i].num_school, y: propertyAssessment[j].avg_value})
				}
			}
		}
	}
	var data = {
		labels: ['Scatter'],
		datasets: [{
			label: 'Dataset',
			fill: false,
			backgroundColor: 'rgba(75,192,192,0.4)',
			pointBorderColor: 'rgba(75,192,192,1)',
			pointBackgroundColor: '#000000',
			pointBorderWidth: 1,
			pointHoverRadius: 5,
			pointHoverBackgroundColor: 'rgba(75,192,192,1)',
			pointHoverBorderColor: 'rgba(220,220,220,1)',
			pointHoverBorderWidth: 2,
			pointRadius: 3,
			pointHitRadius: 10,
			data: result
    	}]
	};
	return data

}

console.log(getData(2))


class App extends Component {

	constructor(props) {
	    super(props);

	    this.state = {
	      index: "",
	      data : [],
	      hospital: 0,
	      police: 0,
	      privateSchool: 0,
	      publicSchool: 0,
	      prediction: 0
	    }
	    this.onChangeSelectDataset = this.onChangeSelectDataset.bind(this);
	    this.onChangeInput = this.onChangeInput.bind(this);
	}

	onChangeSelectDataset = event => {
		if (event.target.value === "") {
			this.setState({index: ""})
		} else {
			this.setState({index: event.target.value, data: getData(event.target.value)})
		}
	}

	onChangeInput = name => event => {
		var value = event.target.value
		var hospital = this.state.hospital
		var police = this.state.police
		var publicSchool = this.state.publicSchool
		var privateSchool = this.state.privateSchool
		switch(name) {
			case "hospital":
			hospital = event.target.value
			break
			case "police":
			police = event.target.value
			break
			case "privateSchool":
			privateSchool = event.target.value
			break
			case "publicSchool":
			publicSchool = event.target.value
			break
		}
		axios.get('http://localhost:3001/getCorrelation')
        	.then(res => {
        		console.log(res.data)
        		this.setState({
				[name]: value, 
				prediction: parseFloat(publicSchool) * parseFloat(res.data[0].publicSchool) + parseFloat(privateSchool) * parseFloat(res.data[0].privateSchool) + parseFloat(police) * parseFloat(res.data[0].police) + parseFloat(hospital) * parseFloat(res.data[0].hospital) + parseFloat(res.data[0].intercept)
		})
        	});
		
	}

	render() {
		return (
			<div>
				<div>
					<h2>Data Visualization</h2>
					<Select 
						value = {this.state.index}
						onChange = {this.onChangeSelectDataset}
						input={<Input name="age" id="age-helper" />}
					>
						<MenuItem value="">
              				<em>None</em>
            			</MenuItem>
						<MenuItem value = {0}>Hospital</MenuItem>
						<MenuItem value = {1}>Police Station</MenuItem>
						<MenuItem value = {2}>Private School</MenuItem>
						<MenuItem value = {3}>Public School</MenuItem>
					</Select>
					<Scatter data = {this.state.data} />
				</div>
				<div>
					<h2>House Price Prediction</h2>
					<form noValidate autoComplete = "off">
						<TextField label = "Hospital" value = {this.state.hospital} onChange = {this.onChangeInput("hospital")} defaultValue = "0"/>
						<TextField label = "Police" value = {this.state.police} onChange = {this.onChangeInput("police")} defaultValue = "0"/>
						<TextField label = "Private School" value = {this.state.privateSchool} onChange = {this.onChangeInput("privateSchool")} defaultValue = "0"/>
						<TextField label = "Public School" value = {this.state.publicSchool} onChange = {this.onChangeInput("publicSchool")} defaultValue = "0"/>
					</form>
					<h3>Predicted house price in $/ft2: {this.state.prediction}</h3>
				</div>
			</div>
		);
	}
}

export default App;