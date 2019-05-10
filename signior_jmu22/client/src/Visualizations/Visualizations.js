import React, { Component } from 'react';
import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import Table from 'react-bootstrap/Table';
import './Visualizations.css';
import CarDataGraph from  './CarDataGraph/CarDataGraph';
import CarbonEmissionsGraph from './CarbonEmissionsGraph/CarbonEmissionsGraph';
import PowerPlants from './PowerPlants/PowerPlants';
class Visualizations extends Component {
  constructor(props) {
    super(props);

    this.state = {data: [], countriesList: [], graphCountries: [], stats: [], stats2: [], constraint: []}
    
    this.getCheckedValues = this.getCheckedValues.bind(this);
  }
  componentDidMount() {
    fetch("http://localhost:5000/car_and_emissions", {method: 'GET', dataType:'json'})
      .then(r => r.json())
      .then(r => {
        let countries = []
        for (let i = 0; i < r.length; i++) {
          let row = r[i];
          countries.push(row['country'])
        }
        this.setState({data: r, countriesList: countries})
      }).catch(err => console.log(err))
    fetch("http://localhost:5000/statistics", {method: 'GET', dataType:'json'})
      .then(r => r.json())
      .then(r => {
        this.setState({stats: r})
      }).catch(err => console.log(err))
    fetch("http://localhost:5000/statistics2", {method: 'GET', dataType:'json'})
      .then(r => r.json())
      .then(r => {
        this.setState({stats2: r})
      }).catch(err => console.log(err))
    fetch("http://localhost:5000/constraint_satisfaction", {method: 'GET', dataType:'json'})
      .then(r => r.json())
      .then(r => {
        this.setState({constraint: r})
      }).catch(err => console.log(err))
  
  }

  getCheckedValues() {
    const { countriesList } = this.state;
    let countryIds = countriesList.map(countryName => ('id-'+countryName));
    let selectedCountries = [];
    for (let i = 0; i < countryIds.length; i++) {
      let currId = countryIds[i];
      let checkboxCurr = document.getElementById(currId);
      if (checkboxCurr.checked) {
        let countryName = currId.split("-")[1];
        selectedCountries.push(countryName);
      }
    }
    this.setState({graphCountries: selectedCountries})
  }



  getColors(graphCountries, countriesList) {
    let colors = ["#b8ac0b","#f5416a","#825d4c","#273b22","#3a1abf","#d32538","#1d63bf","#357465","#d4708d","#fe215f","#cce99b",
    "#9fbf62","#f96918","#fb55b4","#1ec23c","#9cd222","#f7ae29","#511fb2","#7ba573","#39faad","#3ae3c4","#2b9184","#8cd5ea","#19f9b7","#1e85fc",
    "#61fb22","#a8a19b","#59ab86","#a67f32","#56e072","#14b2f1","#f433e0","#e4575f","#651ebc","#109269","#7cfadf","#954e5f","#9366e8",
    "#fea166","#f0f892","#370e2e","#159983","#14486a","#5250a6","#35e016","#216b92","#c210dc","#95510c"];
    let result = [];
    for (let i = 0; i < graphCountries.length; i++) {
      let country = graphCountries[i];
      let index = countriesList.indexOf(country);
      result.push(colors[index]);
    }
    return result;
  }

  render() {
    /* TODO: grab values from check box and send data as prop to the charts below */
    const { data, countriesList, graphCountries, stats, stats2, constraint } = this.state;
    let colors = this.getColors(graphCountries, countriesList);
    return (
      <div>
        <div className="visualDiv">
          <div className="checkBoxDiv">
            <h5>Change Data Displayed</h5>
            <Form>
              {countriesList.map(countryName => (
                <div key={`key-${countryName}`}>
                  <Form.Check 
                    className="formStyle"
                    custom
                    type={'checkbox'}
                    id={`id-${countryName}`}
                    label={`${countryName}`}
                  />
                </div>
              ))}
            </Form>
            <Button onClick={this.getCheckedValues} className="buttonStyling" variant="primary">
              Change Graph Data
            </Button>
          </div>
          <div>
            <CarDataGraph props={[data, graphCountries, colors]}/>
            <CarbonEmissionsGraph props={[data, graphCountries, colors]}/>
          </div>
        </div>
        <div>
          <PowerPlants props={[constraint, graphCountries, colors]}/>
        </div>
        <hr/>
        <div>
          <h5 style={{textAlign:"center"}}>Statistical Analysis for Vehicle Yearly Increase vs. Carbon Emissions</h5>
          <div style={{marginLeft: "10px"}}>
            <p>We performed a Linear Regression test on the above dataset to determine if which of the following hypothesis is true:</p>
            <ol>
              <li>H0: No relationship between increase in vehicles and CO2 emissions.</li>
              <li>H1: There exists a linear relationship between the increase in vehicles and CO2 emissions.</li>
            </ol>
            <p>In this scenario, our alpha value is 0.05.</p>
          </div>
         
          <Table>
            <thead>
              <tr>
                <th>Country</th>
                <th>Slope</th>
                <th>Intercept</th>
                <th>R-Value</th>
                <th>R-Squared</th>
                <th>P-Value</th>
              </tr>
            </thead>
            <tbody>
              {
                stats.map((entry, index) => (
                  <tr key={`key-${entry.country}`}>
                    <th style={{fontWeight: 'normal'}}>{`${entry.country}`}</th>
                    <th style={{fontWeight: 'normal'}}>{`${entry.slope}`}</th>
                    <th style={{fontWeight: 'normal'}}>{`${entry.intercept}`}</th>
                    <th style={{fontWeight: 'normal'}}>{`${entry.r_value}`}</th>
                    <th style={{fontWeight: 'normal'}}>{`${entry.r_squared}`}</th>
                    <th style={{fontWeight: 'normal', color: ((`${entry.p_value}` < 0.06) ? 'green' : 'red')}}>{`${entry.p_value}`}</th>
                  </tr>
                ))
              }
            </tbody>
          </Table>
        </div>
        <hr/>
        <div>
          <h5 style={{textAlign:"center"}}>Statistical Analysis for Number of Power Plants vs. Carbon Emissions</h5>
          <div style={{marginLeft: "10px"}}>
            <p>We performed a Linear Regression test on the above dataset to determine if which of the following hypothesis is true:</p>
            <ol>
              <li>H0: No relationship between increase in the number of power plants and CO2 emissions.</li>
              <li>H1: There exists a linear relationship between the increase in the number of power plants and CO2 emissions.</li>
            </ol>
            <p>Like the previous, our alpha value is 0.05.</p>
          </div>
         
          <Table>
            <thead>
              <tr>
                <th>Country</th>
                <th>Slope</th>
                <th>Intercept</th>
                <th>R-Value</th>
                <th>R-Squared</th>
                <th>P-Value</th>
              </tr>
            </thead>
            <tbody>
              {
                stats2.map((entry, index) => (
                  <tr key={`key-${entry.country}`}>
                    <th style={{fontWeight: 'normal'}}>{`${entry.country}`}</th>
                    <th style={{fontWeight: 'normal'}}>{`${entry.slope}`}</th>
                    <th style={{fontWeight: 'normal'}}>{`${entry.intercept}`}</th>
                    <th style={{fontWeight: 'normal'}}>{`${entry.r_value}`}</th>
                    <th style={{fontWeight: 'normal'}}>{`${entry.r_squared}`}</th>
                    <th style={{fontWeight: 'normal', color: ((`${entry.p_value}` < 0.06) ? 'green' : 'red')}}>{`${entry.p_value}`}</th>
                  </tr>
                ))
              }
            </tbody>
          </Table>
        </div>
      </div>
     
    )
  }
}

export default Visualizations;