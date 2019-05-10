import React, { Component } from 'react';
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ReferenceLine } from 'recharts';

class PowerPlants extends Component {
  constructor(props) {
    super(props);
    this.state = {
      graphData: [], 
      countries: [], 
      colors: [],
      refLine: null,
    }
    
    this.moveReferenceLine = this.moveReferenceLine.bind(this);
  }
  

  componentWillReceiveProps(nextProps) {
    let newProps = nextProps.props;
    let completeData = newProps[0];
    let countriesSelected = newProps[1]
    let colors = newProps[2];
    let filtered = []
    for (let i = 0; i < completeData.length; i++) {
      let row = completeData[i];
      let country = row['country'];
      if (countriesSelected.includes(country)) {
        filtered.push(row);
      }
    }
    let minYear = 2019;
    let maxYear = 1900;
    let totalData = [];
    for (var i = 0; i < filtered.length; i++) {
      let entry = [];

      let currRow = filtered[i];
      let country = currRow['country'];
      let currData = currRow['data']
      for (let j = 0; j < currData.length; j++) {
        let temp = {};
        temp['country'] = country;
        let dataRow = currData[j];
        let key = Object.keys(dataRow)[0];
        let values = dataRow[key]
        let currBefore = values[0]
        let currAfter = values[1]
        let year = parseInt(key)
        minYear = Math.min(minYear, year);
        maxYear = Math.max(maxYear, year);
        temp['year'] = year;
        temp['value_before'] = currBefore;
        temp['value_after'] = currAfter;
        temp['color'] = colors[i];
        entry.push(temp);
        
      }
      entry.sort(this.compare);
      totalData.push(entry)
    }
    console.log(totalData);

    this.setState({graphData: totalData, colors: colors, countries: countriesSelected, minYear: minYear, maxYear: maxYear, refLine: minYear })
  }

  compare(a, b) {
    let yearA = parseInt(a.year)
    let yearB = parseInt(b.year);
    return yearA - yearB;
  }
  
  findObjectWithYear(totalData, year) {
    if (!this.checkExistsObjectWithYear(totalData, year)) {
      return {};
    }
    for (let i = 0; i < totalData.length; i++) {
      let row = totalData[i];
      if (row['year'] === year) {
        return row;
      }
    }
  }

  checkExistsObjectWithYear(totalData, year) {
    for (let i = 0; i < totalData.length; i++) {
      let row = totalData[i];
      if (row['year'] === year) {
        return true;
      }
    }
    return false;
  }

  moveReferenceLine(e) {
    let dest = e.year;
    this.setState({refLine: dest})
  }

  render() {
    const { graphData, minYear, maxYear, refLine } = this.state;
    return (
      /* Using props from parent component generate Line component with corresponding data */
      <div>
          <h5 style={{textAlign:"center"}}>Change in CO2 Emissions Before and After Power Plant Establishment Year</h5>
          <div style={{display: "flex", flexDirection: "row"}}>
            <div>
              <h6> Change in CO2 Emissions Before</h6>
              <div>
                <ScatterChart width={700} height={500} margin={{top: 5, right: 30, bottom: 20, left: 5}}>
                  <XAxis domain={[minYear, maxYear]} type="number" dataKey={"year"}/>
                  <CartesianGrid />
                  <YAxis dataKey={"value_before"}/>
                  <Tooltip cursor={{strokeDasharray: '3 3'}}/>
                  <Legend/>
                  <ReferenceLine x={refLine} stroke="green"/>
                  {graphData.map((entry, index) => (
                    <Scatter onClick={this.moveReferenceLine} name={`${entry[0].country}`} key={`graph1-${entry[0].country}`} data={entry} fill={`${entry[0].color}`} shape='square'/>
                  ))}
                </ScatterChart>
              </div>
            </div>
            <div>
              <h6> Change in CO2 Emissions After </h6>
              <div>
                <ScatterChart width={700} height={500} margin={{top: 5, right: 30, bottom: 20, left: 5}}>
                  <XAxis domain={[minYear, maxYear]} type="number" dataKey={"year"}/>
                  <CartesianGrid />
                  <YAxis dataKey={"value_after"}/>
                  <Tooltip cursor={{strokeDasharray: '3 3'}}/>
                  <Legend/>
                  <ReferenceLine x={refLine} stroke="green"/>
                  {graphData.map((entry, index) => (
                    <Scatter onClick={this.moveReferenceLine} name={`${entry[0].country}`} key={`graph2-${entry[0].country}`} data={entry} fill={`${entry[0].color}`} shape='diamond'/>
                  ))}
                </ScatterChart>
              </div>
              
            </div>
          </div>
        </div>
    )
  }
}

// margi

export default PowerPlants;