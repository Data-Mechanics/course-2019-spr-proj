import React, { Component } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';


const data = [
  {
    year: '2000', uv: 4000, pv: 2400, amt: 2400,
  },
  {
    year: '2001', uv: 3000, pv: 1398, amt: 2210,
  },
  {
    year: '2002', uv: 2000, pv: 9800, amt: 2290,
  },
  {
    year: '2003', uv: 2780, pv: 3908, amt: 2000,
  },
  {
    year: '2004', uv: 1890, pv: 4800, amt: 2181,
  },
  {
    year: '2005', uv: 2390, pv: 3800, amt: 2500,
  },
  {
    year: '2006', uv: 3490, pv: 4300, amt: 2100,
  },
]

class CarbonEmissionsGraph extends Component {
  constructor(props) {
    super(props);
    this.state = {
      graphData: [], 
      countries: [], 
      colors: [],
    }
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

    let totalData = [];
    for (let i = 0; i < filtered.length; i++) {
      let currRow = filtered[i];
      let country = currRow['country'];
      let carbonData = currRow['carbon_emissions'];
      for (let i = 0; i < carbonData.length; i++) {
        let newRow = {};
        let yearData = carbonData[i];
        let key = Object.keys(yearData)[0];
        let value = yearData[key];
        if (this.checkExistsObjectWithYear(totalData, key)) {
          let existingRow = this.findObjectWithYear(totalData, key);
          existingRow[country] = value.toExponential();
          totalData[totalData.indexOf(existingRow)] = existingRow;
          continue;
        }
        newRow['year'] = key;
        newRow[country] = value.toExponential();
        totalData.push(newRow);
        
      }

     
    }
    totalData.sort(this.compare);
    this.setState({graphData: totalData, countries: countriesSelected, colors: colors});
    
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

  render() {
    const { graphData, countries, colors } = this.state;
    let linegraph;
    
    if (countries.length === 0) {
      linegraph = 
        <div>
          <h5 style={{textAlign:"center"}}>Metric Tons of CO2 per Year (by Country)</h5>
          <LineChart
            width={1250}
            height={500}
            data={data}
            margin={{
              top: 5, right: 30, left: 20, bottom: 5,
            }}>
            <CartesianGrid strokeDasharray="3 3"/>
            <XAxis dataKey="year"/>
            <YAxis/>
          </LineChart>
        </div>
    }
    else {
      linegraph = 
        <div>
          <h5 style={{textAlign:"center"}}>Metric Tons of CO2 per Year (by Country)</h5>
          <LineChart
            width={1250}
            height={500}
            data={graphData}
            margin={{
              top: 5, right: 30, left: 20, bottom: 5,
            }}>
            <CartesianGrid strokeDasharray="3 3"/>
            <XAxis dataKey="year"/>
            <YAxis
              tickFormatter={tick => {
                return tick.toExponential()
              }} 
            />
            <Tooltip/>
            <Legend/>
            {countries.map((country, index) => (
              <Line key={`graph-${country}`} type="monotone" dataKey={`${country}`} stroke={colors[index]}/>
            ))}
          </LineChart>
        </div>

    }
    return (
      /* Using props from parent component generate Line component with corresponding data */
      linegraph
    )
  }
}

export default CarbonEmissionsGraph;